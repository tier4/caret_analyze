# Copyright 2021 TIER IV, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Iterator, Sequence, Sized
from datetime import datetime
from functools import cached_property
from logging import getLogger
import os
import pickle
from typing import Any

import bt2
import pandas as pd
from tqdm import tqdm

from .events_factory import EventsFactory
from .lttng_event_filter import LttngEventFilter, SameAddressFilter
from .ros2_tracing.data_model import Ros2DataModel
from .ros2_tracing.data_model_service import DataModelService
from .ros2_tracing.processor import get_field, Ros2Handler
from .value_objects import (CallbackGroupId,
                            PublisherValueLttng,
                            ServiceCallbackValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ..infra_base import InfraBase
from ...common import ClockConverter
from ...exceptions import InvalidArgumentError
from ...record import RecordsInterface
from ...value_objects import  \
    CallbackGroupValue, ExecutorValue, NodeValue, NodeValueWithId, Qos, TimerValue

Event = dict[str, int]

logger = getLogger(__name__)


class EventCollection(Iterable, Sized):

    def __init__(self, trace_dir: str, force_conversion: bool, *, store_cache=True) -> None:
        if not self._trace_dir_exists(trace_dir):
            raise FileNotFoundError(f'Failed to found {trace_dir}')

        self._iterable_events: IterableEvents
        cache_path = self._cache_path(trace_dir)
        use_cache = False

        if self._cache_exists(cache_path) and not force_conversion:
            cache_start_time, _ = PickleEventCollection(cache_path).time_range()
            ctf_start_time, _ = CtfEventCollection(trace_dir).time_range()
            use_cache = cache_start_time == ctf_start_time

        if use_cache:
            logger.info('Found converted file.')
            self._iterable_events = PickleEventCollection(cache_path)
        else:
            self._iterable_events = CtfEventCollection(trace_dir)
            if store_cache:
                self._store_cache(self._iterable_events, cache_path)
            logger.info(f'Converted to {cache_path}')

    @staticmethod
    def _store_cache(iterable_events: IterableEvents, path) -> None:
        with open(path, mode='wb') as f:
            pickle.dump(iterable_events.events, f)

    def __len__(self) -> int:
        return len(self._iterable_events)

    def __iter__(self) -> Iterator[dict]:
        return iter(self._iterable_events)

    def time_range(self) -> tuple[int, int]:
        return self._iterable_events.time_range()

    def _cache_path(self, events_path: str) -> str:
        return os.path.join(events_path, 'caret_converted')

    @staticmethod
    def _trace_dir_exists(path: str) -> bool:
        """
        Check whether trace dir exists.

        Parameters
        ----------
        path : str
            Path to trace dir.

        Returns
        -------
        bool
            True if trace dir exists, false otherwise.

        Note
        ----
        This function is written in isolation to simplify testing.

        """
        return os.path.exists(path)

    @staticmethod
    def _cache_exists(path: str) -> bool:
        """
        Check whether cache exists.

        Parameters
        ----------
        path : str
            Path to cache.

        Returns
        -------
        bool
            True if cache exists, false otherwise.

        Note
        ----
        This function is written in isolation to simplify testing.

        """
        return os.path.exists(path)


class IterableEvents(Iterable, Sized, metaclass=ABCMeta):

    @abstractmethod
    def __iter__(self) -> Iterator[dict]:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @property
    @abstractmethod
    def events(self) -> list[dict]:
        pass

    @abstractmethod
    def time_range(self) -> tuple[int, int]:
        pass


class PickleEventCollection(IterableEvents):

    def __init__(self, events_path: str) -> None:
        with open(events_path, mode='rb') as f:
            self._events = pickle.load(f)

    def __iter__(self) -> Iterator[dict]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)

    @property
    def events(self) -> list[dict]:
        return self._events

    def time_range(self) -> tuple[int, int]:
        begin_time = self._events[0][LttngEventFilter.TIMESTAMP]
        end_time = self._events[-1][LttngEventFilter.TIMESTAMP]
        return begin_time, end_time


class CtfEventCollection(IterableEvents):

    def __init__(self, events_path: str) -> None:
        event_count = 0
        begin_msg: Any = None
        end_msg: Any = None

        for msg in bt2.TraceCollectionMessageIterator(events_path):
            if type(msg) is bt2._EventMessageConst:
                event_count += 1

            # Check for traces lost
            if (type(msg) is bt2._DiscardedEventsMessageConst):
                msg = ('Tracer discarded '
                       f'{msg.count} events between '
                       f'{msg.beginning_default_clock_snapshot.ns_from_origin} and '
                       f'{msg.end_default_clock_snapshot.ns_from_origin}.')
                logger.warning(msg)
                continue

            if type(msg) is bt2._EventMessageConst:
                if not begin_msg:
                    begin_msg = msg  # store first one
                end_msg = msg  # store last one

        # Ensure that trace data includes one at least.
        # If there is no message in trace data, assertion failed.
        assert begin_msg is not None
        assert end_msg is not None

        # NOTE: Begin_time and end_time should be the same time as the PickleEventCollection.
        self._begin_time: int = begin_msg.default_clock_snapshot.ns_from_origin
        self._end_time: int = end_msg.default_clock_snapshot.ns_from_origin
        self._size = event_count
        self._events_path = events_path

    def __iter__(self) -> Iterator[dict]:
        return iter(self.events)

    def __len__(self) -> int:
        return self._size

    @staticmethod
    def _to_event(msg: Any) -> dict[str, Any]:
        event: dict[str, Any] = {}
        event[LttngEventFilter.NAME] = msg.event.name
        event['timestamp'] = msg.default_clock_snapshot.ns_from_origin
        event.update(msg.event.payload_field)
        event.update(msg.event.common_context_field)
        return event

    @cached_property
    def events(self) -> list[dict]:
        return self._to_dicts(self._events_path, self._size)

    def time_range(self) -> tuple[int, int]:
        return self._begin_time, self._end_time

    @staticmethod
    def _to_dicts(trace_dir: str, count: int) -> list[dict]:
        msg_it = bt2.TraceCollectionMessageIterator(trace_dir)
        events = []
        acceptable_tracepoints = set(Ros2Handler.get_trace_points())
        for msg in tqdm(msg_it, total=count, desc='converting', mininterval=1.0):
            if type(msg) is not bt2._EventMessageConst:
                continue
            if msg.event.name not in acceptable_tracepoints:
                continue

            event = CtfEventCollection._to_event(msg)
            event[LttngEventFilter.TIMESTAMP] = event.pop('timestamp')
            event[LttngEventFilter.VTID] = event.pop('vtid')
            event[LttngEventFilter.VPID] = event.pop('vpid')
            event[LttngEventFilter.PROCNAME] = event.pop('procname')
            event_dict = {
                k: get_field(event, k) for k in event
            }
            events.append(event_dict)

        return events


class MultiHostIdRemapper:
    """Class for remapping pid/tid of LTTng event to make it unique across multiple hosts."""

    def __init__(self, id_key: str):
        self._id_key = id_key
        self._other_host_ids: set[int] = set()
        self._current_host_remapped_ids: set[int] = set()
        self._current_host_not_remapped_ids: set[int] = set()
        self._current_host_id_map: dict[int, int] = {}
        self._next_id = 1000000000

    def remap(self, event: dict):
        target_id = get_field(event, self._id_key)
        if target_id in self._other_host_ids or target_id in self._current_host_remapped_ids:
            if target_id in self._current_host_id_map:
                event[self._id_key] = self._current_host_id_map[target_id]
            else:
                while self._next_id in self._other_host_ids or \
                        self._next_id in self._current_host_remapped_ids:
                    self._next_id += 1
            remapped_id = self._next_id
            self._next_id += 1
            self._current_host_id_map[target_id] = remapped_id
            self._current_host_remapped_ids.add(remapped_id)
            event[self._id_key] = remapped_id
        else:
            self._current_host_not_remapped_ids.add(target_id)

    def change_host(self):
        self._other_host_ids |= self._current_host_remapped_ids
        self._other_host_ids |= self._current_host_not_remapped_ids
        self._current_host_remapped_ids.clear()
        self._current_host_not_remapped_ids.clear()
        self._current_host_id_map.clear()


class Lttng(InfraBase):
    """
    Lttng data container class.

    This class is a singleton in order to retain information.
    The main processing is done by LttngInfo and RecordsSource.

    """

    # sort order when timestamps are the same (sorting initialization-related trace events)
    _prioritized_init_events = [
        'ros2_caret:rcl_init',
        'ros2:rcl_init',
        'ros2_caret:rcl_node_init',
        'ros2:rcl_node_init',
        'ros2_caret:rcl_publisher_init',
        'ros2:rcl_publisher_init',
        'ros2_caret:rcl_subscription_init',
        'ros2:rcl_subscription_init',
        'ros2_caret:rclcpp_subscription_init',
        'ros2:rclcpp_subscription_init',
        'ros2_caret:rclcpp_subscription_callback_added',
        'ros2:rclcpp_subscription_callback_added',
        'ros2_caret:rcl_service_init',
        'ros2:rcl_service_init',
        'ros2_caret:rclcpp_service_callback_added',
        'ros2:rclcpp_service_callback_added',
        'ros2_caret:rcl_client_init',
        'ros2:rcl_client_init',
        'ros2_caret:rcl_timer_init',
        'ros2:rcl_timer_init',
        'ros2_caret:rclcpp_timer_callback_added',
        'ros2:rclcpp_timer_callback_added',
        'ros2_caret:rclcpp_timer_link_node',
        'ros2:rclcpp_timer_link_node',
        'ros2_caret:rclcpp_callback_register',
        'ros2:rclcpp_callback_register',
        'ros2_caret:rcl_lifecycle_state_machine_init',
        'ros2:rcl_lifecycle_state_machine_init',
        'ros2_caret:caret_init',
        'ros2_caret:rmw_implementation',
        'ros2_caret:construct_executor',
        'ros2_caret:construct_static_executor',
        'ros2_caret:add_callback_group',
        'ros2_caret:add_callback_group_static_executor',
        'ros2_caret:callback_group_add_timer',
        'ros2_caret:callback_group_add_subscription',
        'ros2_caret:callback_group_add_service',
        'ros2_caret:callback_group_add_client',
        'ros2_caret:rclcpp_construct_ring_buffer',
        'ros2:rclcpp_construct_ring_buffer',
        'ros2_caret:rclcpp_buffer_to_ipb',
        'ros2:rclcpp_buffer_to_ipb',
        'ros2_caret:rclcpp_ipb_to_subscription',
        'ros2:rclcpp_ipb_to_subscription',
    ]

    def __init__(
        self,
        trace_dir_or_events: str | list[dict] | list[str],
        force_conversion: bool = False,
        *,
        event_filters: list[LttngEventFilter] | None = None,
        store_events: bool = False,
        # TODO(hsgwa): change validate function to public "verify".
        validate: bool = True
    ) -> None:
        from .lttng_info import LttngInfo
        from .records_source import RecordsSource
        from .event_counter import EventCounter

        if isinstance(trace_dir_or_events, list) and validate:
            logger.warning('Validation of multiple LTTng log is not supported.')
            validate = False

        # Add SameAddressFilter(10) by default
        modified_event_filters = []
        if event_filters:
            modified_event_filters = event_filters.copy()
        if len(list(filter(lambda f: isinstance(f, SameAddressFilter),
                           modified_event_filters))) == 0:
            modified_event_filters.append(LttngEventFilter.same_address_filter(10))

        data, events, begin, end = self._parse_lttng_data(
            trace_dir_or_events,
            force_conversion,
            modified_event_filters,
            store_events
        )
        self.data = data
        self._info = LttngInfo(data)
        self._source: RecordsSource = RecordsSource(data, self._info)
        self._counter = EventCounter(data, validate=validate)
        self.events = events
        self._begin = begin
        self._end = end

    @staticmethod
    def _parse_lttng_data(
        trace_dir_or_events: str | list[dict] | list[str],
        force_conversion: bool,
        event_filters: list[LttngEventFilter],
        store_events: bool,

    ) -> tuple[Ros2DataModel, list[dict] | None, int, int]:

        data = Ros2DataModel()
        offset: int | None = None
        events = []
        begin: int
        end: int

        if event_filters:
            for f in event_filters:
                f.reset()

        if isinstance(trace_dir_or_events, str):
            trace_dir_or_events = [trace_dir_or_events]

        assert (len(trace_dir_or_events) != 0)

        # TODO(hsgwa): Same implementation duplicated. Refactoring required.
        if isinstance(trace_dir_or_events[0], str):
            tid_remapper = MultiHostIdRemapper('_vtid')
            pid_remapper = MultiHostIdRemapper('_vpid')
            for trace_dir_or_event in trace_dir_or_events:
                if not isinstance(trace_dir_or_event, str):
                    logger.warning(f'{trace_dir_or_event} is an invalid input. '
                                   f'Type of {trace_dir_or_event} is must be `str`.')
                    continue
                event_collection = EventCollection(
                    trace_dir_or_event, force_conversion)
                print('{} events found.'.format(len(event_collection)))

                common = LttngEventFilter.Common()
                begin, end = event_collection.time_range()
                common.start_time, common.end_time = begin, end

                # Offset is obtained for conversion from
                # the monotonic clock time to the system time.
                for event in event_collection:
                    event_name = event[LttngEventFilter.NAME]
                    if event_name == 'ros2_caret:caret_init':
                        offset = Ros2Handler.get_monotonic_to_system_offset(event)
                        break

                handler = Ros2Handler(data, offset)

                init_events = []
                run_events = []
                filtered_event_count = 0

                # distribute all trace events to init_events and run_events
                init_event_names = set(Lttng._prioritized_init_events)
                for event in event_collection:
                    event_name = event[LttngEventFilter.NAME]
                    if len(event_filters) > 0 and \
                            any(not f.accept(event, common) for f in event_filters):
                        continue
                    if event_name in init_event_names:
                        init_events.append(event)
                    else:
                        run_events.append(event)
                    filtered_event_count += 1

                Lttng.apply_init_timestamp(init_events, offset)

                import functools
                init_events.sort(key=functools.cmp_to_key(Lttng._compare_init_event))
                handler.create_init_handler_map()
                for event in tqdm(
                        iter(init_events),
                        total=len(init_events),
                        desc='loading',
                        mininterval=1.0):
                    event_name = event[LttngEventFilter.NAME]
                    handler_ = handler.handler_map[event_name]
                    tid_remapper.remap(event)
                    pid_remapper.remap(event)
                    handler_(event)

                handler.create_runtime_handler_map()
                filtered_event_count = 0
                for event in tqdm(
                        iter(run_events),
                        total=len(run_events),
                        desc='loading',
                        mininterval=1.0):
                    if store_events:
                        event_dict = {
                            k: get_field(event, k) for k in event
                        }
                        events.append(event_dict)
                    filtered_event_count += 1
                    event_name = event[LttngEventFilter.NAME]
                    handler_ = handler.handler_map[event_name]
                    tid_remapper.remap(event)
                    pid_remapper.remap(event)
                    handler_(event)
                tid_remapper.change_host()
                pid_remapper.change_host()
                if len(event_filters) > 0:
                    print('filtered to {} events.'.format(filtered_event_count))
            data.finalize()
        else:
            # Note: giving events as arguments is used only for debugging.
            common = LttngEventFilter.Common()
            filtered_event_count = 0
            events = trace_dir_or_events

            # Offset is obtained for conversion from the monotonic clock time to the system time.
            for event in events:
                event_name = event[LttngEventFilter.NAME]
                if event_name == 'ros2_caret:caret_init':
                    offset = Ros2Handler.get_monotonic_to_system_offset(event)
                    break

            handler = Ros2Handler(data, offset)

            begin = events[0][LttngEventFilter.TIMESTAMP]
            end = events[-1][LttngEventFilter.TIMESTAMP]

            init_events = []
            run_events = []
            filtered_event_count = 0

            # distribute all trace events to init_events and run_events
            init_event_names = set(Lttng._prioritized_init_events)
            for event in events:
                event_name = event[LttngEventFilter.NAME]
                if len(event_filters) > 0 and \
                        any(not f.accept(event, common) for f in event_filters):
                    continue
                if event_name in init_event_names:
                    init_events.append(event)
                else:
                    run_events.append(event)
                filtered_event_count += 1

            Lttng.apply_init_timestamp(init_events, offset)

            import functools
            init_events.sort(key=functools.cmp_to_key(Lttng._compare_init_event))
            handler.create_init_handler_map()
            for event in init_events:
                event_name = event[LttngEventFilter.NAME]
                handler_ = handler.handler_map[event_name]
                handler_(event)

            handler.create_runtime_handler_map()
            for event in run_events:
                if store_events:
                    events.append(event)
                event_name = event[LttngEventFilter.NAME]
                handler_ = handler.handler_map[event_name]
                handler_(event)
            data.finalize()
            if len(event_filters) > 0:
                print('filtered to {} events.'.format(filtered_event_count))

        events_ = None if len(events) == 0 else events
        return data, events_, begin, end

    @staticmethod
    def apply_init_timestamp(
        events: list,
        monotonic_to_system_offset: int | None,
    ):
        for event in events:
            if monotonic_to_system_offset is not None:
                if 'init_timestamp' in event:
                    init_timestamp: int = event.pop('init_timestamp')
                    event['_timestamp'] = init_timestamp + monotonic_to_system_offset

    @staticmethod
    def _compare_init_event(
        event1: dict,
        event2: dict,
    ) -> int:
        # same timestamp
        if Lttng._prioritized_init_events.index(event2[LttngEventFilter.NAME]) < \
                Lttng._prioritized_init_events.index(event1[LttngEventFilter.NAME]):
            return 1
        if Lttng._prioritized_init_events.index(event2[LttngEventFilter.NAME]) > \
                Lttng._prioritized_init_events.index(event1[LttngEventFilter.NAME]):
            return -1
        if event2[LttngEventFilter.TIMESTAMP] < event1[LttngEventFilter.TIMESTAMP]:
            return 1
        if event2[LttngEventFilter.TIMESTAMP] > event1[LttngEventFilter.TIMESTAMP]:
            return -1
        return 0

    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[tuple[str | None, str | None]]:
        """
        Get node names and callback symbols from callback group id.

        Returns
        -------
        Sequence[tuple[str | None, str | None]]
            node names and callback symbols.
            tuple structure: (node_name, callback_symbol)

        """
        data_model_srv = DataModelService(self.data)
        cbg_addr = CallbackGroupId(callback_group_id).group_addr

        return data_model_srv.get_node_names_and_cb_symbols(cbg_addr)

    def get_nodes(
        self
    ) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            nodes info.

        """
        return self._info.get_nodes()

    def get_rmw_impl(
        self
    ) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            rmw_implementation

        """
        return self._info.get_rmw_impl()

    def get_executors(
        self
    ) -> Sequence[ExecutorValue]:
        """
        Get executors information.

        Returns
        -------
        Sequence[ExecutorValue]

        """
        return self._info.get_executors()

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback group information.

        Returns
        -------
        Sequence[CallbackGroupValue]

        """
        return self._info.get_callback_groups(node)

    def get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValueLttng]:
        """
        Get publishers information.

        Parameters
        ----------
        node : NodeValue
            target node.

        Returns
        -------
        Sequence[PublisherInfoLttng]

        """
        return self._info.get_publishers(node)

    def get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        """
        Get timers information.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[TimerValue]

        """
        return self._info.get_timers(node)

    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValueLttng]:
        """
        Get timer callback values.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[TimerCallbackValueLttng]

        """
        return self._info.get_timer_callbacks(node)

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValueLttng]:
        """
        Get subscription callbacks information.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[SubscriptionCallbackValueLttng]

        """
        return self._info.get_subscription_callbacks(node)

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValueLttng]:
        """
        Get service callbacks information.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[ServiceCallbackValueLttng]

        """
        return self._info.get_service_callbacks(node)

    def get_publisher_qos(
        self,
        pub: PublisherValueLttng
    ) -> Qos:
        """
        Get publisher qos.

        Parameters
        ----------
        pub : PublisherValueLttng
            target publisher

        Returns
        -------
        Qos

        """
        return self._info.get_publisher_qos(pub)

    def get_subscription_qos(
        self,
        sub: SubscriptionCallbackValueLttng
    ) -> Qos:
        """
        Get subscription qos.

        Parameters
        ----------
        sub : SubscriptionCallbackValueLttng
            target subscription

        Returns
        -------
        Qos

        """
        return self._info.get_subscription_qos(sub)

    def get_sim_time_converter(
        self,
        min_ns: float,
        max_ns: float
    ) -> ClockConverter:
        records: RecordsInterface = self._source.system_and_sim_times
        system_times = records.get_column_series('system_time')
        sim_times = records.get_column_series('sim_time')
        system_times_filtered = []
        sim_times_filtered = []
        for system_time, sim_time in zip(system_times, sim_times):
            if system_time is not None and sim_time is not None:
                if min_ns <= system_time <= max_ns:
                    system_times_filtered.append(system_time)
                    sim_times_filtered.append(sim_time)

        try:
            return ClockConverter.create_from_series(system_times_filtered, sim_times_filtered)
        except InvalidArgumentError:
            raise InvalidArgumentError(
                'Failed to load sim_time. Please measure again with clock_recorder running.')

    def get_count(
        self,
        groupby: list[str] | None = None
    ) -> pd.DataFrame:
        groupby = groupby or ['trace_point']
        return self._counter.get_count(groupby)

    def get_trace_range(
        self
    ) -> tuple[datetime, datetime]:
        """
        Get trace range.

        Returns
        -------
        trace_range: tuple[datetime, datetime]
            Trace begin time and trace end time.

        """
        return (datetime.fromtimestamp(self._begin * 1.0e-9),
                datetime.fromtimestamp(self._end * 1.0e-9))

    def get_trace_creation_datetime(
        self
    ) -> datetime:
        """
        Get trace creation datetime.

        Returns
        -------
        trace_creation_datetime: datetime
            Date and time the trace data was created.

        """
        return datetime.fromtimestamp(self._begin * 1.0e-9)

    def compose_intra_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose intra process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            Columns

            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - message_timestamp

        """
        return self._source.intra_proc_comm_records.clone()

    def compose_callback_records(
        self,
    ) -> RecordsInterface:
        """
        Compose callback records of all communications in one records.

        Returns
        -------
        RecordsInterface
            Columns

            - callback_start_timestamp
            - callback_end_timestamp
            - is_intra_process
            - callback_object

        """
        return self._source.callback_records.clone()

    def compose_publish_records(
        self,
    ) -> RecordsInterface:
        return self._source.publish_records.clone()

    def compose_subscribe_records(
        self,
    ) -> RecordsInterface:
        return self._source.subscribe_records.clone()

    def create_timer_events_factory(
        self,
        timer_callback: TimerCallbackValueLttng
    ) -> EventsFactory:
        return self._source.create_timer_events_factory(timer_callback)

    def compose_tilde_publish_records(
        self,
    ) -> RecordsInterface:
        return self._source.tilde_publish_records.clone()

    def compose_tilde_subscribe_records(
        self,
    ) -> RecordsInterface:
        return self._source.tilde_subscribe_records.clone()

    def compose_path_beginning_records(
        self
    ) -> RecordsInterface:
        """
        Compose callback records.

        Used to evaluate the beginning node of a path.

        Returns
        -------
        RecordsInterface
            Columns

            - callback_start_timestamp
            - rclcpp_publish_timestamp
            - callback_object
            - publisher_object

        """
        return self._source.path_beginning_records.clone()
