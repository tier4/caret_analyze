# Copyright 2021 Research Institute of Systems Planning, Inc.
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
from functools import cached_property

import os
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
from logging import getLogger
from typing import Any, Dict, List, Optional, Sequence, Sized, Tuple, Union, Iterable, Iterator
import pickle

import bt2
import pandas as pd
from tqdm import tqdm

from .events_factory import EventsFactory
from .ros2_tracing.data_model import Ros2DataModel
from .ros2_tracing.processor import get_field, Ros2Handler
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ..infra_base import InfraBase
from ...common import ClockConverter
from ...exceptions import InvalidArgumentError
from ...record import RecordsInterface
from ...value_objects import  \
    CallbackGroupValue, ExecutorValue, NodeValue, NodeValueWithId, Qos, TimerValue

Event = Dict[str, int]

logger = getLogger(__name__)


class LttngEventFilter(metaclass=ABCMeta):
    NAME = '_name'
    TIMESTAMP = '_timestamp'
    CPU_ID = '_cpuid'
    VPID = 'vpid'
    VTID = 'vtid'

    class Common:
        start_time: int
        end_time: int

    @staticmethod
    def duration_filter(duration_s: float, offset_s: float) -> LttngEventFilter:
        return EventDurationFilter(duration_s, offset_s)

    @staticmethod
    def strip_filter(lsplit_s: Optional[float], rsplit_s: Optional[float]) -> LttngEventFilter:
        return EventStripFilter(lsplit_s, rsplit_s)

    @staticmethod
    def init_pass_filter() -> LttngEventFilter:
        return InitEventPassFilter()

    @abstractmethod
    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        pass


class InitEventPassFilter(LttngEventFilter):

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        init_events = {
            'ros2:rcl_init',
            'ros2:rcl_node_init',
            'ros2:rcl_publisher_init',
            'ros2:rcl_subscription_init',
            'ros2:rclcpp_subscription_init',
            'ros2:rclcpp_subscription_callback_added',
            'ros2:rcl_service_init',
            'ros2:rclcpp_service_callback_added',
            'ros2:rcl_client_init',
            'ros2:rcl_timer_init',
            'ros2:rclcpp_timer_callback_added',
            'ros2:rclcpp_timer_link_node',
            'ros2:rclcpp_callback_register',
            'ros2:rcl_lifecycle_state_machine_init',
            'ros2:rcl_lifecycle_transition',
            'ros2_caret:rmw_implementation',
            'ros2_caret:add_callback_group',
            'ros2_caret:add_callback_group_static_executor',
            'ros2_caret:construct_executor',
            'ros2_caret:construct_static_executor',
            'ros2_caret:callback_group_add_timer',
            'ros2_caret:callback_group_add_subscription',
            'ros2_caret:callback_group_add_service',
            'ros2_caret:callback_group_add_client',
            'ros2_caret:tilde_subscription_init',
            'ros2_caret:tilde_publisher_init',
            'ros2_caret:tilde_subscribe_added',
        }

        return event[self.NAME] in init_events


class EventStripFilter(LttngEventFilter):
    def __init__(
        self,
        lstrip_s: Optional[float],
        rstrip_s: Optional[float]
    ) -> None:
        self._lstrip = lstrip_s
        self._rstrip = rstrip_s
        self._init_events = InitEventPassFilter()

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        if self._init_events.accept(event, common):
            return True

        if self._lstrip:
            diff_ns = event[self.TIMESTAMP] - common.start_time
            diff_s = diff_ns * 1.0e-9
            if diff_s < self._lstrip:
                return False

        if self._rstrip:
            diff_ns = common.end_time - event[self.TIMESTAMP]
            diff_s = diff_ns * 1.0e-9
            if diff_s < self._rstrip:
                return False
        return True


class EventDurationFilter(LttngEventFilter):

    def __init__(self, duration_s: float, offset_s: float) -> None:
        self._duration = duration_s
        self._offset = offset_s
        self._init_events = InitEventPassFilter()

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        if self._init_events.accept(event, common):
            return True

        elapsed_ns = event[self.TIMESTAMP] - common.start_time
        elapsed_s = elapsed_ns * 1.0e-9
        return self._offset <= elapsed_s and elapsed_s < (self._offset + self._duration)


class EventCollection(Iterable, Sized):

    def __init__(self, trace_dir: str) -> None:
        self._iterable_events: IterableEvents
        cache_path = self._cache_path(trace_dir)

        if os.path.exists(cache_path):
            logger.info('Found converted file.')
            self._iterable_events = PickleEventCollection(cache_path)
        else:
            self._iterable_events = CtfEventCollection(trace_dir)
            self._store_cache(self._iterable_events, cache_path)
            logger.info(f'Converted to {cache_path}')

    @staticmethod
    def _store_cache(iterable_events: IterableEvents, path) -> None:
        with open(path, mode='wb') as f:
            pickle.dump(iterable_events.events, f)

    def __len__(self) -> int:
        return len(self._iterable_events)

    def __iter__(self) -> Iterator[Dict]:
        return iter(self._iterable_events)

    def _cache_path(self, events_path: str) -> str:
        return os.path.join(events_path, 'caret_converted')


class IterableEvents(Iterable, Sized, metaclass=ABCMeta):

    @abstractmethod
    def __iter__(self) -> Iterator[Dict]:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractproperty
    def events(self) -> List[Dict]:
        pass


class PickleEventCollection(IterableEvents):

    def __init__(self, events_path: str) -> None:
        with open(events_path, mode='rb') as f:
            self._events = pickle.load(f)

    def __iter__(self) -> Iterator[Dict]:
        return iter(self._events)

    def __len__(self) -> int:
        return len(self._events)

    @property
    def events(self) -> List[Dict]:
        return self._events


class CtfEventCollection(IterableEvents):

    def __init__(self, events_path: str) -> None:
        event_count = 0
        for msg in bt2.TraceCollectionMessageIterator(events_path):
            if type(msg) is bt2._EventMessageConst:
                event_count += 1

            if (not Lttng._last_trace_begin_time
                    and type(msg) is bt2._PacketBeginningMessageConst):
                Lttng._last_trace_begin_time = msg.default_clock_snapshot.ns_from_origin
            elif type(msg) is bt2._PacketEndMessageConst:
                Lttng._last_trace_end_time = msg.default_clock_snapshot.ns_from_origin
            # Check for traces lost
            elif(type(msg) is bt2._DiscardedEventsMessageConst):
                msg = ('Tracer discarded '
                        f'{msg.count} events between '
                        f'{msg.beginning_default_clock_snapshot.ns_from_origin} and '
                        f'{msg.end_default_clock_snapshot.ns_from_origin}.')
                logger.warning(msg)

        self._size = event_count
        self._events = self._to_dicts(events_path)

    def __iter__(self) -> Iterator[Dict]:
        return iter(self._events)

    def __len__(self) -> int:
        return self._size

    @staticmethod
    def _to_event(msg: Any) -> Dict[str, Any]:
        event: Dict[str, Any] = {}
        event[LttngEventFilter.NAME] = msg.event.name
        event['timestamp'] = msg.default_clock_snapshot.ns_from_origin
        event.update(msg.event.payload_field)
        event.update(msg.event.common_context_field)
        return event

    @property
    def events(self) -> List[Dict]:
        return self._events

    @staticmethod
    def _to_dicts(trace_dir: str) -> List[Dict]:
        msg_it = bt2.TraceCollectionMessageIterator(trace_dir)
        events = []
        acceptable_tracepoints = set(Ros2Handler.get_trace_points())
        for msg in msg_it:
            if type(msg) is not bt2._EventMessageConst:
                continue
            if msg.event.name not in acceptable_tracepoints:
                continue

            event = CtfEventCollection._to_event(msg)
            event_dict = {
                k: get_field(event, k) for k in event
            }
            events.append(event_dict)

        return events


class Lttng(InfraBase):
    """
    Lttng data container class.

    This class is a singleton in order to retain information.
    The main processing is done by LttngInfo and RecordsSource.

    """

    _last_load_dir: Optional[str] = None
    _last_filters: Optional[List[LttngEventFilter]] = None
    _last_trace_begin_time: Optional[int] = None
    _last_trace_end_time: Optional[int] = None

    def __init__(
        self,
        trace_dir_or_events: Union[str, Dict],
        *,
        event_filters: Optional[List[LttngEventFilter]] = None,
        store_events: bool = False,
        validate: bool = True  # TODO(hsgwa): change validate function to public "verify".
    ) -> None:
        from .lttng_info import LttngInfo
        from .records_source import RecordsSource
        from .event_counter import EventCounter

        data, events = self._parse_lttng_data(
            trace_dir_or_events,
            event_filters or [],
            store_events
        )
        self.data = data
        self._info = LttngInfo(data)
        self._source: RecordsSource = RecordsSource(data, self._info)
        self._counter = EventCounter(data, validate=validate)
        self.events = events

    @staticmethod
    def _parse_lttng_data(
        trace_dir_or_events: Union[str, Dict],
        event_filters: List[LttngEventFilter],
        store_events: bool
    ) -> Tuple[Any, Dict]:

        data = Ros2DataModel()
        handler = Ros2Handler(data)
        events = []

        if isinstance(trace_dir_or_events, str):
            Lttng._last_trace_begin_time = None
            event_collection = EventCollection(trace_dir_or_events)
            print('{} events found.'.format(len(event_collection)))

            common = LttngEventFilter.Common()
            common.start_time = Lttng._last_trace_begin_time
            common.end_time = Lttng._last_trace_end_time

            filtered_event_count = 0
            for event in tqdm(iter(event_collection), total=len(event_collection)):
                if len(event_filters) > 0 and \
                        any(not f.accept(event, common) for f in event_filters):
                    continue
                if store_events:
                    event_dict = {
                        k: get_field(event, k) for k in event
                    }
                    events.append(event_dict)
                filtered_event_count += 1
                event_name = event[LttngEventFilter.NAME]
                handler_ = handler.handler_map[event_name]
                handler_(event)

            data.finalize()
            Lttng._last_load_dir = trace_dir_or_events
            Lttng._last_filters = event_filters
            if len(event_filters) > 0:
                print('filtered to {} events.'.format(filtered_event_count))
        else:
            events = trace_dir_or_events
            for event in events:
                if len(event_filters) > 0 and \
                        any(not f.accept(event, common) for f in event_filters):
                    continue
                if store_events:
                    events.append(event)
                filtered_event_count += 1
                handler_ = handler.handler_map[msg.event.name]
                handler_(event)

        events_ = None if len(events) == 0 else events
        return data, events_

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
        Sequence[ExecutorInfo]

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
        Sequence[TimerCallbackInfoLttng]

        """
        return self._info.get_timer_callbacks(node)

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValueLttng]:
        """
        Get subscription callbacks infomation.

        Parameters
        ----------
        node : NodeValue
            target node name.

        Returns
        -------
        Sequence[SubscriptionCallbackInfoLttng]

        """
        return self._info.get_subscription_callbacks(node)

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
        self
    ) -> ClockConverter:
        records: RecordsInterface = self._source.system_and_sim_times
        system_times = records.get_column_series('system_time')
        sim_times = records.get_column_series('sim_time')
        system_times_filtered = [_ for _ in system_times if _ is not None]
        sim_times_filtered = [_ for _ in sim_times if _ is not None]
        try:
            return ClockConverter.create_from_series(system_times_filtered, sim_times_filtered)
        except InvalidArgumentError:
            raise InvalidArgumentError(
                'Failed to load sim_time. Please measure again with clock_recorder running.')

    def get_count(
        self,
        groupby: Optional[List[str]] = None
    ) -> pd.DataFrame:
        groupby = groupby or ['trace_point']
        return self._counter.get_count(groupby)

    def get_trace_range(
        self
    ) -> Tuple[datetime, datetime]:
        """
        Get trace range.

        Returns
        -------
        trace_range: Tuple[datetime, datetime]
            Trace begin time and trace end time.

        """
        return (datetime.fromtimestamp(Lttng._last_trace_begin_time * 1.0e-9),
                datetime.fromtimestamp(Lttng._last_trace_end_time * 1.0e-9))

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
        return datetime.fromtimestamp(Lttng._last_trace_begin_time * 1.0e-9)

    def compose_inter_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose inter process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            columns
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp (Optional)
            - dds_write_timestamp (Optional)

        """
        return self._source.inter_proc_comm_records.clone()

    def compose_intra_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose intra process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            columns:
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
            columns:
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
