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

from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime
from logging import getLogger
import os
import pickle
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Sized, Tuple, Union

import bt2
import pandas as pd
from tqdm import tqdm

from .events_factory import EventsFactory
from .lttng_event_filter import LttngEventFilter
from .ros2_tracing.data_model import Ros2DataModel
from .ros2_tracing.data_model_service import DataModelService
from .ros2_tracing.processor import get_field, Ros2Handler
from .value_objects import (CallbackGroupId,
                            PublisherValueLttng,
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


class EventCollection(Iterable, Sized):

    def __init__(self, trace_dir: str, force_conversion: bool) -> None:
        if not os.path.exists(trace_dir):
            raise FileNotFoundError(f'Failed to found {trace_dir}')

        self._iterable_events: IterableEvents
        cache_path = self._cache_path(trace_dir)

        if os.path.exists(cache_path) and not force_conversion:
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

    def time_range(self) -> Tuple[int, int]:
        return self._iterable_events.time_range()

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

    @abstractmethod
    def time_range(self) -> Tuple[int, int]:
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

    def time_range(self) -> Tuple[int, int]:
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
            if(type(msg) is bt2._DiscardedEventsMessageConst):
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

        assert begin_msg is not None
        assert end_msg is not None
        # NOTE: Begin_time and end_time should be the same time as the PickleEventCollection.
        self._begin_time: int = begin_msg.default_clock_snapshot.ns_from_origin
        self._end_time: int = end_msg.default_clock_snapshot.ns_from_origin
        self._size = event_count
        self._events = self._to_dicts(events_path, event_count)

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

    def time_range(self) -> Tuple[int, int]:
        return self._begin_time, self._end_time

    @staticmethod
    def _to_dicts(trace_dir: str, count: int) -> List[Dict]:
        msg_it = bt2.TraceCollectionMessageIterator(trace_dir)
        events = []
        acceptable_tracepoints = set(Ros2Handler.get_trace_points())
        for msg in tqdm(msg_it, total=count, desc='converting'):
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


class Lttng(InfraBase):
    """
    Lttng data container class.

    This class is a singleton in order to retain information.
    The main processing is done by LttngInfo and RecordsSource.

    """

    def __init__(
        self,
        trace_dir_or_events: Union[str, List[Dict]],
        force_conversion: bool = False,
        *,
        event_filters: Optional[List[LttngEventFilter]] = None,
        store_events: bool = False,
        # TODO(hsgwa): change validate function to public "verify".
        validate: bool = True
    ) -> None:
        from .lttng_info import LttngInfo
        from .records_source import RecordsSource
        from .event_counter import EventCounter

        data, events, begin, end = self._parse_lttng_data(
            trace_dir_or_events,
            force_conversion,
            event_filters or [],
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
        trace_dir_or_events: Union[str, List[Dict]],
        force_conversion: bool,
        event_filters: List[LttngEventFilter],
        store_events: bool,

    ) -> Tuple[Ros2DataModel, Optional[List[Dict]], int, int]:

        data = Ros2DataModel()
        handler = Ros2Handler(data)
        events = []
        begin: int
        end: int

        if isinstance(trace_dir_or_events, str):
            event_collection = EventCollection(
                trace_dir_or_events, force_conversion)
            print('{} events found.'.format(len(event_collection)))

            common = LttngEventFilter.Common()
            begin, end = event_collection.time_range()
            common.start_time, common.end_time = begin, end

            filtered_event_count = 0
            for event in tqdm(
                    iter(event_collection),
                    total=len(event_collection),
                    desc='loading'):
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
            if len(event_filters) > 0:
                print('filtered to {} events.'.format(filtered_event_count))
        else:
            # Note: giving events as arguments is used only for debugging.
            assert isinstance(trace_dir_or_events, List)
            filtered_event_count = 0
            events = trace_dir_or_events
            begin = events[0][LttngEventFilter.TIMESTAMP]
            end = events[-1][LttngEventFilter.TIMESTAMP]

            for event in events:
                if len(event_filters) > 0 and \
                        any(not f.accept(event, common) for f in event_filters):
                    continue
                if store_events:
                    events.append(event)
                filtered_event_count += 1
                event_name = event[LttngEventFilter.NAME]
                handler_ = handler.handler_map[event_name]
                handler_(event)
            data.finalize()
            if len(event_filters) > 0:
                print('filtered to {} events.'.format(filtered_event_count))

        events_ = None if len(events) == 0 else events
        return data, events_, begin, end

    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[Tuple[Optional[str], Optional[str]]]:
        """
        Get node names and callback symbols from callback group id.

        Returns
        -------
        Sequence[Tuple[Optional[str], Optional[str]]]
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
        Get subscription callbacks information.

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

    def compose_inter_proc_comm_records(
        self,
    ) -> RecordsInterface:
        """
        Compose inter process communication records of all communications in one records.

        Returns
        -------
        RecordsInterface
            Columns

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
