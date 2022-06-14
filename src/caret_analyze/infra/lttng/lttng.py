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

from abc import ABCMeta, abstractmethod
from logging import getLogger

from typing import Dict, List, Optional, Sequence, Tuple, Union

import bt2

from caret_analyze.value_objects.timer import TimerValue

import pandas as pd

from tracetools_analysis.loading import load_file

from .events_factory import EventsFactory
from .ros2_tracing.data_model import DataModel
from .ros2_tracing.processor import Ros2Handler
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ..infra_base import InfraBase
from ...common import ClockConverter
from ...exceptions import InvalidArgumentError
from ...record import RecordsInterface
from ...value_objects import CallbackGroupValue, ExecutorValue, NodeValue, NodeValueWithId, Qos

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


class Lttng(InfraBase):
    """
    Lttng data container class.

    This class is a singleton in order to retain information.
    The main processing is done by LttngInfo and RecordsSource.

    """

    _last_load_dir: Optional[str] = None
    _last_filters: Optional[List[LttngEventFilter]] = None

    def __init__(
        self,
        trace_dir_or_events: Union[str, Dict],
        force_conversion: bool = False,
        use_singleton_cache: bool = True,
        *,
        event_filters: Optional[List[LttngEventFilter]] = None,
        store_events: bool = False,
    ) -> None:
        from .lttng_info import LttngInfo
        from .records_source import RecordsSource
        from .event_counter import EventCounter

        if self._last_load_dir == trace_dir_or_events and use_singleton_cache is True and \
                event_filters == self._last_filters:
            return

        data, events = self._parse_lttng_data(
            trace_dir_or_events,
            force_conversion,
            event_filters or []
        )
        self._info = LttngInfo(data)
        self._source: RecordsSource = RecordsSource(data, self._info)
        self._counter = EventCounter(data)
        self.events = events if store_events else None

    def clear_singleton_cache(self) -> None:
        self._last_load_dir = None

    @staticmethod
    def _parse_lttng_data(
        trace_dir_or_events: Union[str, Dict],
        force_conversion: bool,
        event_filters: List[LttngEventFilter]
    ) -> Tuple[DataModel, Dict]:
        if isinstance(trace_dir_or_events, str):
            # Check for traces lost
            for msg in bt2.TraceCollectionMessageIterator(trace_dir_or_events):
                if(type(msg) is bt2._DiscardedEventsMessageConst):
                    msg = ('Tracer discarded '
                           f'{msg.count} events between '
                           f'{msg.beginning_default_clock_snapshot.ns_from_origin} and '
                           f'{msg.end_default_clock_snapshot.ns_from_origin}.')
                    logger.warning(msg)

            Lttng._last_load_dir = trace_dir_or_events
            Lttng._last_filters = event_filters
            events = load_file(trace_dir_or_events, force_conversion=force_conversion)
            print('{} events found.'.format(len(events)))
        else:
            events = trace_dir_or_events

        if len(event_filters) > 0:
            events = Lttng._filter_events(events, event_filters)
            print('filted to {} events.'.format(len(events)))

        handler = Ros2Handler.process(events)
        return handler.data, events

    @staticmethod
    def _filter_events(events: List[Event], filters: List[LttngEventFilter]):
        if len(events) == 0:
            return []

        common = LttngEventFilter.Common()
        common.start_time = events[0][LttngEventFilter.TIMESTAMP]
        common.end_time = events[-1][LttngEventFilter.TIMESTAMP]

        filtered = []
        from tqdm import tqdm
        for event in tqdm(events):
            if all(event_filter.accept(event, common) for event_filter in filters):
                filtered.append(event)

        return filtered

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
