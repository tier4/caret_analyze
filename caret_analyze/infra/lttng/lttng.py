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

from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Union

import bt2
import pandas as pd

from tracetools_analysis.loading import load_file

from .event_filter import (
    Event,
    LttngEventFilter,
)
from .records_post_process import post_process_records
from .ros2_tracing.data_model import DataModel
from .ros2_tracing.processor import Ros2Handler
from .value_objects import (
    ClientCallbackValueLttng,
    IntraProcessBufferValueLttng,
    PublisherValueLttng,
    ServiceCallbackValueLttng,
    SubscriptionCallbackValueLttng,
    SubscriptionValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from ..infra_base import InfraBase
from ...common import ClockConverter
from ...exceptions import InvalidRecordsError
from ...record import ColumnMapper, RecordsInterface
from ...value_objects import (
    BroadcastedTransformValue,
    CallbackGroupValue,
    CallbackStructValue,
    CommunicationStructValue,
    ExecutorValue,
    IntraProcessBufferStructValue,
    NodePathStructValue,
    NodeValue,
    PublisherStructValue,
    Qos,
    SubscriptionStructValue,
    TimerStructValue,
    TimerValue,
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)


logger = getLogger(__name__)


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
        from .bridge import LttngBridge
        from .records_source import RecordsSource
        from .event_counter import EventCounter

        event_filters = event_filters or []

        if self._last_load_dir == trace_dir_or_events and use_singleton_cache is True and \
                event_filters == self._last_filters:
            return

        data, events = self._parse_lttng_data(
            trace_dir_or_events,
            force_conversion,
            event_filters
        )
        post_process_records(data)
        self._info = LttngInfo(data)
        self._bridge = LttngBridge(self._info)
        self._source: RecordsSource = RecordsSource(data, self._bridge, self._info)
        self._counter = EventCounter(data, self._info)
        self.events = events if store_events else None
        self.tf_frame_id_mapper = self._init_tf_column_mapper()

    def clear_singleton_cache(self) -> None:
        self._last_load_dir = None

    def _init_tf_column_mapper(
        self,
    ) -> ColumnMapper:
        frame_id_mapper = ColumnMapper()

        frames = self.get_tf_frames()
        frame_names = \
            sorted({f.source_frame_id for f in frames} | {f.target_frame_id for f in frames})
        for i, frame_name in enumerate(frame_names):
            frame_id_mapper.add(i, frame_name)

        return frame_id_mapper

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

    def get_service_callbacks(self, node: NodeValue) -> Sequence[ServiceCallbackValueLttng]:
        return self._info.get_service_callbacks(node)

    def get_client_callbacks(self, node: NodeValue) -> Sequence[ClientCallbackValueLttng]:
        return self._info.get_client_callbacks(node)

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
    ) -> Sequence[NodeValue]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValue]
            nodes info.

        """
        return self._info.get_nodes()

    def get_node(
        self,
        node_name: str
    ) -> NodeValue:
        return self._info.get_node(node_name)

    def get_rmw_impl(
        self
    ) -> Optional[str]:
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

    def get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValueLttng]:
        """
        Get subscriptions information.

        Parameters
        ----------
        node : NodeValue
            target node.

        Returns
        -------
        Sequence[SubscriptionInfoLttng]

        """
        return self._info.get_subscriptions(node)

    def get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValueLttng]:
        return self._info.get_tf_broadcaster(node)

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
        pub: PublisherStructValue
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
        pubs = self._bridge.get_publishers(pub)
        return self._info.get_publisher_qos(pubs[0])

    def get_subscription_qos(
        self,
        sub: SubscriptionStructValue
    ) -> Qos:
        """
        Get subscription qos.

        Parameters
        ----------
        sub : SubscriptionValueLttng
            target subscription

        Returns
        -------
        Qos

        """
        sub_lttng = self._bridge.get_subscription_callback(sub.callback)
        return self._info.get_subscription_qos(sub_lttng)

    def get_sim_time_converter(
        self
    ) -> ClockConverter:
        records: RecordsInterface = self._source.system_and_sim_times

        if len(records) == 0:
            raise InvalidRecordsError(
                'Failed to load sim_time. Please measure again with clock_recorder running.'
            )

        system_times = records.get_column_series('system_time')
        sim_times = records.get_column_series('sim_time')
        system_times_filtered = [_ for _ in system_times if _ is not None]
        sim_times_filtered = [_ for _ in sim_times if _ is not None]
        return ClockConverter.create_from_series(system_times_filtered, sim_times_filtered)

    def get_ipc_buffers(
        self,
        node: NodeValue,
    ) -> Sequence[IntraProcessBufferValueLttng]:
        return self._info.get_ipc_buffers(node)

    def get_ipc_buffer_records(
        self,
        buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        return self._source.ipc_buffer_records(buffer)

    def get_count(
        self,
        groupby: Optional[List[str]] = None
    ) -> pd.DataFrame:
        groupby = groupby or ['trace_point']
        return self._counter.get_count(groupby)

    def get_var_pass_records(
        self,
        var_pass: VariablePassingStructValue
    ) -> RecordsInterface:
        return self._source.get_var_pass_records(var_pass)

    def get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValueLttng]:
        return self._info.get_tf_buffer(node)

    def get_tf_frames(
        self
    ) -> Sequence[BroadcastedTransformValue]:
        return self._info.get_tf_frames()

    def is_intra_process_communication(
        self,
        publisher: PublisherStructValue,
        subscription: SubscriptionStructValue,
    ) -> bool:
        publishers_lttng = self._bridge.get_publishers(publisher)
        subscription_lttng = self._bridge.get_subscription(subscription)
        return self._info.is_intra_process_communication(publishers_lttng, subscription_lttng)

    def get_inter_proc_tf_comm_records(
        self,
        comm: TransformCommunicationStructValue
    ) -> RecordsInterface:
        return self._source.get_inter_proc_tf_comm_records(comm)

    def get_inter_proc_comm_records(
        self,
        comm: CommunicationStructValue,
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
        return self._source.inter_proc_comm_records(comm)

    def get_intra_proc_comm_records(
        self,
        comm: CommunicationStructValue,
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
        # publisher: PublisherStructValue,
        # buffer: IntraProcessBufferStructValue,
        # callback: SubscriptionCallbackStructValue,
        return self._source.intra_proc_comm_records(comm)

    def get_callback_records(
        self,
        callback: CallbackStructValue,
    ) -> RecordsInterface:
        return self._source.callback_records(callback)

    def get_node_records(
        self,
        node_path: NodePathStructValue,
    ) -> RecordsInterface:
        return self._source.get_node_records(node_path)

    def get_publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        return self._source.publish_records(publisher)

    def get_intra_publish_records(
        self,
        publisher: PublisherStructValue,
    ) -> RecordsInterface:
        return self._source.intra_publish_records(publisher)

    def get_timer_callback(
        self,
        timer: TimerStructValue,
    ) -> RecordsInterface:
        return self._source.get_timer_records(timer)

    def get_subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        return self._source.subscribe_records(subscription)

    # def create_timer_events_factory(
    #     self,
    #     timer_callback: TimerCallbackValueLttng
    # ) -> EventsFactory:
    #     return self._source.create_timer_events_factory(timer_callback)

    # def compose_tilde_publish_records(
    #     self,
    # ) -> RecordsInterface:
    #     return self._source.tilde_publish_records

    # def compose_tilde_subscribe_records(
    #     self,
    # ) -> RecordsInterface:
    #     return self._source.tilde_subscribe_records

    def get_send_transform(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:
        return self._source.send_transform_records(broadcaster)

    def get_lookup_transform(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        return self._source.lookup_transform_records(buffer)

    def get_node_tilde(
        self,
        node: NodePathStructValue,
    ) -> RecordsInterface:

        raise NotImplementedError('')
        # tilde_records = self._provider.tilde_records(
        #     self._node_path.subscription, self._node_path.publisher)
        # sub_records = self._provider.subscribe_records(self._node_path.subscription)
        # pub_records = self._provider.publish_records(self._node_path.publisher)

        # left_stamp_key = Util.find_one(
        #     lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, sub_records.columns)
        # right_stamp_key = Util.find_one(
        #     lambda x: COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in x, sub_records.columns)

        # records = merge(
        #     left_records=sub_records,
        #     right_records=tilde_records,
        #     join_left_key=right_stamp_key,
        #     join_right_key=right_stamp_key,
        #     how='left',
        #     progress_label='binding tilde subscribe records.'
        # )

        # left_stamp_key = Util.find_one(
        #     lambda x: COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in x, records.columns)

        # records = merge(
        #     left_records=records,
        #     right_records=pub_records,
        #     join_left_key=left_stamp_key,
        #     join_right_key=left_stamp_key,
        #     # columns=Columns(records.columns + pub_records.columns).as_list(),
        #     how='left',
        #     progress_label='binding tilde publish records.'
        # )

        # columns = [
        #     Util.find_one(lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, records.columns),
        #     Util.find_one(lambda x: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in x, records.columns),
        # ]

        # drop_columns = list(set(records.columns) - set(columns))
        # records.drop_columns(drop_columns)
        # records.reindex(columns)

    def get_find_closest(
        self,
        buffer: TransformBufferValueLttng,
    ) -> RecordsInterface:
        return self._source.find_closest_records(self.tf_frame_id_mapper, buffer).clone()
