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

from functools import lru_cache
from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Union
from caret_analyze.record.interface import RecordInterface

from .lttng import Lttng
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ...common import ClockConverter, Util
from ...exceptions import (InvalidArgumentError,
                           UnsupportedNodeRecordsError,
                           UnsupportedTypeError)
from ...infra.interface import RuntimeDataProvider
from ...infra.lttng.column_names import COLUMN_NAME
from ...record import (
    Column,
    Columns,
    merge,
    merge_sequencial,
    RecordsFactory,
    RecordsInterface)
from ...value_objects import (
    CallbackChain,
    CallbackStructValue,
    MessageContext,
    MessageContextType,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    CommunicationStructValue,
    InheritUniqueStamp,
    NodePathStructValue,
    PublisherStructValue,
    Qos,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    Tilde,
    TimerCallbackStructValue,
    TimerStructValue,
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformCommunicationStructValue,
    TransformValue,
    UseLatestMessage,
    VariablePassingStructValue,
)

logger = getLogger(__name__)


class RecordsProviderLttng(RuntimeDataProvider):
    """
    Records are processed and measurement results are calculated.

    In addition to merging, filtering and other operations are performed here.

    """

    def __init__(
        self,
        lttng: Lttng,
    ) -> None:
        self._lttng = lttng
        self._source = FilteredRecordsSource(lttng)
        self._helper = RecordsProviderLttngHelper(lttng)

    def communication_records(
        self,
        comm_val: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Provide communication records.

        Parameters
        ----------
        comm_info : CommunicationStructInfo
            communicadtion info.

        Returns
        -------
        RecordsInterface
            Columns:
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_publish_timestamp (Optional)
            - [callback_name]/callback_start_timestamp

        """
        assert comm_val.subscription_callback_name is not None

        if self.is_intra_process_communication(comm_val):
            return self._compose_intra_proc_comm_records(comm_val)

        return self._compose_inter_proc_comm_records(comm_val)

    def node_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:
        if node_path_val.message_context is None:
            # dummy record
            msg = 'message context is None. return dummy record. '
            msg += f'node_name: {node_path_val.node_name}'
            logger.info(msg)
            assert False
            return RecordsFactory.create_instance()

        if node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN:
            return NodeRecordsCallbackChain(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.INHERIT_UNIQUE_STAMP:
            return NodeRecordsInheritUniqueTimestamp(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.USE_LATEST_MESSAGE:
            return NodeRecordsUseLatestMessage(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.TILDE:
            return NodeRecordsTilde(self, node_path_val).to_records()

        raise UnsupportedNodeRecordsError(
            'Unknown message context. '
            f'message_context = {node_path_val.message_context.context_type.type_name}'
        )

    def callback_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        """
        Return callback duration records.

        Parameters
        ----------
        callback_val : CallbackStructValue
            target callback value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [callback_name]/callback_end_timestamp

        """
        callback_objects = self._helper.get_callback_objects(callback)
        callback_records = self._source.callback_records(*callback_objects)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP
        ]
        self._format(callback_records, columns)
        self._rename_column(callback_records, callback.callback_name, None, callback.node_name)
        callback_records.drop_columns([COLUMN_NAME.CALLBACK_OBJECT])

        return callback_records

    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        Raises
        ------
        InvalidArgumentError

        """
        callback = subscription.callback
        assert callback is not None

        tilde_subscription = self._helper.get_tilde_subscription(callback)

        if tilde_subscription is None:
            return self._subscribe_records(subscription)

        return self._subscribe_records_with_tilde(subscription)

    def _subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        Raises
        ------
        InvalidArgumentError

        """
        callback = subscription.callback
        if callback is None:
            raise InvalidArgumentError(
                'callback_value is None. '
                f'node_name: {subscription.node_name}'
                f'callback_name: {subscription.callback_name}'
                f'topic_name: {subscription.topic_name}'
            )

        callback_objects = self._helper.get_subscription_callback_objects(callback)
        sub_records = self._source.sub_records(*callback_objects)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
        ]
        self._format(sub_records, columns)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            subscription.node_name
        )

        return sub_records

    def tf_broadcast_records(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:
        """
        Compose transform broadcast records.

        Parameters
        ----------
        broadcaster : TransformBroadcasterStructValue
            target bradcaster
        transform : Optional[TransformValue]
            target transform

        Returns
        -------
        RecordsInterface
            Columns
            - frame_id
            - child_frame_id
            - same as publlish records

        """
        broadcaster_handler = self._helper.get_tf_broadcaster_handler(broadcaster)
        records = self._source.send_transform(broadcaster_handler, broadcaster.transform)

        pub_records = self.publish_records(broadcaster.publisher)
        pub_records.drop_columns([
            column
            for column
            in pub_records.column_names
            if COLUMN_NAME.MESSAGE_TIMESTAMP in column or
            COLUMN_NAME.SOURCE_TIMESTAMP in column
        ])
        records = merge_sequencial(
            left_records=records,
            right_records=pub_records,
            left_stamp_key='send_transform_timestamp',
            right_stamp_key=pub_records.columns[0].column_name,
            join_left_key=None,
            join_right_key=None,
            how='inner'
        )
        records.drop_columns(['send_transform_timestamp', 'broadcaster'])
        return records

    def tf_communication_records(
        self,
        communication: TransformCommunicationStructValue
    ) -> RecordsInterface:
        set_to_lookup_records = self.tf_set_lookup_records(communication.buffer)
        send_records = self.tf_broadcast_records(communication.broadcaster)
        from ...record.column import get_column_name
        comm_records = merge(
            left_records=send_records,
            right_records=set_to_lookup_records,
            join_left_key=get_column_name(send_records.columns, 'tf_timestamp'),
            join_right_key=get_column_name(set_to_lookup_records.columns, 'tf_timestamp'),
            how='left'
        )
        comm_records.drop_columns([
            'frame_id',
            'child_frame_id',
            'tf_timestamp',
            get_column_name(comm_records.columns, 'find_closest_timestamp'),
            get_column_name(comm_records.columns, 'callback_end_timestamp'),
        ])
        return comm_records

    def tf_lookup_records(
        self,
        frame_buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        buffer_handle = self._helper.get_tf_buffer_handler(frame_buffer)
        records = self._source.lookup_transform(buffer_handle, frame_buffer.lookup_transform)
        records.drop_columns(['tf_buffer_core',
                              'frame_id',
                              'child_frame_id'])
        self._rename_column(
            records,
            None,
            frame_buffer.topic_name,
            frame_buffer.lookup_node_name)
        return records

    def tf_set_records(
        self,
        buffer: TransformFrameBufferStructValue,
        transform: TransformValue
    ) -> RecordsInterface:
        """
        Compose transform set records.

        Parameters
        ----------
        buffer : TransformFrameBufferStructValue
            target buffer
        transform : TransformValue
            target transform

        Returns
        -------
        RecordsInterface
            Columns
            - frame_id
            - child_frame_id
            - same as publlish records

        """
        buffer_handle = self._helper.get_tf_buffer_handler(buffer)

        filtered = self._source.set_transform(buffer_handle, transform)

        filtered.drop_columns(['tf_buffer_core',
                               'frame_id',
                               'child_frame_id'])
        self._rename_column(
            filtered,
            '/transform_listener_impl/callback',
            '/tf',
            '/transform_listener_impl')

        return filtered

    def _tf_find_closest_records(
        self,
        buffer: TransformFrameBufferStructValue
    ) -> RecordsInterface:
        buffer_handle = self._helper.get_tf_buffer_handler(buffer)

        filtered = self._source.find_closest(buffer_handle)

        from ...record.column import get_column
        frame_id_column = get_column(filtered.columns, 'frame_id')
        child_frame_id_column = get_column(filtered.columns, 'child_frame_id')

        def is_target_frame(record: RecordInterface):
            frame_id = frame_id_column.get_mapped(record.get('frame_id'))
            child_frame_id = child_frame_id_column.get_mapped(record.get('child_frame_id'))
            return frame_id == buffer.listen_frame_id and \
                child_frame_id == buffer.listen_child_frame_id
        filtered.filter_if(is_target_frame)

        filtered.drop_columns(['tf_buffer_core',
                               'frame_id',
                               'child_frame_id'])
        node_name = buffer.lookup_node_name
        filtered.rename_columns({
            'stamp': 'tf_timestamp',
            'find_closest_timestamp': f'{node_name}/find_closest_timestamp',
        })

        return filtered

    def tf_set_lookup_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        lookup_records = self.tf_lookup_records(buffer)
        # lookup_records.rename_columns(
        #     {
        #         'frame_id': 'lookup_frame_id',
        #         'child_frame_id': 'lookup_child_frame_id'
        #     }
        # )
        from ...record.column import get_column_name

        closest = self._tf_find_closest_records(buffer)

        # TODO(hsgwa): 遅延ありの２つ目の座標版も追加する。
        # ここはスレッドIDを使わないとダメなやつか？
        records = merge_sequencial(
            left_records=closest,
            right_records=lookup_records,
            left_stamp_key=get_column_name(
                closest.columns, 'find_closest_timestamp'),
            right_stamp_key=get_column_name(
                lookup_records.columns, 'lookup_transform_end_timestamp'),
            join_left_key=None,
            join_right_key=None,
            how='left_use_latest'
        )

        set_records = self.tf_set_records(buffer, buffer.listen_transform)
        records = merge(
            left_records=set_records,
            right_records=records,
            join_left_key=get_column_name(set_records.columns, 'tf_timestamp'),
            join_right_key=get_column_name(records.columns, 'tf_timestamp'),
            how='left',
        )

        # group_records = records.groupby(['set_frame_id', 'set_child_frame_id'])
        # records_ = RecordsFactory.create_instance(
        #     None, Columns(
        #         [
        #             Column('callback_start_timestamp'),
        #             Column('set_transform_timestamp'),
        #             Column('tf_timestamp'),
        #             Column('callback_end_timestamp')
        #         ] + records.columns).as_list())

        records.drop_columns(['find_closest_timestamp'])
        return records

    def _subscribe_records_with_tilde(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp
            - [topic_name]/tilde_subscribe_timestamp
            - [topic_name]/tilde_message_id

        Raises
        ------
        InvalidArgumentError

        """
        callback = subscription.callback
        if callback is None:
            raise InvalidArgumentError(
                'callback_value is None. '
                f'node_name: {subscription.node_name}'
                f'callback_name: {subscription.callback_name}'
                f'topic_name: {subscription.topic_name}'
            )

        callback_objects = self._helper.get_subscription_callback_objects(callback)
        sub_records = self._source.sub_records(*callback_objects)

        tilde_subscription = self._helper.get_tilde_subscription(callback)

        if tilde_subscription is not None:
            tilde_records = self._source.tilde_subscribe_records(tilde_subscription)

            sub_records = merge_sequencial(
                left_records=sub_records,
                right_records=tilde_records,
                left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
                join_left_key=None,
                join_right_key=None,
                how='left',
                progress_label='binding: tilde_records',
            )

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
            COLUMN_NAME.TILDE_MESSAGE_ID,
        ]
        self._format(sub_records, columns)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            subscription.node_name,
        )

        return sub_records

    def _publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publish : PublisherStructValue
            target publisher

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        """
        publisher_handles = self._helper.get_publisher_handles(publisher)
        assert len(publisher_handles) > 0
        pub_records = self._source.publish_records(publisher_handles)

        columns = []
        columns.append(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.column_names:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.column_names:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

        self._format(pub_records, columns)
        self._rename_column(pub_records, None, publisher.topic_name, publisher.node_name)

        return pub_records

    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publish : PublisherStructValue
            target publisher

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rclcpp_intra_publish_timestamp (Optional)
            - [topic_name]/rclcpp_inter_publish_timestamp (Optional)
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp (Optional)
            ---
            - [topic_name]/tilde_publish_timestamp (Optional)
            - [topic_name]/tilde_message_id (Optional)

        """
        tilde_publishers = self._helper.get_tilde_publishers(publisher)
        if len(tilde_publishers) == 0:
            return self._publish_records(publisher)

        return self._publish_records_with_tilde(publisher)

    def _publish_records_with_tilde(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publish : PublisherStructValue
            target publisher

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rclcpp_intra_publish_timestamp
            - [topic_name]/rclcpp_inter_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp
            - [topic_name]/tilde_publish_timestamp
            - [topic_name]/tilde_message_id

        """
        publisher_handles = self._helper.get_publisher_handles(publisher)
        pub_records = self._source.publish_records(publisher_handles)

        tilde_publishers = self._helper.get_tilde_publishers(publisher)
        tilde_records = self._source.tilde_publish_records(tilde_publishers)

        pub_records = merge_sequencial(
            left_records=tilde_records,
            right_records=pub_records,
            left_stamp_key='tilde_publish_timestamp',
            right_stamp_key='rclcpp_publish_timestamp',
            join_left_key=None,
            join_right_key=None,
            how='right',
            progress_label='binding: tilde_records',
        )

        columns = [
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)
        columns.append(COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP)
        columns.append(COLUMN_NAME.TILDE_MESSAGE_ID)

        self._format(pub_records, columns)
        self._rename_column(pub_records, None, publisher.topic_name, publisher.node_name)

        return pub_records

    def timer_records(self, timer: TimerStructValue) -> RecordsInterface:
        """
        Return timer records.

        Parameters
        ----------
        timer : TimerStructValue
            [description]

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/timer_event
            - [callback_name]/callback_start
            - [callback_name]/callback_end

        """
        assert timer.callback is not None
        timer_lttng_cb = self._helper.get_lttng_timer(timer.callback)

        timer_events_factory = self._lttng.create_timer_events_factory(timer_lttng_cb)
        callback_records = self.callback_records(timer.callback)
        last_record = callback_records.data[-1]
        last_callback_start = last_record.get(callback_records.column_names[0])
        timer_events = timer_events_factory.create(last_callback_start)
        timer_records = merge_sequencial(
            left_records=timer_events,
            right_records=callback_records,
            left_stamp_key=COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            right_stamp_key=callback_records.column_names[0],
            join_left_key=None,
            join_right_key=None,
            how='left'
        )

        columns = [
            COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            callback_records.column_names[0],
            callback_records.column_names[1],
        ]

        self._format(timer_records, columns)
        self._rename_column(timer_records, timer.callback_name, None, timer.node_name)

        return timer_records

    def tilde_records(
        self,
        subscription: SubscriptionStructValue,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        assert subscription.callback is not None

        publisher_addrs = self._helper.get_tilde_publishers(publisher)
        subscription_addr = self._helper.get_tilde_subscription(subscription.callback)

        assert len(publisher_addrs) > 0
        assert subscription_addr is not None

        pub_records = self._source.tilde_publish_records(publisher_addrs)
        sub_records = self._source.tilde_subscribe_records(subscription_addr)

        records = merge(
            left_records=sub_records,
            right_records=pub_records,
            join_left_key=COLUMN_NAME.TILDE_MESSAGE_ID,
            join_right_key=COLUMN_NAME.TILDE_MESSAGE_ID,
            how='left',
            progress_label='binding: tilde pub and sub records'
        )

        columns = [
            COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
            COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP
        ]
        self._format(records, columns)

        self._rename_column_tilde(records, subscription.topic_name, publisher.topic_name)

        return records

    def get_rmw_implementation(self) -> str:
        return self._lttng.get_rmw_impl()

    def get_qos(
        self,
        pub_sub: Union[PublisherStructValue, SubscriptionStructValue]
    ) -> Qos:
        if isinstance(pub_sub, SubscriptionStructValue):
            sub_cb = pub_sub.callback
            if sub_cb is None:
                raise InvalidArgumentError('Failed to get callback information.'
                                           'pub.callback is None')
            sub_cb_lttng = self._helper.get_lttng_subscription(sub_cb)
            return self._lttng.get_subscription_qos(sub_cb_lttng)

        pubs_lttng = self._helper.get_lttng_publishers(pub_sub)
        if len(pubs_lttng) == 0:
            raise InvalidArgumentError('No publisher matching the criteria was found.')
        if len(pubs_lttng) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        return self._lttng.get_publisher_qos(pubs_lttng[0])

    def get_sim_time_converter(self) -> ClockConverter:
        return self._lttng.get_sim_time_converter()

    def variable_passing_records(
        self,
        variable_passing_info: VariablePassingStructValue
    ) -> RecordsInterface:
        """
        Return variable passing records.

        Parameters
        ----------
        variable_passing_info : VariablePassingStructInfo
            target variable passing info.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_end_timestamp
            - [callback_name]/callback_start_timestamp

        """
        read_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_read)
        write_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_write)

        read_records.drop_columns([read_records.column_names[-1]])  # callback end
        write_records.drop_columns([write_records.column_names[0]])  # callback_start

        columns = [
            write_records.column_names[0],
            read_records.column_names[0],
        ]

        merged_records = merge_sequencial(
            left_records=write_records,
            right_records=read_records,
            left_stamp_key=columns[0],
            right_stamp_key=columns[1],
            join_left_key=None,
            join_right_key=None,
            how='left_use_latest',
            progress_label='binding: callback_end and callback_start'
        )

        merged_records.sort(columns[0])
        self._format(merged_records, columns)
        return merged_records

    def is_intra_process_communication(
        self,
        communication_value: CommunicationStructValue
    ) -> Optional[bool]:
        return self._lttng.is_intra_process_communication(communication_value)

    def _verify_trace_points(
        self,
        node_name: str,
        trace_points: str
    ) -> bool:
        df = self._lttng.get_count(['node_name', 'trace_point'])
        df = df.reset_index()
        node_df = df.loc[df['node_name'] == node_name]
        trace_point_df = node_df[node_df['trace_point'] == trace_points]
        if trace_point_df['size'].empty:
            return False
        elif trace_point_df['size'].item() <= 0:
            return False
        else:
            return True

    def verify_communication(
        self,
        communication: CommunicationStructValue,
    ) -> bool:
        is_intra_proc = self.is_intra_process_communication(communication)
        if is_intra_proc is True:
            pub_node = communication.publish_node.node_name
            sub_node = communication.subscribe_node.node_name
            pub_result = self._verify_trace_points(pub_node, 'ros2:rclcpp_intra_publish')
            sub_result = self._verify_trace_points(
                sub_node,
                'ros2:dispatch_intra_process_subscription_callback'
            )

        elif is_intra_proc is False:
            pub_result = True
            sub_node = communication.subscribe_node.node_name
            sub_result = self._verify_trace_points(
                sub_node,
                'ros2:dispatch_subscription_callback'
            )

        if not pub_result:
            logger.warning(f"'caret/rclcpp' may not be used in publisher of '{pub_node}'.")
            return False
        if not sub_result:
            logger.warning(f"'caret/rclcpp' may not be used in subscriber of '{sub_node}'.")
            return False
        return True

    def _compose_intra_proc_comm_records(
        self,
        comm_info: CommunicationStructValue,
    ) -> RecordsInterface:
        """
        Compose intra process communication records.

        Parameters
        ----------
        comm_info : CommunicationStructInfo
            Target communication info.

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [callback_name]/callback_start_timestamp

        """
        publisher = comm_info.publisher
        subscription_cb = comm_info.subscription.callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object_intra = self._helper.get_subscription_callback_object_intra(
            subscription_cb)

        records = self._source.intra_comm_records(publisher_handles, callback_object_intra)

        columns = [
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        ]
        self._format(records, columns)

        self._rename_column(records, subscription_cb.callback_name, comm_info.topic_name, None)

        return records

    def _compose_inter_proc_comm_records(
        self,
        comm_value: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Composer intar process communication records.

        Parameters
        ----------
        comm_value : CommunicationStructValue
            target communication value.

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [callback_name_name]/callback_start_timestamp

        """
        publisher = comm_value.publisher
        subscription_cb = comm_value.subscription_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object = self._helper.get_subscription_callback_object_inter(subscription_cb)

        records = self._source.inter_comm_records(publisher_handles, callback_object)

        columns = [COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        self._format(records, columns)

        self._rename_column(records,
                            comm_value.subscription_callback_name,
                            comm_value.topic_name,
                            None)

        return records

    @staticmethod
    def _format(records: RecordsInterface, columns: List[str]):
        drop = list(set(records.column_names) - set(columns))
        records.drop_columns(drop)
        records.reindex(columns)

    @staticmethod
    def _rename_column(
        records: RecordsInterface,
        callback_name: Optional[str],
        topic_name: Optional[str],
        node_name: Optional[str],
    ) -> None:
        rename_dict = {}

        if COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'

        if COLUMN_NAME.TIMER_EVENT_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.TIMER_EVENT_TIMESTAMP] = \
                f'{callback_name}/{COLUMN_NAME.TIMER_EVENT_TIMESTAMP}'

        if COLUMN_NAME.CALLBACK_START_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.CALLBACK_START_TIMESTAMP] = \
                f'{callback_name}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'

        if COLUMN_NAME.CALLBACK_END_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.CALLBACK_END_TIMESTAMP] = \
                f'{callback_name}/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'

        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.RCL_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}'

        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.DDS_WRITE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}'

        if COLUMN_NAME.MESSAGE_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.MESSAGE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'

        if COLUMN_NAME.SOURCE_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.SOURCE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_MESSAGE_ID in records.column_names:
            rename_dict[COLUMN_NAME.TILDE_MESSAGE_ID] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_MESSAGE_ID}'

        if COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in records.column_names:
            rename_dict[COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP}'

        if 'lookup_transform_start_timestamp' in records.column_names:
            rename_dict['lookup_transform_start_timestamp'] = \
                f'{node_name}/lookup_transform_start_timestamp'

        if 'lookup_transform_end_timestamp' in records.column_names:
            rename_dict['lookup_transform_end_timestamp'] = \
                f'{node_name}/lookup_transform_end_timestamp'

        if 'tf_lookup_target_time' in records.column_names:
            rename_dict['tf_lookup_target_time'] = \
                f'{node_name}/tf_lookup_target_time'

        if 'set_transform_timestamp' in records.column_names:
            rename_dict['set_transform_timestamp'] = \
                f'{node_name}/set_transform_timestamp'

        records.rename_columns(rename_dict)

    @staticmethod
    def _rename_column_tilde(
        records: RecordsInterface,
        topic_name_sub: str,
        topic_name_pub: str
    ) -> None:
        rename_dict = {}

        if COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP] = \
                f'{topic_name_sub}/{COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_MESSAGE_ID in records.columns:
            rename_dict[COLUMN_NAME.TILDE_MESSAGE_ID] = \
                f'{topic_name_sub}/{COLUMN_NAME.TILDE_MESSAGE_ID}'

        if COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP] = \
                f'{topic_name_pub}/{COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP}'

        records.rename_columns(rename_dict)


class RecordsProviderLttngHelper:
    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        from .bridge import LttngBridge
        self._bridge = LttngBridge(lttng)

    def get_callback_objects(
        self,
        callback: CallbackStructValue
    ) -> Tuple[int, Optional[int]]:
        if isinstance(callback, TimerCallbackStructValue):
            return self.get_timer_callback_object(callback), None

        if isinstance(callback, SubscriptionCallbackStructValue):
            obj = self.get_subscription_callback_object_inter(callback)
            obj_intra = self.get_subscription_callback_object_intra(callback)
            if obj_intra is not None:
                return obj, obj_intra
            return obj, None

        msg = 'Failed to get callback object. '
        msg += f'{callback.callback_type.type_name} is not supported.'
        raise UnsupportedTypeError(msg)

    def get_timer_callback_object(
        self,
        callback: TimerCallbackStructValue
    ) -> int:
        callback_lttng = self._bridge.get_timer_callback(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_objects(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> Tuple[int, Optional[int]]:
        return self.get_callback_objects(callback)

    def get_tf_broadcaster_handler(
        self,
        broadcaster: Union[TransformBroadcasterStructValue,
                           TransformFrameBroadcasterStructValue]
    ) -> int:
        return self._bridge.get_tf_broadcaster(broadcaster).broadcaster_handler

    def get_tf_buffer_handler(
        self,
        buffer: Union[TransformBufferStructValue, TransformFrameBufferStructValue]
    ) -> int:
        buf = self._bridge.get_tf_buffer(buffer)
        return buf.buffer_handler

    def get_subscription_callback_object_inter(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> int:
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_object_intra(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> Optional[int]:
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.callback_object_intra

    def get_tilde_subscription(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> Optional[int]:
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.tilde_subscription

    def get_publisher_handles(
        self,
        publisher: PublisherStructValue
    ) -> List[int]:
        publisher_lttng = self._bridge.get_publishers(publisher)
        return [pub_info.publisher_handle
                for pub_info
                in publisher_lttng]

    def get_tilde_publishers(
        self,
        publisher_info: PublisherStructValue
    ) -> List[int]:
        publisher_lttng = self._bridge.get_publishers(publisher_info)
        publisher = [pub_info.tilde_publisher
                     for pub_info
                     in publisher_lttng
                     if pub_info.tilde_publisher is not None]
        return publisher

    def get_lttng_publishers(
        self,
        publisher: PublisherStructValue
    ) -> List[PublisherValueLttng]:
        return self._bridge.get_publishers(publisher)

    def get_lttng_subscription(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> SubscriptionCallbackValueLttng:
        return self._bridge.get_subscription_callback(callback)

    def get_lttng_timer(
        self,
        callback: TimerCallbackStructValue
    ) -> TimerCallbackValueLttng:
        return self._bridge.get_timer_callback(callback)


class NodeRecordsCallbackChain:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:

        self._provider = provider
        self._validate(node_path)
        self._val = node_path

    def to_records(self):
        chain_info = self._val.child

        if isinstance(chain_info[0], CallbackStructValue):
            cb_info = chain_info[0]
            records = self._provider.callback_records(cb_info)
        else:
            var_pass_info = chain_info[0]
            records = self._provider.variable_passing_records(var_pass_info)

        for chain_element in chain_info[1:]:
            if isinstance(chain_element, CallbackStructValue):
                records_ = self._provider.callback_records(chain_element)
                join_key = records_.column_names[0]
                records = merge(
                    left_records=records,
                    right_records=records_,
                    join_left_key=join_key,
                    join_right_key=join_key,
                    how='left',
                    progress_label='binding: callback_start and callback end'
                )
                continue

            if isinstance(chain_element, VariablePassingStructValue):
                records_ = self._provider.variable_passing_records(chain_element)
                # self._rename_var_pass_records(records_, chain_element)
                join_key = records_.column_names[0]
                records = merge(
                    left_records=records,
                    right_records=records_,
                    join_left_key=join_key,
                    join_right_key=join_key,
                    how='left',
                    progress_label='binding: callback_end and callback start'
                )
                continue

        last_element = chain_info[-1]
        if isinstance(last_element, CallbackStructValue) \
                and self._val.publisher is not None:
            last_callback_end_name = Util.filter_items(
                lambda x: COLUMN_NAME.CALLBACK_END_TIMESTAMP in x, records.column_names)[-1]
            records.drop_columns([last_callback_end_name])
            last_callback_start_name = Util.filter_items(
                lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, records.column_names)[-1]

            publish_records = self._provider.publish_records(self._val.publisher)
            publish_column = publish_records.columns[0]
            column_names = [str(c) for c in records.columns + [publish_column]]
            records = merge_sequencial(
                left_records=records,
                right_records=publish_records,
                join_left_key=None,
                join_right_key=None,
                left_stamp_key=last_callback_start_name,
                right_stamp_key=publish_column.column_name,
                how='left',
                progress_label='binding: callback_start and publish',
            )
            records.drop_columns(list(set(records.column_names) - set(column_names)))
            records.reindex(column_names)
        return records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('')

        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('callback values is None.')

        if not isinstance(node_path.message_context, CallbackChain):
            msg = 'node_path.message context is not CallbackChain'
            raise UnsupportedNodeRecordsError(msg)

        head_callback = node_path.callbacks[0]
        tail_callback = node_path.callbacks[-1]

        if node_path.publish_topic_name is not None and \
            tail_callback.publish_topic_names is not None and \
            len(tail_callback.publish_topic_names) != 0 and \
                node_path.publish_topic_name not in tail_callback.publish_topic_names:
            raise UnsupportedNodeRecordsError('')

        if node_path.subscribe_topic_name is not None and \
                node_path.subscribe_topic_name != head_callback.subscribe_topic_name:
            raise UnsupportedNodeRecordsError('')


class NodeRecordsInheritUniqueTimestamp:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, InheritUniqueStamp):
            msg = 'node_path.message context is not InheritUniqueStamp'
            raise UnsupportedNodeRecordsError(msg)

        self._provider = provider
        self._context: InheritUniqueStamp = node_path.message_context
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        sub_records = self._provider.subscribe_records(self._node_path.subscription)
        pub_records = self._provider.publish_records(self._node_path.publisher)

        columns = [
            sub_records.columns[0],
            pub_records.columns[0],
        ]

        join_left_key = f'{self._node_path.subscribe_topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'
        join_right_key = f'{self._node_path.publish_topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'
        pub_sub_records = merge_sequencial(
            left_records=sub_records,
            right_records=pub_records,
            left_stamp_key=sub_records.columns[0],
            right_stamp_key=pub_records.columns[0],
            join_left_key=join_left_key,
            join_right_key=join_right_key,
            columns=Columns(sub_records.columns + pub_records.columns).as_list(),
            how='left_use_latest',
            progress_label='binding: inherit unique timestamp',
        )

        drop_columns = list(set(pub_sub_records.columns) - set(columns))
        pub_sub_records.drop_columns(drop_columns)
        pub_sub_records.reindex(columns)
        return pub_sub_records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: InheritUniqueStamp,
    ) -> None:
        def is_valid() -> bool:
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False

            return True

        if is_valid():
            return None

        msg = f'InheritUniqueStamp cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class NodeRecordsUseLatestMessage:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, UseLatestMessage):
            raise UnsupportedNodeRecordsError('node_path.message context is not UseLatestMessage')

        self._provider = provider
        self._context: UseLatestMessage = node_path.message_context
        # self._validate(node_path, self._context)  # TODO: uncomment here
        self._node_path = node_path

    def to_records(self):

        if self._node_path.subscription is not None:
            node_in_records = self._provider.subscribe_records(self._node_path.subscription)
        elif self._node_path.tf_frame_buffer is not None:
            node_in_records = self._provider.tf_lookup_records(self._node_path.tf_frame_buffer)

        if self._node_path.publisher is not None:
            node_out_records = self._provider.publish_records(self._node_path.publisher)
        elif self._node_path.tf_frame_broadcaster is not None and \
                self._node_path.tf_frame_broadcaster.publisher is not None:
            node_out_records = self._provider.publish_records(
                self._node_path.tf_frame_broadcaster.publisher)
        else:
            raise UnsupportedNodeRecordsError('node_path.publisher is None')

        column_names = [
            node_in_records.columns[0].column_name,
            f'{self._node_path.publish_topic_name}/rclcpp_publish_timestamp',
        ]
        node_io_records = merge_sequencial(
            left_records=node_in_records,
            right_records=node_out_records,
            left_stamp_key=node_in_records.columns[0].column_name,
            right_stamp_key=node_out_records.columns[0].column_name,
            join_left_key=None,
            join_right_key=None,
            how='left_use_latest',
            progress_label='binding use_latest_message.'
        )

        drop_columns = list(set(node_io_records.column_names) - set(column_names))
        node_io_records.drop_columns(drop_columns)
        node_io_records.reindex(column_names)
        return node_io_records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: UseLatestMessage,
    ) -> None:
        def is_valid() -> bool:
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False

            return True

        if is_valid():
            return None

        msg = f'UseLatest cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class NodeRecordsTilde:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, Tilde):
            raise UnsupportedNodeRecordsError('node_path.message context is not UseLatestMessage')

        self._provider = provider
        self._context: MessageContext = node_path.message_context
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        tilde_records = self._provider.tilde_records(
            self._node_path.subscription, self._node_path.publisher)
        sub_records = self._provider.subscribe_records(self._node_path.subscription)
        pub_records = self._provider.publish_records(self._node_path.publisher)

        left_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, sub_records.columns)
        right_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in x, sub_records.columns)

        records = merge(
            left_records=sub_records,
            right_records=tilde_records,
            join_left_key=right_stamp_key,
            join_right_key=right_stamp_key,
            columns=Columns(sub_records.columns + tilde_records.columns).as_list(),
            how='left',
            progress_label='binding tilde subscribe records.'
        )

        left_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in x, records.columns)

        records = merge(
            left_records=records,
            right_records=pub_records,
            join_left_key=left_stamp_key,
            join_right_key=left_stamp_key,
            columns=Columns(records.columns + pub_records.columns).as_list(),
            how='left',
            progress_label='binding tilde publish records.'
        )

        columns = [
            Util.find_one(lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, records.columns),
            Util.find_one(lambda x: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in x, records.columns),
        ]

        drop_columns = list(set(records.columns) - set(columns))
        records.drop_columns(drop_columns)
        records.reindex(columns)
        return records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: MessageContext,
    ) -> None:
        def is_valid() -> bool:
            if not isinstance(context, Tilde):
                return False
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False

            return True

        if is_valid():
            return None

        msg = f'UseLatest cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class FilteredRecordsSource:

    def __init__(
        self,
        lttng: Lttng,
    ) -> None:
        self._lttng = lttng

    def tilde_subscribe_records(
        self,
        tilde_subscription: int
    ) -> RecordsInterface:
        """
        Compose filtered tilde subscribe records.

        Parameters
        ----------
        tilde_subscription : int

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_tilde_subscribe_records()
            records.filter_if(
                lambda x: x.get('tilde_subscription') == tilde_subscription
            )
            records.drop_columns(['tilde_subscription])

        """
        columns, grouped_records = self._grouped_tilde_sub_records()
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        sub_records = RecordsFactory.create_instance(None, columns)

        if tilde_subscription is not None and tilde_subscription in grouped_records:
            sub_records_ = grouped_records[tilde_subscription].clone()
            sub_records.concat(sub_records_)

        sub_records.drop_columns([COLUMN_NAME.TILDE_SUBSCRIPTION])
        return sub_records

    def sub_records(
        self,
        inter_callback_object: int,
        intra_callback_object: Optional[int]
    ) -> RecordsInterface:
        """
        Compose filtered subscribe records.

        Parameters
        ----------
        inter_callback_object : int
        intra_callback_object : Optional[int]

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_subscribe_records()
            records.filter_if(
                lambda x: x.get('callback_object') in [
                    inter_callback_object, intra_callback_object
                ]
            )

        """
        columns, grouped_records = self._grouped_sub_records()
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        sample_records = list(grouped_records.values())[0]
        columns = sample_records.columns
        sub_records = RecordsFactory.create_instance(None, columns)

        if inter_callback_object in grouped_records:
            sub_records.concat(grouped_records[inter_callback_object].clone())

        if intra_callback_object is not None and intra_callback_object in grouped_records:
            intra_sub_records = grouped_records[intra_callback_object].clone()
            sub_records.concat(intra_sub_records)
            sub_records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        return sub_records

    def inter_comm_records(
        self,
        publisher_handles: List[int],
        callback_object: int
    ) -> RecordsInterface:
        """
        Compose filtered inter communication records.

        Parameters
        ----------
        publisher_handles : List[int]
        callback_object : int

        Returns
        -------
        RecordsInterface

        """
        pub_records = self.publish_records(publisher_handles)
        sub_records = self.sub_records(callback_object, None)

        merged = merge(
            left_records=pub_records,
            right_records=sub_records,
            join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            how='left'
        )

        column_names = [
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in merged.columns:
            column_names.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in merged.columns:
            column_names.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        column_names += [
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP
        ]
        drop = list(set(merged.column_names) - set(column_names))
        merged.drop_columns(drop)
        merged.reindex(column_names)

        return merged

    def send_transform(
        self,
        broadcaster: int,
        transform: Optional[TransformValue]
    ) -> RecordsInterface:
        """
        Compose filtered send transform records.

        Parameters
        ----------
        broadcaster : TransformBufferStructValue
            Target broadcaster
        transform : Optional[TransformValue]
            target frames

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_send_transform()
            TODO: comment here.

        """
        columns, grouped_records = self._grouped_send_transform_records()
        records = RecordsFactory.create_instance(None, columns)
        if transform is None:
            for key in grouped_records:
                records.concat(grouped_records[key].clone())
            return records

        mapper = self._lttng.tf_frame_id_mapper
        frame_id = mapper.get_key(transform.frame_id)
        child_frame_id = mapper.get_key(transform.child_frame_id)
        key = (broadcaster, frame_id, child_frame_id)

        if key in grouped_records:
            records.concat(grouped_records[key].clone())

        return records

    def set_transform(
        self,
        buffer_core: int,
        transform: Optional[TransformValue]
    ) -> RecordsInterface:
        """
        Compose filtered lookup transform records.

        Parameters
        ----------
        broadcaster : TransformBufferStructValue
            Target broadcaster
        transform : Optional[TransformValue]
            target frames

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            TODO: comment here.

        """
        columns, grouped_records = self._grouped_tf_set_records()

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        if transform is None:
            records = RecordsFactory.create_instance(None, columns)
            for key in grouped_records:
                if key[0] == buffer_core:
                    records_ = grouped_records[key].clone()
                    records.concat(records_)
            return records

        mapper = self._lttng.tf_frame_id_mapper
        key = (
            buffer_core,
            mapper.get_key(transform.frame_id),
            mapper.get_key(transform.child_frame_id),
        )
        if key in grouped_records:
            return grouped_records[key].clone()

        records = RecordsFactory.create_instance(None, columns)
        return records

    def lookup_transform(
        self,
        buffer_core: int,
        transform: Optional[TransformValue]
    ) -> RecordsInterface:
        """
        Compose filtered lookup transform records.

        Parameters
        ----------
        broadcaster : TransformBufferStructValue
            Target broadcaster
        transform : Optional[TransformValue]
            target frames

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            TODO: comment here.

        """
        columns, grouped_records = self._grouped_lookup_records()

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        if transform is None:
            records = RecordsFactory.create_instance(None, columns)
            for key in grouped_records:
                if key[0] == buffer_core:
                    records.concat(grouped_records[key].clone())
            return records

        mapper = self._lttng.tf_frame_id_mapper
        frame_id = mapper.get_key(transform.frame_id)
        child_frame_id = mapper.get_key(transform.child_frame_id)

        key = (
            buffer_core,
            frame_id,
            child_frame_id
        )
        if key in grouped_records:
            return grouped_records[key].clone()

        return RecordsFactory.create_instance(None, columns)

    def find_closest(
        self,
        buffer_core: int
    ) -> RecordsInterface:
        """
        Compose filtered lookup transform records.

        Parameters
        ----------
        broadcaster : TransformBufferStructValue
            Target broadcaster
        transform : Optional[TransformValue]
            target frames

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            TODO: comment here.

        """
        columns, grouped_records = self._grouped_closest_records()

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        key = buffer_core
        return grouped_records[key]

    def intra_comm_records(
        self,
        publisher_handles: List[int],
        intra_callback_object: Optional[int]
    ) -> RecordsInterface:
        """
        Compose filtered intra communication records.

        Parameters
        ----------
        publisher_handles : List[int]
        intra_callback_object : Optional[int]

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_intra_proc_comm_records()
            records.filter_if(
                lambda x: x.get('callback_object') == callback_object and
                          x.get('publisher_handle') in publisher_handles
            )


        """
        columns, grouped_records = self._grouped_intra_comm_records()

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        sample_records = list(grouped_records.values())[0]
        columns = sample_records.columns
        records = RecordsFactory.create_instance(None, columns)

        if intra_callback_object is not None:
            for publisher_handle in publisher_handles:
                key = (intra_callback_object, publisher_handle)
                if key in grouped_records:
                    records_ = grouped_records[key].clone()
                    records.concat(records_)
        records.sort(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        return records

    def publish_records(
        self,
        publisher_handles: List[int],
    ) -> RecordsInterface:
        """
        Compose publish records.

        Parameters
        ----------
        publisher_handles : List[int]

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_publish_records()
            records.filter_if(
                lambda x: x.get('publisher_handle') in publisher_handles
                ]
            )

        Columns
        - rclcpp_publish_timestamp
        - rcl_publish_timestamp (Optional)
        - dds_write_timestamp (Optional)
        - message_timestamp
        - source_timestamp
        - tilde_publish_timestamp
        - tilde_message_id

        """
        columns, grouped_records = self._grouped_publish_records()

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        pub_records = RecordsFactory.create_instance(None, columns)

        for publisher_handle in publisher_handles:
            if publisher_handle in grouped_records:
                inter_pub_records = grouped_records[publisher_handle].clone()
                pub_records.concat(inter_pub_records)

        return pub_records

    def tilde_publish_records(
        self,
        tilde_publishers: Sequence[int]
    ) -> RecordsInterface:
        """
        Compose tilde publish records.

        Parameters
        ----------
        tilde_publishers : Sequence[int]

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_tilde_publish_records()
            records.filter_if(
                lambda x: x.get('tilde_publisher') in tilde_publishers
            )

        """
        columns, grouped_records = self._grouped_tilde_pub_records()
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(None, columns)

        tilde_records = RecordsFactory.create_instance(None, columns)

        for tilde_publisher in tilde_publishers:
            if tilde_publisher in grouped_records:
                tilde_records_ = grouped_records[tilde_publisher].clone()
                tilde_records.concat(tilde_records_)

        tilde_records.drop_columns([COLUMN_NAME.TILDE_PUBLISHER])
        return tilde_records

    def _expand_key_tuple(
        self,
        group: Dict[Tuple[int, ...], RecordsInterface]
    ) -> Dict[int, RecordsInterface]:
        group_: Dict[int, RecordsInterface] = {}
        for key in group.keys():
            assert len(key) == 1
            group_[key[0]] = group[key]
        return group_

    def callback_records(
        self,
        inter_callback_object: int,
        intra_callback_object: Optional[int]
    ) -> RecordsInterface:
        """
        Compose callback records.

        Parameters
        ----------
        inter_callback_object : int
        intra_callback_object : Optional[int]

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_callback_records()
            records.filter_if(
                lambda x: x.['callback_object] in [inter_callback_object, intra_callback_object]
            )

        """
        columns, records = self._grouped_callback_records()
        callback_records = RecordsFactory.create_instance(None, columns)

        if inter_callback_object in records:
            inter_callback_records = records[inter_callback_object].clone()
            callback_records.concat(inter_callback_records)

        if intra_callback_object is not None and intra_callback_object in records:
            intra_callback_records = records[intra_callback_object].clone()
            callback_records.concat(intra_callback_records)
            callback_records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        return callback_records

    @lru_cache
    def _grouped_lookup_records(
        self,
    ) -> Tuple[List[Column], Dict[Tuple[int, ...], RecordsInterface]]:
        records = self._lttng.compose_lookup_transform()
        columns = records.columns
        group = records.groupby(
            ['tf_buffer_core', 'frame_id', 'child_frame_id'])
        return columns, group

    @lru_cache
    def _grouped_send_transform_records(
        self,
    ) -> Tuple[List[Column], Dict[Tuple[int, ...], RecordsInterface]]:
        records = self._lttng.compose_send_transform()
        columns = records.columns
        group = records.groupby(['broadcaster', 'frame_id', 'child_frame_id'])
        return columns, group

    @lru_cache
    def _grouped_callback_records(
        self
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_callback_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.CALLBACK_OBJECT])
        return columns, self._expand_key_tuple(group)

    @lru_cache
    def _grouped_inter_comm_records(
        self
    ) -> Tuple[List[Column], Dict[Tuple[int, ...], RecordsInterface]]:
        records = self._lttng.compose_inter_proc_comm_records()
        columns = records.columns
        group = records.groupby(
            [COLUMN_NAME.CALLBACK_OBJECT, COLUMN_NAME.PUBLISHER_HANDLE])
        return columns, group

    @lru_cache
    def _grouped_intra_comm_records(
        self
    ) -> Tuple[List[Column], Dict[Tuple[int, ...], RecordsInterface]]:
        records = self._lttng.compose_intra_proc_comm_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.CALLBACK_OBJECT, COLUMN_NAME.PUBLISHER_HANDLE])
        return columns, group

    @lru_cache
    def _grouped_publish_records(
        self
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_publish_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.PUBLISHER_HANDLE])
        return columns, self._expand_key_tuple(group)

    @lru_cache
    def _grouped_sub_records(
        self
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_subscribe_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.CALLBACK_OBJECT])
        return columns, self._expand_key_tuple(group)

    @lru_cache
    def _grouped_tf_set_records(
        self,
    ) -> Tuple[List[Column], Dict[Tuple[int, ...], RecordsInterface]]:
        records = self._lttng.compose_set_transform()
        columns = records.columns
        group = records.groupby(['tf_buffer_core', 'frame_id', 'child_frame_id'])
        return columns, group

    @lru_cache
    def _grouped_closest_records(
        self,
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_find_closest()
        columns = records.columns
        group = records.groupby(['tf_buffer_core'])
        return columns, self._expand_key_tuple(group)

    @lru_cache
    def _grouped_tilde_pub_records(
        self
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_tilde_publish_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.TILDE_PUBLISHER])
        return columns, self._expand_key_tuple(group)

    @lru_cache
    def _grouped_tilde_sub_records(
        self
    ) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._lttng.compose_tilde_subscribe_records()
        columns = records.columns
        group = records.groupby([COLUMN_NAME.TILDE_SUBSCRIPTION])
        return columns, self._expand_key_tuple(group)
