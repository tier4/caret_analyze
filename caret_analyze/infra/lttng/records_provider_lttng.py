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

from logging import getLogger
from typing import Optional, Union
from caret_analyze.record.interface import RecordInterface
from caret_analyze.value_objects.subscription import IntraProcessBufferStructValue

from .lttng import Lttng
from .bridge import LttngBridge
from ...common import ClockConverter, Util
from ...exceptions import (InvalidArgumentError,
                           UnsupportedNodeRecordsError,
                           )
from ...infra.interface import RuntimeDataProvider
from ...infra.lttng.column_names import COLUMN_NAME
from ...record.column import get_column
from ...record import (
    merge,
    merge_sequencial,
    RecordsFactory,
    RecordsInterface)
from ...value_objects import (
    CallbackStructValue,
    MessageContext,
    MessageContextType,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    CommunicationStructValue,
    NodePathStructValue,
    PublisherStructValue,
    Qos,
    SubscriptionStructValue,
    Tilde,
    TimerStructValue,
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
        self._bridge = LttngBridge(lttng)
        # self._source = (lttng)
        # self._helper = RecordsProviderLttngHelper(lttng)

    def is_intra_process_communication(
        self,
        publisher: PublisherStructValue,
        subscription: SubscriptionStructValue,
    ) -> Optional[bool]:
        publisher_lttng = self._bridge.get_publishers(publisher)[0]
        subscription_lttng = self._bridge.get_subscription_callback(subscription.callback)
        return self._lttng.is_intra_process_communication(publisher_lttng, subscription_lttng)

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

        publishers_lttng = self._bridge.get_publishers(comm_val.publisher)
        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]
        assert comm_val.subscription.callback is not None

        callback_lttng = self._bridge.get_subscription_callback(
            comm_val.subscription.callback)

        if self.is_intra_process_communication(comm_val):
            assert comm_val.subscription.intra_process_buffer is not None
            buffer_lttng = self._bridge.get_ipc_buffer(
                comm_val.subscription.intra_process_buffer)

            return self._lttng.get_intra_proc_comm_records(
                publisher_lttng, buffer_lttng, callback_lttng)

        records = self._lttng.get_inter_proc_comm_records(publisher_lttng, callback_lttng)
        return records

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

        assert node_path_val.publisher is not None
        publishers_lttng = self._bridge.get_publishers(node_path_val.publisher)
        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]

        if node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN:
            assert node_path_val.callbacks is not None
            callbacks_lttng = [self._bridge.get_callback(_) for _ in node_path_val.callbacks]
            return self._lttng.get_node_callback_chain(callbacks_lttng, publisher_lttng)

        # if node_path_val.message_context_type == MessageContextType.INHERIT_UNIQUE_STAMP:
            # return NodeRecordsInheritUniqueTimestamp(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.USE_LATEST_MESSAGE:
            assert node_path_val.subscription is not None
            assert node_path_val.subscription.callback is not None

            subscription_lttng = self._bridge.get_subscription_callback(
                node_path_val.subscription.callback)
            return self._lttng.get_node_use_latest_message(subscription_lttng, publisher_lttng)
            # return NodeRecordsUseLatestMessage(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.TILDE:
            return self._lttng.get_node_tilde(node_path_val)

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
        callback_lttng = self._bridge.get_callback(callback)
        callback_records = self._lttng.get_callback_records(callback_lttng)

        # columns = [
        #     COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        #     COLUMN_NAME.CALLBACK_END_TIMESTAMP
        # ]
        # self._format(callback_records, columns)
        self._rename_column(callback_records, callback.callback_name, None, callback.node_name)

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

        subscription_callback = self._bridge.get_subscription_callback(callback)
        tilde_subscription = subscription_callback.tilde_subscription

        if tilde_subscription is None:
            records = self._subscribe_records(subscription)
            self._rename_column(
                records,
                callback.callback_name,
                subscription.topic_name,
                subscription.node_name
            )
            return records

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

        callback_lttng = self._bridge.get_subscription_callback(callback)
        sub_records = self._lttng.get_subscribe_records(callback_lttng)

        return sub_records

    def intra_subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        callback = subscription.callback
        assert callback is not None

        callback = subscription.callback
        if callback is None:
            raise InvalidArgumentError(
                'callback_value is None. '
                f'node_name: {subscription.node_name}'
                f'callback_name: {subscription.callback_name}'
                f'topic_name: {subscription.topic_name}'
            )

        callback_lttng = self._bridge.get_subscription_callback(callback)
        sub_records = self._lttng.get_intra_subscribe_records(callback_lttng)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            subscription.node_name
        )
        return sub_records

    def ipc_buffer_records(
        self,
        ipc_buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        buffer_lttng = self._bridge.get_ipc_buffer(ipc_buffer)
        records = self._lttng.ipc_buffer_records(buffer_lttng)
        return records

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
        broadcaster_lttng = self._bridge.get_tf_broadcaster(broadcaster)
        records = self._lttng.get_send_transform(broadcaster_lttng, broadcaster.transform)

        return records

    def tf_communication_records(
        self,
        communication: TransformCommunicationStructValue
    ) -> RecordsInterface:
        broadcaster_lttng = self._bridge.get_tf_broadcaster(communication.broadcaster)
        buffer_lttng = self._bridge.get_tf_buffer(communication.buffer)
        is_intra_proc = self.is_intra_process_communication(
            communication.broadcaster.publisher,
            communication.buffer.listener_subscription
        )
        if is_intra_proc:
            return self._lttng.get_intra_proc_tf_comm_records
        return self._lttng.get_inter_proc_tf_comm_records(
            broadcaster_lttng,
            communication.listen_transform,
            buffer_lttng,
            communication.lookup_transform
        )

    def tf_lookup_records(
        self,
        frame_buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        tf_buffer = self._bridge.get_tf_buffer(frame_buffer)
        records = self._lttng.get_lookup_transform(tf_buffer, frame_buffer.lookup_transform)
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

        callback_lttng = self._bridge.get_subscription_callback(callback)
        sub_records = self._lttng.get_subscribe_records(callback_lttng)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            subscription.node_name,
        )

        return sub_records

    def intra_publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        publishers_lttng = self._bridge.get_publishers(publisher)
        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]
        records = self._lttng.get_intra_publish_records(publisher_lttng)
        self._rename_column(records, None, publisher.topic_name, publisher.node_name)
        return records

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
        publishers_lttng = self._bridge.get_publishers(publisher)

        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]
        pub_records = self._lttng.get_publish_records(publisher_lttng)

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
        callback_lttng = self._bridge.get_timer_callback(timer.callback)
        timer_records = self._lttng.get_timer_callback(callback_lttng)
        self._rename_column(timer_records, timer.callback_name, None, timer.node_name)

        return timer_records

    # def tilde_records(
    #     self,
    #     subscription: SubscriptionStructValue,
    #     publisher: PublisherStructValue
    # ) -> RecordsInterface:
    #     assert subscription.callback is not None

    #     publishers = self._bridge.get_publishers(publisher)
    #     subscription_lttng = self._bridge.get_subscription_callback(subscription.callback)

    #     assert len(publishers) > 0
    #     publisher_lttng = publishers[0]

    #     pub_records = self._lttng.get_tilde_publish_records(publisher_lttng)
    #     sub_records = self._lttng.get_tilde_subscribe_records(subscription)

    #     # records = merge(
    #     #     left_records=sub_records,
    #     #     right_records=pub_records,
    #     #     join_left_key=COLUMN_NAME.TILDE_MESSAGE_ID,
    #     #     join_right_key=COLUMN_NAME.TILDE_MESSAGE_ID,
    #     #     how='left',
    #     #     progress_label='binding: tilde pub and sub records'
    #     # )

    #     # columns = [
    #     #     COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
    #     #     COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP
    #     # ]
    #     # self._format(records, columns)

    #     self._rename_column_tilde(records, subscription.topic_name, publisher.topic_name)

    #     return records

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
            sub_cb_lttng = self._bridge.get_subscription_callback(sub_cb)
            return self._lttng.get_subscription_qos(sub_cb_lttng)

        pubs_lttng = self._bridge.get_publishers(pub_sub)
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
        # self._format(merged_records, columns)
        return merged_records

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
        is_intra_proc = self._lttng.is_intra_process_communication(communication)
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

        records.columns.rename(rename_dict)

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


class NodeRecordsTilde:
    def __init__(
        self,
        lttng: Lttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, Tilde):
            raise UnsupportedNodeRecordsError('node_path.message context is not UseLatestMessage')

        self._lttng = lttng
        self._context: MessageContext = node_path.message_context
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        records = self._lttng.get_node_tilde_records(
            self._node_path.subscription, self._node_path.publisher)
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

