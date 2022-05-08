
from .callback_records import CallbackRecordsContainer
from .publish_records import PublishRecordsContainer
from .transform import TransformSendRecordsContainer, TransformLookupContainer

from ..column_names import COLUMN_NAME
from ..bridge import LttngBridge
from ..value_objects import (
    PublisherValueLttng,
    TransformBufferValueLttng,

)

from ....record import RecordsInterface, merge_sequencial, ColumnAttribute
from ....value_objects import NodePathStructValue, MessageContextType


class NodeRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        cb_records: CallbackRecordsContainer,
        pub_records: PublishRecordsContainer,
        tf_lookup_records: TransformLookupContainer,
        tf_send_records: TransformSendRecordsContainer
    ) -> None:
        self._bridge = bridge
        self._cb_records = cb_records
        self._pub_records = pub_records
        self._tf_lookup_records = tf_lookup_records
        self._tf_send_records = tf_send_records

    def get_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:

        if node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN:
            return self._get_callback_chain(node_path_val)

        if node_path_val.message_context_type == MessageContextType.USE_LATEST_MESSAGE:
            return self._get_use_latest_message(node_path_val)

        if node_path_val.message_context_type == MessageContextType.TILDE:
            return self._get_tilde(node_path_val)

        raise NotImplementedError('')
        # raise UnsupportedNodeRecordsError(
        #     'Unknown message context. '
        #     f'message_context = {node_path_val.message_context.context_type.type_name}'
        # )

    def _get_use_latest_message(
        self,
        node_path_val: NodePathStructValue,
        # callback: SubscriptionCallbackValueLttng,
        # publisher: PublisherValueLttng,
    ) -> RecordsInterface:
        # node_output_lttng = None
        # node_input_lttng = None

        node_out_records: RecordsInterface
        node_in_recoreds: RecordsInterface

        if node_path_val.publisher is not None:
            node_out_records = self._pub_records.get_records(node_path_val.publisher)
            node_out_column = node_out_records.columns.get(
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
        elif node_path_val.tf_frame_broadcaster is not None:
            node_out_records = self._tf_send_records.get_records(
                node_path_val.tf_frame_broadcaster)
            node_out_column = node_out_records.columns.get(
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
        else:
            raise NotImplementedError('')

        # if node_path_val.subscription is not None:
        #     assert node_path_val.subscription.callback is not None
        #     node_input_lttng = self._bridge.get_subscription_callback(
        #         node_path_val.subscription.callback)
        # elif node_path_val.tf_frame_buffer is not None:
        #     node_input_lttng = self._bridge.get_tf_buffer(node_path_val.tf_frame_buffer)
        # else:
        #     raise NotImplementedError('')
        if node_path_val.subscription_callback is not None:
            node_in_records = self._cb_records.get_records(node_path_val.subscription_callback)
            node_in_column = node_in_records.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                base_name_match=True
            )
        elif node_path_val.tf_frame_buffer is not None:
            node_in_records = self._tf_lookup_records.get_records(node_path_val.tf_frame_buffer)
            node_in_column = node_in_records.columns.get(
                'lookup_transform_end_timestamp',
                base_name_match=True
            )
        else:
            raise NotImplementedError('')

        node_in_column.attrs.add(ColumnAttribute.NODE_IO)
        node_out_column.attrs.add(ColumnAttribute.NODE_IO)
        # node_out_column = node_out_records.columns.get_by_attrs([
        #     ColumnAttribute.NODE_IO,
        #     ColumnAttribute.SEND_MSG,
        #     ColumnAttribute.SYSTEM_TIME,
        # ], 'head')
        records = merge_sequencial(
            left_records=node_in_records,
            right_records=node_out_records,
            left_stamp_key=node_in_column.column_name,
            right_stamp_key=node_out_column.column_name,
            join_left_key=['pid'],
            join_right_key=['pid'],
            how='left'
        )

        # if self._node_path.subscription is not None:
        #     node_in_records = self._provider.subscribe_records(self._node_path.subscription)
        # elif self._node_path.tf_frame_buffer is not None:
        #     node_in_records = self._provider.tf_lookup_records(self._node_path.tf_frame_buffer)

        # if self._node_path.publisher is not None:
        #     node_out_records = self._provider.publish_records(self._node_path.publisher)
        # elif self._node_path.tf_frame_broadcaster is not None and \
        #         self._node_path.tf_frame_broadcaster.publisher is not None:
        #     node_out_records = self._provider.publish_records(
        #         self._node_path.tf_frame_broadcaster.publisher)
        # else:
        #     raise UnsupportedNodeRecordsError('node_path.publisher is None')

        # column_names = [
        #     node_in_records.columns[0].column_name,
        #     f'{self._node_path.publish_topic_name}/rclcpp_publish_timestamp',
        # ]
        # node_io_records = merge_sequencial(
        #     left_records=node_in_records,
        #     right_records=node_out_records,
        #     left_stamp_key=node_in_records.columns[0].column_name,
        #     right_stamp_key=node_out_records.columns[0].column_name,
        #     join_left_key=None,
        #     join_right_key=None,
        #     how='left_use_latest',
        #     progress_label='binding use_latest_message.'
        # )

        # drop_columns = list(set(node_io_records.column_names) - set(column_names))
        # node_io_records.drop_columns(drop_columns)
        # node_io_records.reindex(column_names)
        # return node_io_records

        return records

    def get_use_latest_message_records_tf_lookup_to_pub(
        self,
        tf_buffer: TransformBufferValueLttng,
        publisher: PublisherValueLttng,
    ) -> RecordsInterface:

        publish_records = self._pub_records.get_records(publisher)
        publish_column = publish_records.columns.get_by_attrs([
            ColumnAttribute.NODE_IO,
            ColumnAttribute.SEND_MSG,
            ColumnAttribute.SYSTEM_TIME,
        ], 'head')
        callback_records = self._cb_records.get_records(callback)

        callback_column = callback_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        )
        records = merge_sequencial(
            left_records=callback_records,
            right_records=publish_records,
            left_stamp_key=callback_column.column_name,
            right_stamp_key=publish_column.column_name,
            join_left_key=['pid'],
            join_right_key=['pid'],
            how='left'
        )

        # if self._node_path.subscription is not None:
        #     node_in_records = self._provider.subscribe_records(self._node_path.subscription)
        # elif self._node_path.tf_frame_buffer is not None:
        #     node_in_records = self._provider.tf_lookup_records(self._node_path.tf_frame_buffer)

        # if self._node_path.publisher is not None:
        #     node_out_records = self._provider.publish_records(self._node_path.publisher)
        # elif self._node_path.tf_frame_broadcaster is not None and \
        #         self._node_path.tf_frame_broadcaster.publisher is not None:
        #     node_out_records = self._provider.publish_records(
        #         self._node_path.tf_frame_broadcaster.publisher)
        # else:
        #     raise UnsupportedNodeRecordsError('node_path.publisher is None')

        # column_names = [
        #     node_in_records.columns[0].column_name,
        #     f'{self._node_path.publish_topic_name}/rclcpp_publish_timestamp',
        # ]
        # node_io_records = merge_sequencial(
        #     left_records=node_in_records,
        #     right_records=node_out_records,
        #     left_stamp_key=node_in_records.columns[0].column_name,
        #     right_stamp_key=node_out_records.columns[0].column_name,
        #     join_left_key=None,
        #     join_right_key=None,
        #     how='left_use_latest',
        #     progress_label='binding use_latest_message.'
        # )

        # drop_columns = list(set(node_io_records.column_names) - set(column_names))
        # node_io_records.drop_columns(drop_columns)
        # node_io_records.reindex(column_names)
        # return node_io_records

        return records

    def _get_tilde(self, node_path: NodePathStructValue) -> RecordsInterface:
        raise NotImplementedError('')

    def _get_callback_chain(
        self,
        node_path: NodePathStructValue,
        # callbacks: Sequence[Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]],
        # publisher: PublisherValueLttng,
    ) -> RecordsInterface:
        # assert node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN

        # assert node_path_val.callbacks is not None
        # callbacks_lttng = [self._bridge.get_callback(_) for _ in node_path_val.callbacks]

        # if node_path_val.publisher is not None:
        #     publishers_lttng = self._bridge.get_publishers(node_path_val.publisher)
        # elif node_path_val.tf_frame_broadcaster is not None:
        #     publishers_lttng = self._bridge.get_publishers(
        #         node_path_val.tf_frame_broadcaster.publisher)
        # else:
        #     raise NotImplementedError('')
        callbacks = node_path.callbacks

        records = self._cb_records.get_records(callbacks[0])
        records.columns.drop([COLUMN_NAME.CALLBACK_OBJECT], base_name_match=True)

        for callback in callbacks[1:]:
            records_ = self._cb_records.get_records(callback)
            records_.columns.drop([COLUMN_NAME.CALLBACK_OBJECT], base_name_match=True)

            end_column = records.columns.get(
                COLUMN_NAME.CALLBACK_END_TIMESTAMP, take='tail', base_name_match=True
            )
            start_column = records_.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, take='head', base_name_match=True
            )

            records = merge_sequencial(
                left_records=records,
                right_records=records_,
                left_stamp_key=end_column.column_name,
                right_stamp_key=start_column.column_name,
                join_left_key=[COLUMN_NAME.PID],
                join_right_key=[COLUMN_NAME.PID],
                how='left'
            )

        column_callback_start_time = records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, take='tail', base_name_match=True)

        publish_records = self._pub_records.get_records(node_path.publisher)
        publish_records.columns.drop([COLUMN_NAME.PUBLISHER_HANDLE], base_name_match=True)
        column_publish_time = publish_records.columns.get(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, take='head', base_name_match=True)

        records = merge_sequencial(
            left_records=records,
            right_records=publish_records,
            join_left_key=None,
            join_right_key=None,
            left_stamp_key=column_callback_start_time.column_name,
            right_stamp_key=column_publish_time.column_name,
            how='left',
            progress_label='binding: callback_start and publish',
        )

        records.columns.drop([COLUMN_NAME.PID, COLUMN_NAME.TID])

        return records
