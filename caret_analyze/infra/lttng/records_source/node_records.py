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

from .callback_records import CallbackRecordsContainer
from .publish_records import PublishRecordsContainer
from .subscribe_records import SubscribeRecordsContainer
from .transform import (
    TransformLookupContainer,
    TransformSendRecordsContainer,
)
from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ....record import (
    ColumnAttribute,
    merge,
    merge_sequencial,
    RecordsInterface,
)
from ....value_objects import (
    MessageContextType,
    NodePathStructValue,
    PublisherStructValue,
    TransformFrameBufferStructValue,
)


class NodeRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        cb_records: CallbackRecordsContainer,
        sub_records: SubscribeRecordsContainer,
        pub_records: PublishRecordsContainer,
        tf_lookup_records: TransformLookupContainer,
        tf_send_records: TransformSendRecordsContainer
    ) -> None:
        self._bridge = bridge
        self._cb_records = cb_records
        self._pub_records = pub_records
        self._tf_lookup_records = tf_lookup_records
        self._tf_send_records = tf_send_records
        self._sub_records = sub_records

    def get_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:

        if node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN:
            return self._get_callback_chain(node_path_val).clone()

        if node_path_val.message_context_type == MessageContextType.USE_LATEST_MESSAGE:
            return self._get_use_latest_message(node_path_val).clone()

        if node_path_val.message_context_type == MessageContextType.TILDE:
            return self._get_tilde(node_path_val).clone()

        raise NotImplementedError('')
        # raise UnsupportedNodeRecordsError(
        #     'Unknown message context. '
        #     f'message_context = {node_path_val.message_context.context_type.type_name}'
        # )

    @lru_cache
    def _get_use_latest_message(
        self,
        node_path_val: NodePathStructValue,
        # callback: SubscriptionCallbackValueLttng,
        # publisher: PublisherValueLttng,
    ) -> RecordsInterface:
        # node_output_lttng = None
        # node_input_lttng = None

        node_out_records: RecordsInterface
        node_in_records: RecordsInterface

        if node_path_val.publisher is not None:
            node_out_records = self._pub_records.get_records(node_path_val.publisher)
            node_out_column = node_out_records.columns.get(
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
            node_out_records.columns.drop(
                [
                    COLUMN_NAME.PID,
                    COLUMN_NAME.TID,
                    COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                    COLUMN_NAME.MESSAGE_TIMESTAMP,
                    COLUMN_NAME.SOURCE_TIMESTAMP,
                ], base_name_match=True
            )
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
            node_in_records.columns.drop(
                [
                    COLUMN_NAME.PID,
                    COLUMN_NAME.TID,
                    COLUMN_NAME.CALLBACK_END_TIMESTAMP
                ], base_name_match=True
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

        records = merge_sequencial(
            left_records=node_in_records,
            right_records=node_out_records,
            left_stamp_key=node_in_column.column_name,
            right_stamp_key=node_out_column.column_name,
            join_left_key=None,
            join_right_key=None,
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
        tf_buffer: TransformFrameBufferStructValue,
        publisher: PublisherStructValue,
    ) -> RecordsInterface:
        raise NotImplementedError('')

        publish_records = self._pub_records.get_records(publisher)
        publish_column = publish_records.columns.get_by_attrs([
            ColumnAttribute.NODE_IO,
            ColumnAttribute.SYSTEM_TIME,
        ], 'head')
        callback_records = self._cb_records.get_records()

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

    @lru_cache
    def _get_tilde(self, node_path: NodePathStructValue) -> RecordsInterface:
        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP
        ]
        assert node_path.subscription is not None
        assert node_path.publisher is not None
        sub_records = self._sub_records.get_records(node_path.subscription)
        pub_records = self._pub_records.get_records(node_path.publisher)
        sub_tilde_message_id_column = sub_records.columns.get(
            COLUMN_NAME.TILDE_MESSAGE_ID,
            base_name_match=True
        )
        pub_tilde_message_id_column = pub_records.columns.get(
            COLUMN_NAME.TILDE_MESSAGE_ID,
            base_name_match=True
        )
        records = merge(
            left_records=sub_records,
            right_records=pub_records,
            join_left_key=sub_tilde_message_id_column.column_name,
            join_right_key=pub_tilde_message_id_column.column_name,
            how='left'
        )
        records.columns.drop([
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.TILDE_MESSAGE_ID,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.TILDE_SUBSCRIPTION,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            COLUMN_NAME.DDS_WRITE_TIMESTAMP,
        ], base_name_match=True)
        records.columns.reindex(columns, base_name_match=True)
        return records

    @lru_cache
    def _get_callback_chain(
        self,
        node_path: NodePathStructValue,
    ) -> RecordsInterface:
        callbacks = node_path.callbacks

        assert callbacks is not None
        records = self._cb_records.get_records(callbacks[0])

        for callback in callbacks[1:]:
            records_ = self._cb_records.get_records(callback)

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

        assert node_path.publisher is not None
        publish_records = self._pub_records.get_records(node_path.publisher)
        publish_records.columns.drop(
            [
                COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
                COLUMN_NAME.SOURCE_TIMESTAMP
            ], base_name_match=True)
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

        callback_end_column = records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP, take='tail', base_name_match=True)
        records.columns.drop([
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            callback_end_column.column_name])

        return records
