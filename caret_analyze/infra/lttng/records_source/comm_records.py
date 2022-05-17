from functools import lru_cache
from attr import attr
from caret_analyze.record.column import ColumnAttribute
from .subscribe_records import SubscribeRecordsContainer
from .ipc_buffer_records import IpcBufferRecordsContainer
from .publish_records import PublishRecordsContainer

from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..bridge import LttngBridge
from ....record import RecordsInterface, merge_sequencial
from ....value_objects import CommunicationStructValue


class CommRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        sub_records: SubscribeRecordsContainer,
        buffer_records: IpcBufferRecordsContainer,
        pub_records: PublishRecordsContainer,
    ) -> None:
        self._bridge = bridge
        self._sub_records = sub_records
        self._buffer_records = buffer_records
        self._pub_records = pub_records

    def get_intra_records(
        self,
        comm: CommunicationStructValue
    ) -> RecordsInterface:
        return self._get_intra_records(comm).clone()

    @lru_cache()
    def _get_intra_records(
        self,
        comm: CommunicationStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            'queued_msg_size',
            'is_full',
            COLUMN_NAME.BUFFER_DEQUEUE_TIMESTAMP,
            'dequeued_msg_size',
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            ]
        publish_records = self._pub_records.get_intra_records(comm.publisher)
        callback_records = self._sub_records.get_intra_records(comm.subscription_callback)
        buffer_records = self._buffer_records.get_records(comm.subscription.intra_process_buffer)

        pub_column = publish_records.columns.get(
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP, base_name_match=True)
        pub_column.rename(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        enqueue_column = buffer_records.columns.get(
            COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            base_name_match=True
        )

        records = merge_sequencial(
            left_records=publish_records,
            right_records=buffer_records,
            left_stamp_key=pub_column.column_name,
            right_stamp_key=enqueue_column.column_name,
            join_left_key=['pid', 'tid'],
            join_right_key=['pid', 'enqueue_tid'],
            how='left'
        )

        dequeue_column = records.columns.get(
            COLUMN_NAME.BUFFER_DEQUEUE_TIMESTAMP,
            base_name_match=True
        )
        callback_start_column = callback_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
        )

        records = merge_sequencial(
            left_records=records,
            right_records=callback_records,
            left_stamp_key=dequeue_column.column_name,
            right_stamp_key=callback_start_column.column_name,
            join_left_key=['pid', 'dequeue_tid'],
            join_right_key=['pid', 'tid'],
            how='left'
        )

        records.columns.drop([
            'dequeue_tid',
            'enqueue_tid',
            'index',
            COLUMN_NAME.BUFFER,
        ], base_name_match=True)
        records.columns.reindex(columns, base_name_match=True)
        attr_columns = records.columns.gets(
            [
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            ], base_name_match=True
        )
        for column in attr_columns:
            column.attrs.add(ColumnAttribute.NODE_IO)

        return records

    def get_inter_records(
        self,
        comm: CommunicationStructValue
    ) -> RecordsInterface:
        return self._get_inter_records(comm).clone()

    @lru_cache
    def _get_inter_records(
        self,
        comm: CommunicationStructValue
    ) -> RecordsInterface:
        columns = [
            'rclcpp_publish_timestamp',
            'rcl_publish_timestamp',
            'dds_write_timestamp',
            'message_timestamp',
            'callback_start_timestamp',
            'callback_end_timestamp']
        publish_records = self._pub_records.get_inter_records(comm.publisher)
        callback_records = self._sub_records.get_inter_records(comm.subscription_callback)

        publish_column = publish_records.columns.get(
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            base_name_match=True
        )
        publish_column.rename(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        left_source_stamp_column = publish_records.columns.get(
            COLUMN_NAME.SOURCE_TIMESTAMP,
            base_name_match=True
        )
        right_source_stamp_column = callback_records.columns.get(
            COLUMN_NAME.SOURCE_TIMESTAMP,
            base_name_match=True
        )

        records = merge_sequencial(
            left_records=publish_records,
            right_records=callback_records,
            left_stamp_key=left_source_stamp_column.column_name,
            right_stamp_key=right_source_stamp_column.column_name,
            join_left_key=None,
            join_right_key=None,
            how='left'
        )

        records.columns.drop([
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_OBJECT,
            ], base_name_match=True)

        records.columns.reindex(columns, base_name_match=True)

        pub_column = records.columns.get(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
        pub_column.attrs.add(ColumnAttribute.NODE_IO)

        cb_column = records.columns.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True)
        cb_column.attrs.add(ColumnAttribute.NODE_IO)

        return records
