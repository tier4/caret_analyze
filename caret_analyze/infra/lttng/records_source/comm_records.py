from functools import lru_cache

from .ipc_buffer_records import IpcBufferRecordsContainer
from .publish_records import PublishRecordsContainer
from .subscribe_records import SubscribeRecordsContainer
from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ....record import (
    ColumnAttribute,
    GroupedRecords,
    merge,
    merge_sequencial,
    merge_sequencial_for_addr_track,
    RecordsInterface,
)
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
        self._intra_comm_records = self._intra_records_deprecated(data)

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
        ]

        publisher_lttng = self._bridge.get_publishers(comm.publisher)[0]
        cb_lttng = self._bridge.get_subscription_callback(comm.subscription_callback)

        if cb_lttng.callback_object_intra is not None and \
            self._intra_comm_records.has(publisher_lttng.publisher_handle,
                                         cb_lttng.callback_object_intra):
            records = self._intra_comm_records.get(publisher_lttng.publisher_handle,
                                                   cb_lttng.callback_object_intra)
            columns = [
                        COLUMN_NAME.PID,
                        COLUMN_NAME.TID,
                        COLUMN_NAME.PUBLISHER_HANDLE,
                        COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
                        COLUMN_NAME.MESSAGE_TIMESTAMP,
                        COLUMN_NAME.CALLBACK_OBJECT,
                        COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                    ]
            records.columns.get(
                        COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True
            ).add_prefix(comm.topic_name)
            records.columns.get(
                        COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True
            ).add_prefix(comm.subscription_callback_name)
            records.columns.reindex(columns, base_name_match=True)
            return records

        publish_records = self._pub_records.get_intra_records(comm.publisher)
        callback_records = self._sub_records.get_intra_records(
            comm.subscription_callback)
        buffer_records = self._buffer_records.get_records(
            comm.subscription.intra_process_buffer)

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
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
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
        callback_records = self._sub_records.get_inter_records(
            comm.subscription_callback)

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

        cb_column = records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True)
        cb_column.attrs.add(ColumnAttribute.NODE_IO)

        return records

    @staticmethod
    def _intra_records_deprecated(
        data: Ros2DataModel
    ) -> GroupedRecords:
        sink_records = data.dispatch_intra_process_subscription_callback
        intra_publish_records = merge_sequencial_for_addr_track(
            source_records=data.rclcpp_intra_publish,
            copy_records=data.message_construct,
            sink_records=sink_records,
            source_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.MESSAGE_CONSTRUCT_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ORIGINAL_MESSAGE,
            copy_to_key=COLUMN_NAME.CONSTRUCTED_MESSAGE,
            sink_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            sink_from_key=COLUMN_NAME.MESSAGE,
            progress_label='bindig: publish_timestamp and message_addr',
        )

        # note: Incorrect latency is calculated when intra_publish of ros-rclcpp is included.
        intra_publish_records = merge(
            left_records=intra_publish_records,
            right_records=sink_records,
            join_left_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_right_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            how='inner'
        )

        intra_publish_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=sink_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE,
            join_right_key=COLUMN_NAME.MESSAGE,
            how='left_use_latest'
        )

        cb_records = GroupedRecords(
            data.callback_start,
            [COLUMN_NAME.IS_INTRA_PROCESS]
        )
        callback_start = cb_records.get(1)

        intra_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=callback_start,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            how='left',
            progress_label='bindig: dispath_subsription and callback_start',
        )

        intra_records.columns.drop(
            [
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )
        intra_records.columns.rename(
            {
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP
            }
        )

        attr_columns = intra_records.columns.gets(
            [
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            ], base_name_match=True
        )
        for attr_column in attr_columns:
            attr_column.attrs.add(ColumnAttribute.NODE_IO)
            attr_column.attrs.add(ColumnAttribute.SYSTEM_TIME)

        records = GroupedRecords(
            intra_records,
            [
                COLUMN_NAME.PUBLISHER_HANDLE, COLUMN_NAME.CALLBACK_OBJECT,
            ]
        )
        return records
