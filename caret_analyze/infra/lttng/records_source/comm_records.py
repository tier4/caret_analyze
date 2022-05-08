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
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            COLUMN_NAME.BUFFER,
            COLUMN_NAME.MESSAGE,
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
        ], base_name_match=True)
        records.columns.reindex(columns, base_name_match=True)
        return records

    def get_inter_records(
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
    # @cached_property
    # def inter_proc_comm_records(self) -> RecordsInterface:
    #     """
    #     Compose inter process communication records.

    #     Used tracepoints
    #     - rclcpp_publish
    #     - dds_bind_addr_to_addr
    #     - dds_bind_addr_to_stamp
    #     - rcl_publish (Optional)
    #     - dds_write (Optional)
    #     - dispatch_subscription_callback
    #     - callback_start

    #     Returns
    #     -------
    #     RecordsInterface
    #         columns
    #         - callback_object
    #         - callback_start_timestamp
    #         - publisher_handle
    #         - rclcpp_publish_timestamp
    #         - rcl_publish_timestamp (Optional)
    #         - dds_write_timestamp (Optional)
    #         - message_timestamp
    #         - source_timestamp

    #     """
    #     publish = self._data.rclcpp_publish

    #     publish = merge_sequencial_for_addr_track(
    #         source_records=publish,
    #         copy_records=self._data.dds_bind_addr_to_addr,
    #         sink_records=self._data.dds_bind_addr_to_stamp,
    #         source_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         source_key=COLUMN_NAME.MESSAGE,
    #         copy_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
    #         copy_from_key=COLUMN_NAME.ADDR_FROM,
    #         copy_to_key=COLUMN_NAME.ADDR_TO,
    #         sink_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #         sink_from_key=COLUMN_NAME.ADDR,
    #         progress_label='binding: message_addr and rclcpp_publish',
    #     )

    #     rcl_publish_records = self._data.rcl_publish
    #     rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
    #     if len(rcl_publish_records) > 0:
    #         publish = merge_sequencial(
    #             left_records=publish,
    #             right_records=rcl_publish_records,
    #             left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #             right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
    #             join_left_key=COLUMN_NAME.MESSAGE,
    #             join_right_key=COLUMN_NAME.MESSAGE,
    #             how='left',
    #             progress_label='binding: rclcpp_publish and rcl_publish',
    #         )

    #     dds_write = self._data.dds_write
    #     if len(dds_write) > 0:
    #         publish = merge_sequencial(
    #             left_records=publish,
    #             right_records=dds_write,
    #             left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #             right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
    #             join_left_key=COLUMN_NAME.MESSAGE,
    #             join_right_key=COLUMN_NAME.MESSAGE,
    #             how='left',
    #             progress_label='binding: rcl_publish and dds_write',
    #         )

    #     # When both intra_publish and inter_publish are used, value mismatch occurs when merging.
    #     # In order to merge at the latency time,
    #     # align the time to intra_publish if intra_process communication is used.
    #     intra_publish = self._data.rclcpp_intra_publish.clone()
    #     intra_publish.drop_columns([
    #         COLUMN_NAME.MESSAGE, COLUMN_NAME.MESSAGE_TIMESTAMP
    #     ])
    #     publish = merge_sequencial(
    #         left_records=intra_publish,
    #         right_records=publish,
    #         left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         join_left_key=COLUMN_NAME.PUBLISHER_HANDLE,
    #         join_right_key=COLUMN_NAME.PUBLISHER_HANDLE,
    #         how='right'
    #     )
    #     rclcpp_publish: List[int] = []
    #     for record in publish.data:
    #         if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.data:
    #             rclcpp_publish.append(record.data[COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP])
    #         else:
    #             rclcpp_publish.append(
    #                 record.data[COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP])

    #     publish.append_column(
    #         Column(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, [ColumnAttribute.SYSTEM_TIME]),
    #         rclcpp_publish)
    #     publish.drop_columns([
    #         COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP
    #     ])

    #     callback_start= self.inter_callback_records
    #     subscription = self._data.dispatch_subscription_callback

    #     subscription = merge_sequencial(
    #         left_records=subscription,
    #         right_records=callback_start,
    #         left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
    #         join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
    #         join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
    #         how='left',
    #         progress_label='binding: dispatch_subscription_callback and callback_start',
    #     )

    #     # communication = merge(
    #     #     publish,
    #     #     self._data.on_data_available,
    #     #     join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #     #     join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #     #     columns=publish.columns,
    #     #     how='left',
    #     #     progress_label='binding: source_timestamp and on_data_available',
    #     # )

    #     communication = merge(
    #         publish,
    #         subscription,
    #         join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #         join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #         how='left',
    #         progress_label='binding: source_timestamp and callback_start',
    #     )

    #     communication.drop_columns(
    #         [
    #             COLUMN_NAME.IS_INTRA_PROCESS,
    #             COLUMN_NAME.ADDR,
    #             COLUMN_NAME.MESSAGE,
    #             COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #             COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
    #         ],
    #     )

    #     return communication