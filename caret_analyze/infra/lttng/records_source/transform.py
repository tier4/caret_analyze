
from typing import Optional
from functools import lru_cache

from caret_analyze.record.column import ColumnAttribute

from .publish_records import PublishRecordsContainer
from .callback_records import CallbackRecordsContainer
from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..bridge import LttngBridge
from ..lttng_info import LttngInfo
from ....value_objects import (
    TransformFrameBroadcasterStructValue, TransformFrameBufferStructValue,
    TransformCommunicationStructValue,
)
from ....record import (
    RecordsInterface, merge_sequencial, GroupedRecords, merge
)


class TransformSendRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        info: LttngInfo,
        pub_records: PublishRecordsContainer,
    ) -> None:
        self._bridge = bridge
        self._send_transform = GroupedRecords(
            data.send_transform,
            [
                'broadcaster',
                'frame_id_compact',
                'child_frame_id_compact',
            ]
        )
        self._pub_records = pub_records
        self._info = info

    @lru_cache
    def get_records(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:

        broadcaster_lttng = self._bridge.get_tf_broadcaster(broadcaster)
        transform = broadcaster.transform

        columns = [
            'pid',
            'tid',
            'tf_timestamp', 'rclcpp_publish_timestamp', 'rcl_publish_timestamp',
            'dds_write_timestamp'
        ]
        to_frame_id = self._info.get_tf_broadcaster_frame_compact_map(
            broadcaster_lttng.broadcaster_handler)
        to_compact_frame_id = {v: k for k, v in to_frame_id.items()}
        frame_id_compact = to_compact_frame_id[transform.frame_id]
        child_frame_id_compact = to_compact_frame_id[transform.child_frame_id]

        records = self._send_transform.get(
            broadcaster_lttng.broadcaster_handler,
            frame_id_compact,
            child_frame_id_compact)
        pub_records = self._pub_records.get_records(broadcaster.publisher)

        drop_columns = pub_records.columns.gets([
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP
        ], base_name_match=True
        )
        pub_records.columns.drop([str(c) for c in drop_columns])

        send_tf_column = records.columns.get(
            'send_transform_timestamp'
        )
        publish_column = pub_records.columns.get(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            base_name_match=True
        )
        records = merge_sequencial(
            left_records=records,
            right_records=pub_records,
            left_stamp_key=send_tf_column.column_name,
            right_stamp_key=publish_column.column_name,
            join_left_key=None,
            join_right_key=None,
            how='inner'
        )

        # frame_ids: List[int] = []
        # child_frame_ids: List[int] = []
        # for record in records:
        #     to_frame_id = self._info.get_tf_broadcaster_frame_compact_map(
        #         record.get('broadcaster'))
        #     frame_id = to_frame_id[record.get('frame_id_compact')]
        #     frame_ids.append(frame_mapper.get_key(frame_id))
        #     child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
        #     child_frame_ids.append(frame_mapper.get_key(child_frame_id))

        # records.append_column(
        #     ColumnValue('frame_id', mapper=frame_mapper),
        #     frame_ids
        # )
        # records.append_column(
        #     ColumnValue('child_frame_id', mapper=frame_mapper),
        #     child_frame_ids
        # )

        records.columns.drop(
            [
                'frame_id_compact', 'child_frame_id_compact', 'broadcaster',
                'send_transform_timestamp', COLUMN_NAME.PUBLISHER_HANDLE
            ],
            base_name_match=True
        )

        records.columns.reindex(columns, base_name_match=True)

        return records
        # columns = [
        #     COLUMN_NAME.PID,
        #     COLUMN_NAME.TID,
        #     COLUMN_NAME.PUBLISHER_HANDLE,
        #     COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
        #     COLUMN_NAME.MESSAGE_TIMESTAMP,
        # ]

        # records = self._intra_rclcpp_publish.get(publisher.publisher_handle)
        # records.columns.drop(
        #     [
        #         COLUMN_NAME.MESSAGE
        #     ]
        # )
        # records.columns.reindex(columns)
        # ordered_columns = records.columns.gets_by_base_name(*columns[3:])
        # for column in ordered_columns:
        #     column.add_prefix(publisher.topic_name)
        # return records

    # @lru_cache
    # def get_inter_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
    #     columns = [
    #         COLUMN_NAME.PID,
    #         COLUMN_NAME.TID,
    #         COLUMN_NAME.PUBLISHER_HANDLE,
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.DDS_WRITE_TIMESTAMP,
    #         COLUMN_NAME.SOURCE_TIMESTAMP,
    #         COLUMN_NAME.MESSAGE_TIMESTAMP,
    #     ]

    #     records = self._get_inter_records(publisher)

    #     records.columns.drop([
    #         COLUMN_NAME.MESSAGE,
    #         COLUMN_NAME.ADDR,
    #         COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
    #         COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #     ])

    #     records.columns.reindex(columns)
    #     ordered_columns = records.columns.gets_by_base_name(*columns[3:])
    #     for column in ordered_columns:
    #         column.add_prefix(publisher.topic_name)

    #     return records

    # def _get_inter_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
    #     records = self._inter_rclcpp_publish.get(publisher.publisher_handle)

    #     join_keys = [
    #         # COLUMN_NAME.PID,
    #         COLUMN_NAME.TID,
    #         # COLUMN_NAME.PUBLISHER_HANDLE
    #     ]

    #     rcl_publish_records = self._rcl_publish.get(publisher.publisher_handle)
    #     records = merge_sequencial(
    #         left_records=records,
    #         right_records=rcl_publish_records,
    #         left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
    #         join_left_key=join_keys,
    #         join_right_key=join_keys,
    #         how='left',
    #         progress_label='binding: rclcpp_publish and rcl_publish',
    #     )

    #     # TODO(hsgwa) split by publisher_handle
    #     dds_write_records = self._dds_write
    #     records = merge_sequencial(
    #         left_records=records,
    #         right_records=dds_write_records,
    #         left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
    #         join_left_key=join_keys,
    #         join_right_key=join_keys,
    #         how='left',
    #         progress_label='binding: rclcppp_publish and dds_write',
    #     )

    #     # TODO(hsgwa) split by publisher_handle
    #     dds_stamp = self._dds_bind_addr_to_stamp
    #     records = merge_sequencial(
    #         left_records=records,
    #         right_records=dds_stamp,
    #         left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #         join_left_key=join_keys,
    #         join_right_key=join_keys,
    #         how='left',
    #         progress_label='binding: rclcppp_publish and source_timestamp',
    #     )
    #     return records

    # def get_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
    #     columns = [
    #         COLUMN_NAME.PID,
    #         COLUMN_NAME.TID,
    #         COLUMN_NAME.PUBLISHER_HANDLE,
    #         COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.DDS_WRITE_TIMESTAMP,
    #         COLUMN_NAME.SOURCE_TIMESTAMP,
    #         COLUMN_NAME.MESSAGE_TIMESTAMP,
    #     ]
    #     intra_records = self.get_intra_records(publisher)
    #     inter_records = self.get_inter_records(publisher)

    #     # intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])

    #     # When publishing to both intra-process and inter-process,
    #     # intra-process communication is done first.
    #     # On the other hand,
    #     # there are cases where only one or the other is done, so we use outer join.
    #     # https://github.com/ros2/rclcpp/blob/galactic/rclcpp/include/rclcpp/publisher.hpp#L203
    #     join_keys = [
    #         COLUMN_NAME.PUBLISHER_HANDLE,
    #         COLUMN_NAME.PID,
    #         COLUMN_NAME.TID,
    #     ]
    #     intra_publish_column = intra_records.columns.get_by_base_name(
    #         COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP)
    #     inter_publish_column = inter_records.columns.get_by_base_name(
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)

    #     records = merge_sequencial(
    #         left_records=intra_records,
    #         right_records=inter_records,
    #         left_stamp_key=intra_publish_column.column_name,
    #         right_stamp_key=inter_publish_column.column_name,
    #         join_left_key=join_keys,
    #         join_right_key=join_keys,
    #         how='outer',
    #         progress_label='binding intra_publish and inter_publish'
    #     )

    #     publish_stamps = []
    #     maxsize = 2**64-1

    #     for record in records.data:
    #         rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
    #         if inter_publish_column.column_name in record.columns:
    #             rclcpp_publish = record.get(inter_publish_column.column_name)
    #         if intra_publish_column.column_name in record.columns:
    #             rclcpp_intra_publish = record.get(intra_publish_column.column_name)

    #         inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
    #         publish_stamps.append(inter_intra_publish)

    #     records.append_column(
    #         ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, [
    #             ColumnAttribute.SYSTEM_TIME,
    #             ColumnAttribute.SEND_MSG,
    #             ColumnAttribute.NODE_IO
    #         ]),
    #         publish_stamps)
    #     publish_column = records.columns.get_by_base_name(
    #         COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
    #     publish_column.add_prefix(publisher.topic_name)

    #     # columns = []
    #     # columns.append(COLUMN_NAME.PUBLISHER_HANDLE)
    #     # columns.append(COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)
    #     # if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in publish_records.column_names:
    #     #     columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
    #     # if COLUMN_NAME.DDS_WRITE_TIMESTAMP in publish_records.column_names:
    #     #     columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

    #     # drop_columns = list(set(publish_records.column_names) - set(columns))
    #     # publish_records.drop_columns(drop_columns)
    #     # publish_records.reindex(columns)

    #     # publisher_handles = self._helper.get_publisher_handles(publisher)
    #     # pub_records = self._source.publish_records(publisher_handles)

    #     # tilde_publishers = self._helper.get_tilde_publishers(publisher)
    #     # tilde_records = self._source.tilde_publish_records(tilde_publishers)

    #     # pub_records = merge_sequencial(
    #     #     left_records=tilde_records,
    #     #     right_records=pub_records,
    #     #     left_stamp_key='tilde_publish_timestamp',
    #     #     right_stamp_key='rclcpp_publish_timestamp',
    #     #     join_left_key=None,
    #     #     join_right_key=None,
    #     #     how='right',
    #     #     progress_label='binding: tilde_records',
    #     # )

    #     # columns = [
    #     #     COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
    #     # ]
    #     # if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.columns:
    #     #     columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
    #     # if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.columns:
    #     #     columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP)
    #     # columns.append(COLUMN_NAME.TILDE_MESSAGE_ID)

    #     ordered_columns = records.columns.gets_by_base_name(*columns)
    #     ordered_column_names = [str(_) for _ in ordered_columns]
    #     records.columns.reindex(ordered_column_names)

    #     attr_remove_columns = records.columns.gets_by_base_name(
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #     )

    #     for column in attr_remove_columns:
    #         column.attrs.remove(ColumnAttribute.NODE_IO)

    #     return records


class TransformSetRecordsContainer:
    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        info: LttngInfo,
        cb_records: CallbackRecordsContainer
    ) -> None:
        self._bridge = bridge
        self._info = info
        self._data = data
        self._cb_records = cb_records
        self._set_records = GroupedRecords(
            data.tf_set_transform,
            ['tf_buffer_core', 'frame_id_compact', 'child_frame_id_compact']
        )

    def get_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        columns = [
            'pid',
            'tid',
            'set_transform_timestamp',
            'tf_timestamp']

        buffer_lttng = self._bridge.get_tf_buffer(buffer)
        transform = buffer.listen_transform

        to_frame_id = self._info.get_tf_buffer_frame_compact_map(
            buffer_lttng.buffer_handler)
        to_compact_frame_id = {v: k for k, v in to_frame_id.items()}
        frame_id_compact = to_compact_frame_id[transform.source_frame_id]
        child_frame_id_compact = to_compact_frame_id[transform.target_frame_id]

        records = self._set_records.get(
            buffer_lttng.buffer_handler, frame_id_compact, child_frame_id_compact)

        # records = set_tf
        # frame_ids = []
        # child_frame_ids = []
        # for record in records:
        #     to_frame_id = self._info.get_tf_buffer_frame_compact_map(
        #         record.get('tf_buffer_core'))
        #     frame_id = to_frame_id[record.get('frame_id_compact')]
        #     frame_ids.append(frame_mapper.get_key(frame_id))
        #     child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
        #     child_frame_ids.append(frame_mapper.get_key(child_frame_id))
        # records.append_column(
        #     Column('frame_id', mapper=frame_mapper),
        #     frame_ids
        # )
        # records.append_column(
        #     Column('child_frame_id', mapper=frame_mapper),
        #     child_frame_ids
        # )
        records.columns.drop([
            'frame_id_compact', 'child_frame_id_compact',
            'tf_buffer_core'
        ], base_name_match=True)

        records.columns.reindex(columns, base_name_match=True)

        return records


class TransformLookupContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        info: LttngInfo,
    ) -> None:
        self._bridge = bridge
        self._data = data
        self._info = info
        self._lookup_start = GroupedRecords(
            data.tf_lookup_transform_start,
            ['tf_buffer_core', 'target_frame_id_compact', 'source_frame_id_compact']
        )
        self._lookup_end = GroupedRecords(
            data.tf_lookup_transform_end,
            ['tf_buffer_core']
        )

    def get_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        columns = ['pid', 'tid', 'tf_buffer_core', 'lookup_transform_start_timestamp',
                   'tf_lookup_target_time', 'lookup_transform_end_timestamp']

        transform = buffer.lookup_transform
        buffer_lttng = self._bridge.get_tf_buffer(buffer)

        to_frame_id = self._info.get_tf_buffer_frame_compact_map(
            buffer_lttng.buffer_handler)
        to_compact_frame_id = {v: k for k, v in to_frame_id.items()}
        source_frame_id_compact = to_compact_frame_id[transform.source_frame_id]
        target_frame_id_compact = to_compact_frame_id[transform.target_frame_id]

        lookup_start = self._lookup_start.get(
            buffer_lttng.buffer_handler, target_frame_id_compact, source_frame_id_compact
        )
        lookup_end = self._lookup_end.get(
            buffer_lttng.buffer_handler
        )

        join_key = [
            'pid', 'tid', 'tf_buffer_core'
        ]
        records = merge_sequencial(
            left_records=lookup_start,
            right_records=lookup_end,
            join_left_key=join_key,
            join_right_key=join_key,
            left_stamp_key='lookup_transform_start_timestamp',
            right_stamp_key='lookup_transform_end_timestamp',
            how='left'
        )

        records.columns.drop(
            ['target_frame_id_compact', 'source_frame_id_compact'], base_name_match=True)
        records.columns.reindex(columns, base_name_match=True)

        prefix_columns = records.columns.gets(
            [
                'lookup_transform_start_timestamp', 'tf_lookup_target_time',
                'lookup_transform_end_timestamp'
            ], base_name_match=True
        )
        for column in prefix_columns:
            column.add_prefix(buffer.lookup_node_name)

        return records


class TransformCommRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        info: LttngInfo,
        send_records: TransformSendRecordsContainer,
        set_records: TransformSetRecordsContainer,
        lookup_records: TransformLookupContainer,
    ) -> None:
        self._bridge = bridge
        self._data = data
        self._info = info
        self._send_records = send_records
        self._set_records = set_records
        self._lookup_records = lookup_records

        closest = data.tf_buffer_find_closest
        closest.columns.rename({'stamp': 'tf_timestamp'})
        self._find_closest = GroupedRecords(
            closest,
            ['tf_buffer_core', 'frame_id_compact', 'child_frame_id_compact'])

    def get_inter_records(
        self,
        comm: TransformCommunicationStructValue,
    ) -> RecordsInterface:
        buffer = self._bridge.get_tf_buffer(comm.buffer)
        listen_transform = comm.listen_transform

        columns = [
            'pid', 'tid', 'rclcpp_publish_timestamp', 'rcl_publish_timestamp',
            'dds_write_timestamp', 'set_transform_timestamp',
            'lookup_transform_start_timestamp',
            'tf_lookup_target_time',
            'lookup_transform_end_timestamp'
            ]

        to_frame_id = self._info.get_tf_buffer_frame_compact_map(
            buffer.buffer_handler)
        to_compact_frame_id = {v: k for k, v in to_frame_id.items()}
        buffer_frame_id_compact = to_compact_frame_id[listen_transform.source_frame_id]
        child_buffer_frame_id_compact = to_compact_frame_id[listen_transform.target_frame_id]
        closest = self._find_closest.get(
            buffer.buffer_handler, buffer_frame_id_compact, child_buffer_frame_id_compact)

        # TODO(hsgwa): 遅延ありの２つ目の座標版も追加する。
        lookup_records = self._lookup_records.get_records(comm.buffer)
        find_closest_column = closest.columns.get(
            'find_closest_timestamp',
            base_name_match=True
        )
        lookup_end_timestamp = lookup_records.columns.get(
            'lookup_transform_end_timestamp',
            base_name_match=True
        )

        join_keys = [
            'pid', 'tid', 'tf_buffer_core'
        ]
        records = merge_sequencial(
            left_records=closest,
            right_records=lookup_records,
            left_stamp_key=find_closest_column.column_name,
            right_stamp_key=lookup_end_timestamp.column_name,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left_use_latest'
        )

        set_records = self._set_records.get_records(comm.buffer)
        if COLUMN_NAME.CALLBACK_END_TIMESTAMP in set_records.column_names:
            set_records.columns.drop(
                [COLUMN_NAME.CALLBACK_END_TIMESTAMP],
                base_name_match=True
            )

        set_tf_timestamp_column = set_records.columns.get(
            'tf_timestamp',
            base_name_match=True
        )
        records_tf_timestamp_column = records.columns.get(
            'tf_timestamp',
            base_name_match=True
        )
        records = merge(
            left_records=set_records,
            right_records=records,
            join_left_key=set_tf_timestamp_column.column_name,
            join_right_key=records_tf_timestamp_column.column_name,
            how='left',
        )

        send_records = self._send_records.get_records(comm.broadcaster)
        send_timestamp_column = send_records.columns.get(
            'tf_timestamp',
            base_name_match=True
        )

        records = merge(
            left_records=send_records,
            right_records=records,
            join_left_key=send_timestamp_column.column_name,
            join_right_key=records_tf_timestamp_column.column_name,
            how='left'
        )
        records.columns.drop([
            'frame_id',
            'child_frame_id',
            'tf_timestamp',
            'frame_id_compact',
            'child_frame_id_compact',
            'tf_buffer_core',
            'find_closest_timestamp',
        ])

        records.columns.reindex(columns, base_name_match=True)
        lookup_end_column = records.columns.get(
            'lookup_transform_end_timestamp',
            base_name_match=True
        )
        lookup_end_column.attrs.add(ColumnAttribute.NODE_IO)

        return records
