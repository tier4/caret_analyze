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

from caret_analyze.record.column import ColumnAttribute

from .callback_records import CallbackRecordsContainer
from .publish_records import PublishRecordsContainer
from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ..lttng_info import LttngInfo
from ..ros2_tracing.data_model import Ros2DataModel
from ....record import (
    GroupedRecords,
    merge,
    merge_sequencial,
    RecordsInterface,
)
from ....value_objects import (
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
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

        records.columns.drop(
            [
                'frame_id_compact', 'child_frame_id_compact', 'broadcaster',
                'send_transform_timestamp'
            ],
            base_name_match=True
        )

        records.columns.reindex(columns, base_name_match=True)

        return records


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
        self._lookup = GroupedRecords(
            data.tf_lookup_transform,
            ['tf_buffer_core', 'target_frame_id_compact', 'source_frame_id_compact']
        )

    def get_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        columns = [
            'pid', 'tid', 'tf_buffer_core',
            'lookup_transform_start_timestamp', 'lookup_transform_end_timestamp',
        ]

        lookup_transform = buffer.lookup_transform
        buffer_lttng = self._bridge.get_tf_buffer(buffer)

        to_frame_id = self._info.get_tf_buffer_frame_compact_map(
            buffer_lttng.buffer_handler)
        to_compact_frame_id = {v: k for k, v in to_frame_id.items()}
        source_frame_id_compact = to_compact_frame_id[lookup_transform.source_frame_id]
        target_frame_id_compact = to_compact_frame_id[lookup_transform.target_frame_id]

        records = self._lookup.get(
            buffer_lttng.buffer_handler, target_frame_id_compact, source_frame_id_compact,
        )

        records.columns.drop(
            ['target_frame_id_compact', 'source_frame_id_compact'], base_name_match=True)
        records.columns.reindex(columns, base_name_match=True)

        prefix_columns = records.columns.gets(
            [
                'lookup_transform_start_timestamp', 'lookup_transform_end_timestamp'
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
