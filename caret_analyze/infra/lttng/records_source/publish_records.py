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

from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ....record import (
    ColumnAttribute,
    ColumnValue,
    GroupedRecords,
    merge_sequencial,
    RecordsInterface,
)
from ....value_objects import PublisherStructValue


class PublishRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
    ) -> None:
        self._intra_publish = GroupedRecords(
            data.rclcpp_intra_publish,
            [
                COLUMN_NAME.PUBLISHER_HANDLE,
            ]
        )
        self._inter_publish = GroupedRecords(
            data.inter_publish,
            [
                COLUMN_NAME.PUBLISHER_HANDLE,
            ]
        )
        self._tilde_publish = GroupedRecords(
            data.tilde_publish,
            [
                COLUMN_NAME.TILDE_PUBLISHER
            ]
        )

        self._bridge = bridge

    def get_intra_records(
        self,
        publisher: PublisherStructValue,
    ) -> RecordsInterface:
        return self._get_intra_records(publisher).clone()

    @lru_cache
    def _get_intra_records(
        self,
        publisher: PublisherStructValue,
    ) -> RecordsInterface:
        publishers_lttng = self._bridge.get_publishers(publisher)
        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]

        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]

        records = self._intra_publish.get(publisher_lttng.publisher_handle).clone()
        records.columns.drop([
            COLUMN_NAME.MESSAGE,
            COLUMN_NAME.PUBLISHER_HANDLE,
        ], base_name_match=True)
        records.columns.reindex(columns)
        ordered_columns = records.columns.gets([
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP], base_name_match=True)
        for column in ordered_columns:
            column.add_prefix(publisher.topic_name)

        records.columns.reindex(columns, base_name_match=True)
        return records

    def get_inter_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        return self._get_inter_records(publisher).clone()

    @lru_cache
    def _get_inter_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        publishers_lttng = self._bridge.get_publishers(publisher)
        assert len(publishers_lttng) == 1
        publisher_lttng = publishers_lttng[0]

        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
        ]

        records = self._inter_publish.get(publisher_lttng.publisher_handle)

        if COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP in records.column_names:
            records.columns.drop([
                COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
            ], base_name_match=True)
        records.columns.drop([
            COLUMN_NAME.PUBLISHER_HANDLE,
        ], base_name_match=True)

        records.columns.reindex(columns)
        prefix_columns = records.columns.gets(
            [
                COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
                COLUMN_NAME.SOURCE_TIMESTAMP,
            ], base_name_match=True
        )
        for column in prefix_columns:
            column.add_prefix(publisher.topic_name)

        return records

    def get_records(self, publisher: PublisherStructValue) -> RecordsInterface:
        return self._get_records(publisher).clone()

    def get_tilde_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        publishers_lttng = self._bridge.get_publishers(publisher)
        publishers_tilde = [pub for pub in publishers_lttng if pub.tilde_publisher is not None]

        assert len(publishers_tilde) <= 1

        if len(publishers_tilde) == 0 or publishers_tilde[0].tilde_publisher is None:
            return self._tilde_publish.get(0)  # return empty records

        publisher_tilde = publishers_tilde[0]
        assert publisher_tilde.tilde_publisher is not None
        tilde_records = self._tilde_publish.get(publisher_tilde.tilde_publisher)

        return tilde_records

    def _has_tilde(self, publisher: PublisherStructValue) -> bool:
        publishers_lttng = self._bridge.get_publishers(publisher)
        for pub_lttng in publishers_lttng:
            if pub_lttng.tilde_publisher:
                return True
        return False

    @lru_cache
    def _get_records(self, publisher: PublisherStructValue) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
        ]
        if self._has_tilde(publisher):
            columns.extend([
                COLUMN_NAME.TILDE_SUBSCRIPTION,
                COLUMN_NAME.TILDE_MESSAGE_ID,
            ])
        columns.extend(
            [
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
                COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
                COLUMN_NAME.SOURCE_TIMESTAMP,
            ]
        )
        intra_records = self.get_intra_records(publisher)
        inter_records = self.get_inter_records(publisher)

        # intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])

        # When publishing to both intra-process and inter-process,
        # intra-process communication is done first.
        # On the other hand,
        # there are cases where only one or the other is done, so we use outer join.
        # https://github.com/ros2/rclcpp/blob/galactic/rclcpp/include/rclcpp/publisher.hpp#L203
        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
        ]
        intra_publish_column = intra_records.columns.get(
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP, base_name_match=True)
        inter_publish_column = inter_records.columns.get(
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP, base_name_match=True)

        records = merge_sequencial(
            left_records=intra_records,
            right_records=inter_records,
            left_stamp_key=intra_publish_column.column_name,
            right_stamp_key=inter_publish_column.column_name,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='outer',
            progress_label='binding intra_publish and inter_publish'
        )

        publish_stamps = []
        maxsize = 2**64-1

        for record in records.data:
            rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
            if inter_publish_column.column_name in record.columns:
                rclcpp_publish = record.get(inter_publish_column.column_name)
            if intra_publish_column.column_name in record.columns:
                rclcpp_intra_publish = record.get(intra_publish_column.column_name)

            inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
            publish_stamps.append(inter_intra_publish)

        records.append_column(
            ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, [
                ColumnAttribute.SYSTEM_TIME,
                ColumnAttribute.NODE_IO
            ]),
            publish_stamps)

        records.columns.drop([
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP],
            base_name_match=True)

        publish_column = records.columns.get(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
        publish_column.add_prefix(publisher.topic_name)

        if self._has_tilde(publisher):
            tilde_records = self.get_tilde_records(publisher)
            publish_column = records.columns.get(
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, base_name_match=True)
            tilde_pub_column = tilde_records.columns.get(
                'tilde_publish_timestamp', base_name_match=True)
            join_keys = [
                COLUMN_NAME.TID
            ]
            records = merge_sequencial(
                left_records=tilde_records,
                right_records=records,
                left_stamp_key=tilde_pub_column.column_name,
                right_stamp_key=publish_column.column_name,
                join_left_key=join_keys,
                join_right_key=join_keys,
                how='inner'
            )
            records.columns.drop([
                COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP,
                COLUMN_NAME.TILDE_PUBLISHER,
            ])
            attr_columns = records.columns.gets(
                [
                    COLUMN_NAME.TILDE_SUBSCRIPTION,
                    COLUMN_NAME.TILDE_MESSAGE_ID,
                ]
            )
            for attr_column in attr_columns:
                attr_column.add_prefix(publisher.topic_name)

        records.columns.reindex(columns, base_name_match=True)

        return records
