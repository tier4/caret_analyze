from typing import Union

from functools import lru_cache

from caret_analyze.record.column import ColumnValue

from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    PublisherValueLttng
)
from ....record import RecordsInterface, merge_sequencial, GroupedRecords, Column, ColumnAttribute


class PublishRecordsContainer:

    def __init__(
        self,
        data: Ros2DataModel,
    ) -> None:
        self._intra_rclcpp_publish = GroupedRecords(
            data.rclcpp_intra_publish,
            [
                COLUMN_NAME.PUBLISHER_HANDLE
            ]
        )

        self._inter_rclcpp_publish = GroupedRecords(
            data.rclcpp_publish,
            [
                COLUMN_NAME.PUBLISHER_HANDLE
            ]
        )

        self._rcl_publish = GroupedRecords(
            data.rcl_publish,
            [
                COLUMN_NAME.PUBLISHER_HANDLE
            ]
        )
        self._dds_write = data.dds_write
        self._dds_bind_addr_to_stamp = data.dds_bind_addr_to_stamp

    @lru_cache
    def get_intra_records(
        self,
        publisher: PublisherValueLttng,
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]

        records = self._intra_rclcpp_publish.get(publisher.publisher_handle)
        records.columns.drop(
            [
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.PUBLISHER_HANDLE,
            ]
        )
        records.columns.reindex(columns)
        ordered_columns = records.columns.gets_by_base_name(*columns[3:])
        for column in ordered_columns:
            column.add_prefix(publisher.topic_name)
        return records

    @lru_cache
    def get_inter_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]

        records = self._get_inter_records(publisher)

        records.columns.drop([
            COLUMN_NAME.MESSAGE,
            COLUMN_NAME.ADDR,
            COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
            COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            COLUMN_NAME.PUBLISHER_HANDLE,
        ])

        records.columns.reindex(columns)
        ordered_columns = records.columns.gets_by_base_name(*columns[3:])
        for column in ordered_columns:
            column.add_prefix(publisher.topic_name)

        return records

    def _get_inter_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
        records = self._inter_rclcpp_publish.get(publisher.publisher_handle)

        join_keys = [
            # COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            # COLUMN_NAME.PUBLISHER_HANDLE
        ]

        rcl_publish_records = self._rcl_publish.get(publisher.publisher_handle)
        records = merge_sequencial(
            left_records=records,
            right_records=rcl_publish_records,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left',
            progress_label='binding: rclcpp_publish and rcl_publish',
        )

        # TODO(hsgwa) split by publisher_handle
        dds_write_records = self._dds_write
        records = merge_sequencial(
            left_records=records,
            right_records=dds_write_records,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left',
            progress_label='binding: rclcppp_publish and dds_write',
        )

        # TODO(hsgwa) split by publisher_handle
        dds_stamp = self._dds_bind_addr_to_stamp
        records = merge_sequencial(
            left_records=records,
            right_records=dds_stamp,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left',
            progress_label='binding: rclcppp_publish and source_timestamp',
        )
        return records

    def get_records(self, publisher: PublisherValueLttng) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]
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
        intra_publish_column = intra_records.columns.get_by_base_name(
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP)
        inter_publish_column = inter_records.columns.get_by_base_name(
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)

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
                ColumnAttribute.SEND_MSG,
                ColumnAttribute.NODE_IO
            ]),
            publish_stamps)
        publish_column = records.columns.get_by_base_name(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
        publish_column.add_prefix(publisher.topic_name)

        # columns = []
        # columns.append(COLUMN_NAME.PUBLISHER_HANDLE)
        # columns.append(COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)
        # if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in publish_records.column_names:
        #     columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        # if COLUMN_NAME.DDS_WRITE_TIMESTAMP in publish_records.column_names:
        #     columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        # columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        # columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

        # drop_columns = list(set(publish_records.column_names) - set(columns))
        # publish_records.drop_columns(drop_columns)
        # publish_records.reindex(columns)

        # publisher_handles = self._helper.get_publisher_handles(publisher)
        # pub_records = self._source.publish_records(publisher_handles)

        # tilde_publishers = self._helper.get_tilde_publishers(publisher)
        # tilde_records = self._source.tilde_publish_records(tilde_publishers)

        # pub_records = merge_sequencial(
        #     left_records=tilde_records,
        #     right_records=pub_records,
        #     left_stamp_key='tilde_publish_timestamp',
        #     right_stamp_key='rclcpp_publish_timestamp',
        #     join_left_key=None,
        #     join_right_key=None,
        #     how='right',
        #     progress_label='binding: tilde_records',
        # )

        # columns = [
        #     COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        # ]
        # if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.columns:
        #     columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        # if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.columns:
        #     columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        # columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        # columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)
        # columns.append(COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP)
        # columns.append(COLUMN_NAME.TILDE_MESSAGE_ID)

        remove_columns = records.columns.gets_by_base_name(
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
        )
        records.columns.drop([str(c) for c in remove_columns])

        ordered_columns = records.columns.gets_by_base_name(*columns)
        ordered_column_names = [str(_) for _ in ordered_columns]
        records.columns.reindex(ordered_column_names)

        return records
