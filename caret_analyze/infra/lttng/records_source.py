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

from typing import Optional

from .column_names import COLUMN_NAME
from .ros2_tracing.data_model import Ros2DataModel
from ...common import Columns
from ...record import (merge, merge_sequencial,
                       merge_sequencial_for_addr_track, RecordsInterface)


class RecordsSource():
    # """測定結果から、recordsの元となる情報を作る。
    # １回だけ作ったら、後はキャッシュする。mergeしか行わない。　
    # """

    def __init__(
        self,
        data: Ros2DataModel,
    ) -> None:
        self._data = data

        self._timer_callback_records_cache: Optional[RecordsInterface] = None
        self._intra_process_callback_records_cache: Optional[RecordsInterface] = None
        self._inter_process_callback_records_cache: Optional[RecordsInterface] = None
        self._intra_process_publish_records_cache: Optional[RecordsInterface] = None
        self._inter_process_publish_records_cache: Optional[RecordsInterface] = None
        self._intra_process_communication_records_cache: Optional[RecordsInterface] = None
        self._inter_process_communication_records_cache: Optional[RecordsInterface] = None

    def compose_inter_proc_comm_records(self) -> RecordsInterface:
        """
        Compose inter process communication records.

        Returns
        -------
        RecordsInterface
            columns
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp
            - dds_write_timestamp
            - message_timestamp
            - source_timestamp

        """
        dds_write = merge_sequencial_for_addr_track(
            source_records=self._data.dds_write_instances,
            copy_records=self._data.dds_bind_addr_to_addr,
            sink_records=self._data.dds_bind_addr_to_stamp,
            source_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ADDR_FROM,
            copy_to_key=COLUMN_NAME.ADDR_TO,
            sink_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            sink_from_key=COLUMN_NAME.ADDR,
            columns=[
                COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.SOURCE_TIMESTAMP,
            ],
            progress_label='binding: message_addr and dds_write',
        )

        rcl_publish_records = self._data.rcl_publish_instances
        rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
        publish = merge_sequencial(
            left_records=self._data.rclcpp_publish_instances,
            right_records=rcl_publish_records,
            left_stamp_key=COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE,
            join_right_key=COLUMN_NAME.MESSAGE,
            how='left_use_latest',
            columns=[
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
                COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                COLUMN_NAME.PUBLISHER_HANDLE,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
            ],
            progress_label='binding: rclcpp_publish and rcl_publish',
        )

        publish = merge_sequencial(
            left_records=publish,
            right_records=dds_write,
            left_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE,
            join_right_key=COLUMN_NAME.MESSAGE,
            columns=Columns(publish.columns + dds_write.columns).as_list(),
            how='left',
            progress_label='binding: rcl_publish and dds_write',
        )

        callback_start_instances = self._data.callback_start_instances.clone()
        callback_start_instances.filter_if(lambda x: x.get(COLUMN_NAME.IS_INTRA_PROCESS) == 0)
        subscription = self._data.dispatch_subscription_callback_instances

        subscription = merge_sequencial(
            left_records=subscription,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns(subscription.columns + callback_start_instances.columns).as_list(),
            how='left_use_latest',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )

        # communication = merge(
        #     publish,
        #     self._data.on_data_available_instances,
        #     join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
        #     join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
        #     columns=publish.columns,
        #     how='left',
        #     progress_label='binding: source_timestamp and on_data_available',
        # )

        communication = merge(
            publish,
            subscription,
            join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            columns=Columns(publish.columns + subscription.columns).as_list(),
            how='left',
            progress_label='binding: source_timestamp and callback_start',
        )

        communication.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS,
                COLUMN_NAME.ADDR,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
                COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            ],
        )

        return communication

    def compose_intra_proc_comm_records(self) -> RecordsInterface:
        """
        Compose intra process communication records.

        Returns
        -------
        RecordsInterface
            columns:
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_intra_publish_timestamp
            - message_timestamp

        """
        sink_records = self._data.dispatch_intra_process_subscription_callback_instances
        intra_publish_records = merge_sequencial_for_addr_track(
            source_records=self._data.rclcpp_intra_publish_instances,
            copy_records=self._data.message_construct_instances,
            sink_records=sink_records,
            source_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.MESSAGE_CONSTRUCT_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ORIGINAL_MESSAGE,
            copy_to_key=COLUMN_NAME.CONSTRUCTED_MESSAGE,
            sink_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            sink_from_key=COLUMN_NAME.MESSAGE,
            columns=[
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.PUBLISHER_HANDLE,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
            ],
            progress_label='bindig: publish_timestamp and message_addr',
        )

        callback_start_instances = self._data.callback_start_instances.clone()
        callback_start_instances.filter_if(lambda x: x.get(COLUMN_NAME.IS_INTRA_PROCESS) == 1)

        intra_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=intra_publish_records.columns + [
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, COLUMN_NAME.IS_INTRA_PROCESS],
            how='left_use_latest',
            progress_label='bindig: dispath_subsription and callback_start',
        )

        intra_records.drop_columns(
            [
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )

        return intra_records

    def compose_callback_records(self, limit: int = 0) -> RecordsInterface:
        """
        Compose callback records.

        Returns
        -------
        RecordsInterface
            columns:
            - callback_start_timestamp
            - callback_end_timestamp
            - callback_object

        """
        records: RecordsInterface

        records = merge_sequencial(
            left_records=self._data.callback_start_instances,
            right_records=self._data.callback_end_instances,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=[
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.CALLBACK_END_TIMESTAMP,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.IS_INTRA_PROCESS,
            ],
            how='inner',
            progress_label=('binding: '
                            f'{COLUMN_NAME.CALLBACK_START_TIMESTAMP} and '
                            f'{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'),
        )

        records.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )

        return records
