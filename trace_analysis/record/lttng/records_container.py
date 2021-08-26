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

from typing import Tuple

from trace_analysis.record import (
    Record,
    Records,
    merge,
    merge_sequencial,
    merge_sequencial_with_copy,
)
from .impl import (
    CallbackImpl,
    SubscriptionCallbackImpl,
    TimerCallbackImpl,
)
from .util import Ros2DataModelUtil
from .dataframe_container import DataframeContainer


class RecordsContainer:
    def __init__(self, data_util: Ros2DataModelUtil, dataframe_container: DataframeContainer):
        self._dataframe_coitainer = dataframe_container
        self._data_util = data_util

        drop_inter_mediate_columns = True

        (
            self._timer_callback_records,
            self._intra_process_callback_records,
            self._inter_process_callback_records,
        ) = self._compose_callback_records(drop_inter_mediate_columns)

        self._intra_process_publish_records = self._compose_intra_publish_records()
        self._intra_process_communication_records = self._compose_intra_process_communication(
            self._intra_process_callback_records, drop_inter_mediate_columns
        )

        self._inter_process_publish_records = self._compose_inter_publish_records()
        self._inter_process_communication_records = self._compose_inter_process_records(
            self._inter_process_callback_records, drop_inter_mediate_columns
        )

    def get_publish_records(self, publisher_handle, is_intra_process) -> Records:
        def is_target_publish(x):
            return x["publisher_handle"] == publisher_handle

        records: Records
        if is_intra_process:
            records = self._intra_process_publish_records.filter(is_target_publish)  # type: ignore
        else:
            records = self._inter_process_publish_records.filter(is_target_publish)  # type: ignore

        return records

    def get_communication_records(self, is_intra_process: bool):
        if is_intra_process:
            return self._intra_process_communication_records
        else:
            return self._inter_process_communication_records

    def _get_subscription_callback_records(self, callback: SubscriptionCallbackImpl):
        def has_same_intra_callback_object(record: Record):
            return record.get("callback_object") == callback.intra_callback_object

        def has_same_inter_callback_object(record: Record):
            return record.get("callback_object") == callback.inter_callback_object

        records: Records
        records = self._inter_process_callback_records.filter(
            has_same_inter_callback_object
        )  # type: ignore

        if callback.intra_callback_object is not None:
            records += self._intra_process_callback_records.filter(
                has_same_intra_callback_object
            )  # type: ignore

        return records

    def _get_timer_callback_records(self, callback: TimerCallbackImpl):
        def has_same_callback_object(record: Record):
            return record.get("callback_object") == callback.callback_object

        return self._timer_callback_records.filter(has_same_callback_object)

    def get_callback_records(self, callback: CallbackImpl):
        records: Records

        if isinstance(callback, SubscriptionCallbackImpl):
            records = self._get_subscription_callback_records(callback)
        elif isinstance(callback, TimerCallbackImpl):
            records = self._get_timer_callback_records(callback)

        records.sort(key=lambda x: x["callback_start_timestamp"])
        return records

    def _compose_intra_publish_records(self) -> Records:
        return self._data_util.data.rclcpp_intra_publish_instances.drop_columns(["message"])

    def _compose_inter_publish_records(self) -> Records:
        return self._data_util.data.rcl_publish_instances.drop_columns(["message"])

    def _compose_callback_records(
        self, drop_inter_mediate_columns
    ) -> Tuple[Records, Records, Records]:
        callback_records = merge_sequencial(
            left_records=self._data_util.data.callback_start_instances,
            right_records=self._data_util.data.callback_end_instances,
            left_stamp_key="callback_start_timestamp",
            right_stamp_key="callback_end_timestamp",
            join_key="callback_object",
        )

        timer_callback_records = Records()
        subscription_callback_records = Records()

        cb_to_records = {}

        cb_to_records.update(
            {
                _: timer_callback_records
                for _ in self._dataframe_coitainer.get_timer_info()["callback_object"]
            }
        )
        cb_to_records.update(
            {
                _: subscription_callback_records
                for _ in self._dataframe_coitainer.get_subscription_info()["callback_object"]
            }
        )

        for callback_record in callback_records:
            callback_object = callback_record["callback_object"]
            records = cb_to_records[callback_object]
            records.append(callback_record)

        subscription_intra_callback_records = Records()
        subscription_inter_callback_records = Records()

        subscription_intra_callback_records = Records(
            [
                callback_record
                for callback_record in subscription_callback_records
                if callback_record["is_intra_process"] == 1
            ]
        )

        subscription_inter_callback_records = Records(
            [
                callback_record
                for callback_record in subscription_callback_records
                if callback_record["is_intra_process"] == 0
            ]
        )

        if drop_inter_mediate_columns:
            timer_callback_records.drop_columns(["is_intra_process"], inplace=True)
            subscription_intra_callback_records.drop_columns(["is_intra_process"], inplace=True)
            subscription_inter_callback_records.drop_columns(["is_intra_process"], inplace=True)

        return (
            timer_callback_records,
            subscription_intra_callback_records,
            subscription_inter_callback_records,
        )

    def _compose_intra_process_communication(
        self, intra_process_callback_records, drop_inter_mediate_columns
    ) -> Records:
        # inter_subscription = self._get_inter_subscription_records(
        #     self.inter_callback, drop_inter_mediate_columns)
        # アドレスでの紐付けなので、トピックなどを全てまぜた状態での算出が必要
        intra_publish_records = merge_sequencial_with_copy(
            source_records=self._data_util.data.rclcpp_intra_publish_instances,
            copy_records=self._data_util.data.message_construct_instances,
            sink_records=self._data_util.data.dispatch_intra_process_instances,
            source_stamp_key="rclcpp_intra_publish_timestamp",
            source_key="message",
            copy_stamp_key="message_construct_timestamp",
            copy_from_key="original_message",
            copy_to_key="constructed_message",
            sink_stamp_key="dispatch_intra_process_timestamp",
            sink_from_key="message",
        )

        intra_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=intra_process_callback_records,
            left_stamp_key="dispatch_intra_process_timestamp",
            right_stamp_key="callback_start_timestamp",
            join_key="callback_object",
        )

        if drop_inter_mediate_columns:
            intra_records.drop_columns(
                [
                    "dispatch_intra_process_timestamp",
                    "message",
                    "callback_end_timestamp",
                ],
                inplace=True,
            )

        return intra_records

    def _compose_inter_process_records(
        self, inter_process_callback, drop_inter_mediate_columns
    ) -> Records:
        # アドレスでの紐付けなので、トピックなどを全てまぜた状態での算出が必要
        dds_write = merge_sequencial_with_copy(
            source_records=self._data_util.data.dds_write_instances,
            copy_records=self._data_util.data.dds_bind_addr_to_addr,
            sink_records=self._data_util.data.dds_bind_addr_to_stamp,
            source_stamp_key="dds_write_timestamp",
            source_key="message",
            copy_stamp_key="dds_bind_addr_to_addr_timestamp",
            copy_from_key="addr_from",
            copy_to_key="addr_to",
            sink_stamp_key="dds_bind_addr_to_stamp_timestamp",
            sink_from_key="addr",
        )

        publish = merge_sequencial(
            left_records=self._data_util.data.rclcpp_publish_instances,
            right_records=self._data_util.data.rcl_publish_instances,
            left_stamp_key="rclcpp_publish_timestamp",
            right_stamp_key="rcl_publish_timestamp",
            join_key="message",
        )

        publish = merge_sequencial(
            left_records=publish,
            right_records=dds_write,
            left_stamp_key="rcl_publish_timestamp",
            right_stamp_key="dds_write_timestamp",
            join_key="message",
        )

        subscription = merge_sequencial(
            left_records=self._data_util.data.take_type_erased_instances,
            right_records=self._data_util.data.dispatch_instances,
            left_stamp_key="take_type_erased_timestamp",
            right_stamp_key="dispatch_timestamp",
            join_key="message",
        )

        subscription = merge_sequencial(
            left_records=subscription,
            right_records=inter_process_callback.drop_columns(["callback_end_timestamp"]),
            left_stamp_key="dispatch_timestamp",
            right_stamp_key="callback_start_timestamp",
            join_key="callback_object",
        )

        communication = merge(
            publish,
            self._data_util.data.on_data_available_instances,
            join_key="source_timestamp",
            how="left",
        )

        communication = merge(communication, subscription, join_key="source_timestamp", how="left")

        if drop_inter_mediate_columns:
            communication.drop_columns(
                [
                    "addr",
                    "message",
                    "source_timestamp",
                    "dds_bind_addr_to_stamp_timestamp",
                    "take_type_erased_timestamp",
                    "dispatch_timestamp",
                ],
                inplace=True,
            )

        return communication
