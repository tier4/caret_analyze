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

from .lttng_info import DataFrameFormatter
from .objects_with_runtime_info import CallbackWithRuntime
from .objects_with_runtime_info import SubscriptionCallbackWithRuntime
from .objects_with_runtime_info import TimerCallbackWithRuntime
from .ros2_tracing.util import Ros2DataModelUtil
from ...record import merge
from ...record import merge_sequencial
from ...record import merge_sequencial_for_addr_track
from ...record import RecordInterface
from ...record import RecordsInterface
from ...record.record_factory import RecordsFactory


class RecordsFormatter:
    def __init__(
        self, data_util: Ros2DataModelUtil, dataframe_formatter: DataFrameFormatter
    ) -> None:
        self._dataframe_coitainer = dataframe_formatter
        self._data_util = data_util

        self._drop_inter_mediate_columns = True

        self._timer_callback_records_cache: Optional[RecordsInterface] = None
        self._intra_process_callback_records_cache: Optional[RecordsInterface] = None
        self._inter_process_callback_records_cache: Optional[RecordsInterface] = None
        self._intra_process_publish_records_cache: Optional[RecordsInterface] = None
        self._inter_process_publish_records_cache: Optional[RecordsInterface] = None
        self._intra_process_communication_records_cache: Optional[RecordsInterface] = None
        self._inter_process_communication_records_cache: Optional[RecordsInterface] = None

    def get_publish_records(self, publisher_handle, is_intra_process) -> RecordsInterface:
        def is_target_publish(x):
            return x['publisher_handle'] == publisher_handle

        records: RecordsInterface
        if is_intra_process:
            intra_process_records = self._compose_intra_process_publish_records_with_cache()
            records = intra_process_records.filter_if(
                is_target_publish)  # type: ignore
        else:
            inter_process_records = self._compose_inter_process_publish_records_with_cache()
            records = inter_process_records.filter_if(
                is_target_publish)  # type: ignore

        return records

    def get_communication_records(self, is_intra_process: bool) -> RecordsInterface:
        if is_intra_process:
            return self._compose_intra_process_communication_records_with_cache()
        else:
            return self._compose_inter_process_communication_records_with_cache()

    def get_callback_records(self, callback: CallbackWithRuntime) -> RecordsInterface:
        records: RecordsInterface

        if isinstance(callback, SubscriptionCallbackWithRuntime):
            records = self._get_subscription_callback_records(callback)
        elif isinstance(callback, TimerCallbackWithRuntime):
            records = self._get_timer_callback_records(callback)

        records.sort(key='callback_start_timestamp', inplace=True)
        return records

    def _get_subscription_callback_records(
        self, callback: SubscriptionCallbackWithRuntime
    ) -> RecordsInterface:
        def has_same_intra_callback_object(record: RecordInterface):
            return record.data.get('callback_object') == callback.intra_callback_object

        def has_same_inter_callback_object(record: RecordInterface):
            return record.data.get('callback_object') == callback.inter_callback_object

        records: RecordsInterface

        inter_records = self._compose_inter_process_callback_records_with_cache()
        records = inter_records.filter_if(
            has_same_inter_callback_object)  # type: ignore

        if callback.intra_callback_object is not None:
            intra_records = self._compose_intra_process_callback_records_with_cache()
            callback_records = intra_records.filter_if(
                has_same_intra_callback_object)

            assert callback_records is not None
            records.concat(
                callback_records,
                inplace=True,
            )

        return records

    def _get_timer_callback_records(self, callback: TimerCallbackWithRuntime) -> RecordsInterface:
        def has_same_callback_object(record: RecordInterface):
            return record.data.get('callback_object') == callback.callback_object

        records = self._compose_timer_callback_records_with_cache()
        records_filtered = records.filter_if(has_same_callback_object)
        assert records_filtered is not None
        return records_filtered

    def _compose_timer_callback_records_with_cache(self) -> RecordsInterface:
        if self._timer_callback_records_cache is None:
            self._update_callback_records_cache()

        assert self._timer_callback_records_cache is not None
        return self._timer_callback_records_cache

    def _compose_intra_process_callback_records_with_cache(self) -> RecordsInterface:
        if self._intra_process_callback_records_cache is None:
            self._update_callback_records_cache()

        assert self._intra_process_callback_records_cache is not None
        return self._intra_process_callback_records_cache

    def _compose_inter_process_callback_records_with_cache(self) -> RecordsInterface:
        if self._inter_process_callback_records_cache is None:
            self._update_callback_records_cache()

        assert self._inter_process_callback_records_cache is not None
        return self._inter_process_callback_records_cache

    def _compose_intra_process_publish_records_with_cache(self) -> RecordsInterface:
        if self._intra_process_publish_records_cache is None:
            self._intra_process_publish_records_cache = (
                self._data_util.data.rcl_publish_instances.drop_columns([
                                                                        'message'])
            )

        assert self._intra_process_publish_records_cache is not None
        return self._intra_process_publish_records_cache

    def _compose_inter_process_publish_records_with_cache(self) -> RecordsInterface:
        if self._inter_process_publish_records_cache is None:
            self._inter_process_publish_records_cache = (
                self._data_util.data.rclcpp_publish_instances.drop_columns([
                                                                           'message'])
            )

        assert self._inter_process_publish_records_cache is not None
        return self._inter_process_publish_records_cache

    def _compose_intra_process_communication_records_with_cache(self) -> RecordsInterface:
        if self._intra_process_communication_records_cache is None:
            self._update_intra_process_communication_cache()

        assert self._intra_process_communication_records_cache is not None
        return self._intra_process_communication_records_cache

    def _compose_inter_process_communication_records_with_cache(self) -> RecordsInterface:
        if self._inter_process_communication_records_cache is None:
            self._update_inter_process_communication_cache()

        assert self._inter_process_communication_records_cache is not None
        return self._inter_process_communication_records_cache

    def _update_callback_records_cache(self) -> None:
        callback_records = merge_sequencial(
            left_records=self._data_util.data.callback_start_instances,
            right_records=self._data_util.data.callback_end_instances,
            left_stamp_key='callback_start_timestamp',
            right_stamp_key='callback_end_timestamp',
            join_key='callback_object',
            progress_label='binding: callback_start and callback_end',
        )

        timer_callback_records = RecordsFactory.create_instance()
        subscription_callback_records = RecordsFactory.create_instance()

        cb_to_records = {}

        cb_to_records.update(
            {
                _: timer_callback_records
                for _ in self._dataframe_coitainer.get_timer_info()['callback_object']
            }
        )
        cb_to_records.update(
            {
                _: subscription_callback_records
                for _ in self._dataframe_coitainer.get_subscription_info()['callback_object']
            }
        )

        for callback_record in callback_records.data:
            if 'callback_object' not in callback_record.columns:
                continue
            callback_object = callback_record.get('callback_object')
            if callback_object not in cb_to_records.keys():
                print('failed to find callback info: callback object = ' +
                      str(callback_object))
                continue
            records = cb_to_records[callback_object]
            records.append(callback_record)

        subscription_intra_callback_records = RecordsFactory.create_instance()
        subscription_inter_callback_records = RecordsFactory.create_instance()

        subscription_intra_callback_records = RecordsFactory.create_instance(
            [
                callback_record
                for callback_record in subscription_callback_records.data
                if callback_record.get('is_intra_process') == 1
            ]
        )

        subscription_inter_callback_records = RecordsFactory.create_instance(
            [
                callback_record
                for callback_record in subscription_callback_records.data
                if callback_record.get('is_intra_process') == 0
            ]
        )

        if self._drop_inter_mediate_columns:
            timer_callback_records.drop_columns(
                ['is_intra_process'], inplace=True)
            subscription_intra_callback_records.drop_columns(
                ['is_intra_process'], inplace=True)
            subscription_inter_callback_records.drop_columns(
                ['is_intra_process'], inplace=True)

        self._timer_callback_records_cache = timer_callback_records
        self._intra_process_callback_records_cache = subscription_intra_callback_records
        self._inter_process_callback_records_cache = subscription_inter_callback_records

    def _update_intra_process_communication_cache(self) -> None:
        intra_process_callback_records = self._compose_intra_process_callback_records_with_cache()

        # inter_subscription = self._get_inter_subscription_records(
        #     self.inter_callback, drop_inter_mediate_columns)
        # アドレスでの紐付けなので、トピックなどを全てまぜた状態での算出が必要
        sink_records = self._data_util.data.dispatch_intra_process_subscription_callback_instances
        intra_publish_records = merge_sequencial_for_addr_track(
            source_records=self._data_util.data.rclcpp_intra_publish_instances,
            copy_records=self._data_util.data.message_construct_instances,
            sink_records=sink_records,
            source_stamp_key='rclcpp_intra_publish_timestamp',
            source_key='message',
            copy_stamp_key='message_construct_timestamp',
            copy_from_key='original_message',
            copy_to_key='constructed_message',
            sink_stamp_key='dispatch_intra_process_subscription_callback_timestamp',
            sink_from_key='message',
            progress_label='bindig: rclcpp_intra_publish and message_address',
        )

        intra_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=intra_process_callback_records,
            left_stamp_key='dispatch_intra_process_subscription_callback_timestamp',
            right_stamp_key='callback_start_timestamp',
            join_key='callback_object',
            how='left',
            progress_label='bindig: dispatch_subscription_callback and callback_object',
        )

        if self._drop_inter_mediate_columns:
            intra_records.drop_columns(
                [
                    'dispatch_intra_process_subscription_callback_timestamp',
                    'message',
                    'callback_end_timestamp',
                ],
                inplace=True,
            )

        self._intra_process_communication_records_cache = intra_records

    def _update_inter_process_communication_cache(self) -> None:
        inter_process_callback: RecordsInterface = (
            self._compose_inter_process_callback_records_with_cache()
        )

        # アドレスでの紐付けなので、トピックなどを全てまぜた状態での算出が必要
        dds_write = merge_sequencial_for_addr_track(
            source_records=self._data_util.data.dds_write_instances,
            copy_records=self._data_util.data.dds_bind_addr_to_addr,
            sink_records=self._data_util.data.dds_bind_addr_to_stamp,
            source_stamp_key='dds_write_timestamp',
            source_key='message',
            copy_stamp_key='dds_bind_addr_to_addr_timestamp',
            copy_from_key='addr_from',
            copy_to_key='addr_to',
            sink_stamp_key='dds_bind_addr_to_stamp_timestamp',
            sink_from_key='addr',
            progress_label='binding: message_address and source_timestamp',
        )

        publish = merge_sequencial(
            left_records=self._data_util.data.rclcpp_publish_instances,
            right_records=self._data_util.data.rcl_publish_instances,
            left_stamp_key='rclcpp_publish_timestamp',
            right_stamp_key='rcl_publish_timestamp',
            join_key='message',
            how='left',
            progress_label='binding: rclcpp_publish and rcl_publish',
        )

        publish = merge_sequencial(
            left_records=publish,
            right_records=dds_write,
            left_stamp_key='rcl_publish_timestamp',
            right_stamp_key='dds_write_timestamp',
            join_key='message',
            how='left',
            progress_label='binding: rcl_publish and dds_write',
        )

        subscription = self._data_util.data.dispatch_subscription_callback_instances
        inter_process_callback_dropped = inter_process_callback.drop_columns(
            ['callback_end_timestamp']
        )
        assert inter_process_callback_dropped is not None

        subscription = merge_sequencial(
            left_records=subscription,
            right_records=inter_process_callback_dropped,
            left_stamp_key='dispatch_subscription_callback_timestamp',
            right_stamp_key='callback_start_timestamp',
            join_key='callback_object',
            how='left',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )

        communication = merge(
            publish,
            self._data_util.data.on_data_available_instances,
            join_key='source_timestamp',
            how='left',
            progress_label='binding: source_timestamp and on_data_available',
        )

        communication = merge(
            communication,
            subscription,
            join_key='source_timestamp',
            how='left',
            progress_label='binding: source_timestamp and callback_start',
        )

        if self._drop_inter_mediate_columns:
            communication.drop_columns(
                [
                    'addr',
                    'message',
                    'source_timestamp',
                    'dds_bind_addr_to_stamp_timestamp',
                    'take_type_erased_timestamp',
                    'dispatch_subscription_callback_timestamp',
                ],
                inplace=True,
            )

        self._inter_process_communication_records_cache = communication
