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


# from caret_analyze.record.record import RecordsInterface
# from caret_analyze.infra.lttng.lttng_info import LttngInfo
# from caret_analyze.infra.lttng.records_source import RecordsSource
# from caret_analyze.value_objects import TimerCallbackStructValue
# from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
# from caret_analyze.infra.lttng.column_names import COLUMN_NAME


# class TestLttngRecordsSource:

#     def test_compose_callback_records(self):
#         period_ns = 1
#         callback_object = 2

#         data = Ros2DataModel()
#         data.add_callback_start_instance(1, callback_object, False)
#         data.add_callback_end_instance(2, callback_object)
#         data.finalize()

#         source = RecordsSource(data)
#         info = LttngInfo(data, source)

#         callback = TimerCallbackStructValue(
#             '/node',
#             'timer_callback_0',
#             'symbol',
#             period_ns,
#             runtime_id=callback_object,
#         )

#         records = source.compose_callback_records()

#         assert isinstance(records, RecordsInterface)
#         assert len(records.data) == 1
#         columns_expect = {
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#             COLUMN_NAME.CALLBACK_END_TIMESTAMP,
#             COLUMN_NAME.CALLBACK_OBJECT,
#         }
#         assert records.columns == columns_expect
#         record = records.data[0]

#         assert record.columns == columns_expect
#         assert record.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 1
#         assert record.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 2
#         assert record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object

#     def test_compose_intra_process_communication_records(self):
#         callback_object = 2
#         message = 0
#         message_copy = 2
#         publisher_handle = 3

#         data = Ros2DataModel()
#         data.add_rclcpp_intra_publish_instance(
#             1, publisher_handle, message
#         )
#         data.add_message_construct_instance(
#             2, message, message_copy
#         )
#         data.add_dispatch_intra_process_subscription_callback_instance(
#             3, callback_object, message
#         )
#         data.add_callback_start_instance(
#             4, callback_object, True
#         )
#         data.finalize()

#         source = RecordsSourse(data)

#         records = source.compose_intra_process_communication_records()

#         assert isinstance(records, RecordsInterface)
#         assert len(records.data) == 1

#         columns_expect = {
#             COLUMN_NAME.CALLBACK_OBJECT,
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#             COLUMN_NAME.PUBLISHER_HANDLE,
#             COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP
#         }
#         assert records.columns == columns_expect
#         record = records.data[0]

#         assert record.columns == columns_expect
#         assert record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object
#         assert record.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 4
#         assert record.get(COLUMN_NAME.PUBLISHER_HANDLE) == publisher_handle
#         assert record.get(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP) == 1

#     def test_compose_inter_process_communication_records(self):
#         callback_object = 2
#         message = 0
#         message_copy = 2
#         message_sub = 3
#         publisher_handle = 3
#         source_timestamp = 4

#         data = Ros2DataModel()
#         data.add_rclcpp_publish_instance(
#             0, publisher_handle, message
#         )
#         data.add_rcl_publish_instance(
#             1, publisher_handle, message
#         )
#         data.add_dds_write_instance(
#             2, message
#         )
#         data.add_dds_bind_addr_to_addr(
#             3, message, message_copy
#         )
#         data.add_dds_bind_addr_to_stamp(
#             4, message_copy, source_timestamp
#         )

#         data.add_dispatch_subscription_callback_instance(
#             5, callback_object, message_sub, source_timestamp
#         )
#         data.add_callback_start_instance(
#             6, callback_object, False
#         )
#         data.finalize()

#         source = RecordsSourse(data)

#         records = source.compose_inter_process_communication_records()

#         assert isinstance(records, RecordsInterface)
#         assert len(records.data) == 1

#         columns_expect = {
#             COLUMN_NAME.CALLBACK_OBJECT,
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#             COLUMN_NAME.PUBLISHER_HANDLE,
#             COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
#             COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
#             COLUMN_NAME.DDS_WRITE_TIMESTAMP,
#         }

#         assert records.columns == columns_expect
#         record = records.data[0]

#         assert record.columns == columns_expect
#         assert record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object
#         assert record.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 6
#         assert record.get(COLUMN_NAME.PUBLISHER_HANDLE) == publisher_handle
#         assert record.get(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP) == 0
#         assert record.get(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP) == 1
#         assert record.get(COLUMN_NAME.DDS_WRITE_TIMESTAMP) == 2
