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

# from copy import deepcopy
# from typing import Any, Optional

# from caret_analyze.record.record import Record
# from caret_analyze.record.record import Records
# from caret_analyze.infra.lttng.lttng import Lttng
# from caret_analyze.infra.lttng.records_composer_lttng import Lttng
# from caret_analyze.value_objects.callback_info import SubscriptionCallbackStructInfo,
#  TimerCallbackStructInfo
# from caret_analyze.value_objects.publisher_info import PublisherInfo
# from caret_analyze.infra.lttng.column_names import COLUMN_NAME

# import pandas as pd
# import pytest
# from pytest_mock import MockerFixture


# class TestLttngRecord:
#     def test_compose_timer_callback_records(self, mocker: MockerFixture):
#         lttng = mocker.Mock(spec=Lttng)
#         records_lttng = RecordsLttng(lttng)
#         node_name = '/node'
#         callback_name = 'timer_callback_0'
#         symbol = 'symbol'
#         period_ns = 100
#         callback_object = 5

#         timer_cb_info = TimerCallbackStructInfo(
#                 node_name, callback_name, symbol, period_ns, runtime_id=callback_object
#             )
#         mocker.patch.object(lttng, 'get_timer_callbacks_info', return_value=[timer_cb_info])

#         records_cb = Records(
#             [
#                 Record({
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 0,
#                     COLUMN_NAME.CALLBACK_END_TIMESTAMP: 2,
#                     COLUMN_NAME.CALLBACK_OBJECT: callback_object})
#             ]
#         )
#         mocker.patch.object(lttng, 'compose_callback_records', return_value=records_cb)

#         callback_info = TimerCallbackStructInfo(
#             node_name, callback_name, symbol, period_ns
#         )
#         records = records_lttng.compose_callback_records(callback_info)
#         assert len(records.data) == 1
#         datum = records.data[0]
#         columns_expect = {COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#                                  COLUMN_NAME.CALLBACK_END_TIMESTAMP,
#                                  COLUMN_NAME.CALLBACK_OBJECT}
#         assert records.columns == columns_expect
#         assert datum.columns == columns_expect
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 0
#         assert datum.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 2
#         assert datum.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object

#         callback_info = TimerCallbackStructInfo(
#             node_name, 'no_exist_callback', symbol, period_ns
#         )
#         records = records_lttng.compose_callback_records(callback_info)
#         assert len(records.data) == 0
#         assert records.columns == columns_expect

#     def test_compose_subscription_callback_records(self, mocker: MockerFixture):
#         lttng = mocker.Mock(spec=Lttng)
#         records_lttng = RecordsLttng(lttng)
#         node_name = '/node'
#         callback_name = 'timer_callback_0'
#         symbol = 'symbol'
#         callback_object = 5
#         callback_object_intra = 7

#         sub_cb_info = SubscriptionCallbackStructInfo(
#                 node_name, callback_name, symbol, '/topic_name',
#                 runtime_id=callback_object, runtime_id_intra=callback_object_intra
#             )
#         mocker.patch.object(lttng, 'get_subscription_callbacks_info', return_value=[sub_cb_info])

#         records_cb = Records(

#             [
#                 Record({
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 0,
#                     COLUMN_NAME.CALLBACK_END_TIMESTAMP: 2,
#                     COLUMN_NAME.CALLBACK_OBJECT: callback_object}),
#                 Record({
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 4,
#                     COLUMN_NAME.CALLBACK_END_TIMESTAMP: 5,
#                     COLUMN_NAME.CALLBACK_OBJECT: callback_object_intra})
#             ]
#         )
#         mocker.patch.object(lttng, 'compose_callback_records', return_value=records_cb)

#         sub_callback_info = SubscriptionCallbackStructInfo(
#             node_name, callback_name, symbol, '/topic_name'
#         )
#         records = records_lttng.compose_callback_records(sub_callback_info)
#         column_expect = {COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#                          COLUMN_NAME.CALLBACK_END_TIMESTAMP,
#                          COLUMN_NAME.CALLBACK_OBJECT}

#         assert records.columns == column_expect
#         assert len(records.data) == 2
#         datum = records.data[0]
#         assert datum.columns == column_expect
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 0
#         assert datum.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 2
#         assert datum.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object

#         sub_callback_info = SubscriptionCallbackStructInfo(
#             node_name, callback_name, symbol, '/topic_name'
#         )
#         records = records_lttng.compose_callback_records(sub_callback_info)
#         assert len(records.data) == 2
#         datum = records.data[0]
#         assert datum.columns == column_expect
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 0
#         assert datum.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 2
#         assert datum.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object

#         datum = records.data[1]
#         assert datum.columns == column_expect
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 4
#         assert datum.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 5
#         assert datum.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object_intra

#         sub_callback_info = SubscriptionCallbackStructInfo(
#             node_name, 'no_exist_callback', symbol, '/topic_name'
#         )
#         records = records_lttng.compose_callback_records(sub_callback_info)
#         assert records.columns == column_expect
#         assert len(records.data) == 0

#     def test_compose_inter_process_communication_records(self, mocker: MockerFixture):
#         lttng = mocker.Mock(spec=Lttng)
#         records_lttng = RecordsLttng(lttng)

#         subscription_cadllback_object = 8
#         publisher_handle = 9
#         period_ns = 88
#         pub_callback_object = 880
#         sub_callback_object = 890
#         depth = 2

#         sub_callback = SubscriptionCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', '/topic_name', depth=depth,
#             publishers=[],
#             runtime_id=sub_callback_object
#         )

#         publisher = PublisherInfo(
#             '/node',
#             '/topic_name',
#             'timer_callback_0',
#             runtime_id=publisher_handle,
#         )
#         pub_callback = TimerCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1',
#             publishers=[publisher],
#             period_ns=period_ns, runtime_id=pub_callback_object
#         )
#         records_cb = Records(
#             [
#                 Record({
#                     COLUMN_NAME.CALLBACK_OBJECT: subscription_cadllback_object,
#                     COLUMN_NAME.PUBLISHER_HANDLE: publisher_handle,
#                     COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP: 1,
#                     COLUMN_NAME.RCL_PUBLISH_TIMESTAMP: 2,
#                     COLUMN_NAME.DDS_WRITE_TIMESTAMP: 3,
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 4,
#                 })
#             ]
#         )

#         mocker.patch.object(lttng, 'get_subscription_callbacks_info',
# return_value=[sub_callback])
#         mocker.patch.object(lttng, 'get_timer_callbacks_info', return_value=[pub_callback])
#         mocker.patch.object(lttng, 'get_publishers_info', return_value=[publisher])
#         mocker.patch.object(
#             lttng, 'compose_inter_process_communication_records', return_value=records_cb)

#         sub_callback = SubscriptionCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', '/topic_name'
#         )
#         pub_callback = TimerCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', period_ns
#         )
#         records = records_lttng.compose_inter_process_communication_records(
#             pub_callback, sub_callback
#         )

#         assert len(records.data) == 1
#         columns_expect = {
#             COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
#             COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
#             COLUMN_NAME.DDS_WRITE_TIMESTAMP,
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#         }
#         assert records.columns == columns_expect
#         datum = records.data[0]
#         assert datum.columns == columns_expect
#         assert datum.get(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP) == 1
#         assert datum.get(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP) == 2
#         assert datum.get(COLUMN_NAME.DDS_WRITE_TIMESTAMP) == 3
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 4

#     def test_compose_intra_process_communication_records(self, mocker: MockerFixture):
#         lttng = mocker.Mock(spec=Lttng)
#         records_lttng = RecordsLttng(lttng)

#         subscription_cadllback_object = 8
#         publisher_handle = 9
#         period_ns = 88
#         pub_callback_object = 880
#         sub_callback_object = 890
#         sub_callback_object_intra = 891
#         depth = 2

#         sub_callback = SubscriptionCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', '/topic_name', depth=depth,
#             publishers=[],
#             runtime_id=sub_callback_object, runtime_id_intra=sub_callback_object_intra
#         )

#         publisher = PublisherInfo(
#             '/node',
#             '/topic_name',
#             'timer_callback_0',
#             runtime_id=publisher_handle,
#         )
#         pub_callback = TimerCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1',
#             publishers=[publisher],
#             period_ns=period_ns, runtime_id=pub_callback_object
#         )
#         records_cb = Records(
#             [
#                 Record({
#                     COLUMN_NAME.CALLBACK_OBJECT: subscription_cadllback_object,
#                     COLUMN_NAME.PUBLISHER_HANDLE: publisher_handle,
#                     COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP: 1,
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 2,
#                 })
#             ]
#         )

#         mocker.patch.object(lttng, 'get_subscription_callbacks_info',
#           return_value=[sub_callback])
#         mocker.patch.object(lttng, 'get_timer_callbacks_info', return_value=[pub_callback])
#         mocker.patch.object(lttng, 'get_publishers_info', return_value=[publisher])
#         mocker.patch.object(
#             lttng, 'compose_intra_process_communication_records', return_value=records_cb)

#         sub_callback = SubscriptionCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', '/topic_name'
#         )
#         pub_callback = TimerCallbackStructInfo(
#             '/node', 'subscription_callback_0', 'symbol1', period_ns
#         )
#         records = records_lttng.compose_intra_process_communication_records(
#             pub_callback, sub_callback
#         )

#         assert len(records.data) == 1
#         columns_expect = {
#             COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#         }
#         assert records.columns == columns_expect
#         datum = records.data[0]
#         assert datum.columns == columns_expect
#         assert datum.get(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP) == 1
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 2

#     def test_compose_variable_passing_records(self, mocker: MockerFixture):
#         lttng = mocker.Mock(spec=Lttng)
#         records_lttng = RecordsLttng(lttng)

#         period_ns = [0, 1]
#         callback_object = [3, 6]
#         callback_object = [7, 9]

#         write_callback = TimerCallbackStructInfo(
#             '/node1', 'timer_callback_0', 'symbol1',
#             publishers=[],
#             period_ns=period_ns[0], runtime_id=callback_object[0]
#         )
#         read_callback = TimerCallbackStructInfo(
#             '/node2', 'timer_callback_1', 'symbol2',
#             publishers=[],
#             period_ns=period_ns[1], runtime_id=callback_object[1]
#         )

#         records_cb = Records(
#             [
#                 Record({
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 4,
#                     COLUMN_NAME.CALLBACK_END_TIMESTAMP: 5,
#                     COLUMN_NAME.CALLBACK_OBJECT: callback_object[0]}),
#                 Record({
#                     COLUMN_NAME.CALLBACK_START_TIMESTAMP: 6,
#                     COLUMN_NAME.CALLBACK_END_TIMESTAMP: 7,
#                     COLUMN_NAME.CALLBACK_OBJECT: callback_object[1]}),
#             ]
#         )

#         mocker.patch.object(
#             lttng, 'get_timer_callbacks_info', return_value=[write_callback, read_callback])
#         mocker.patch.object(
#             lttng, 'compose_callback_records', return_value=records_cb)

#         write_callback = TimerCallbackStructInfo(
#             '/node1', 'timer_callback_0', 'symbol1', period_ns[0]
#         )
#         read_callback = TimerCallbackStructInfo(
#             '/node2', 'timer_callback_1', 'symbol2', period_ns[1]
#         )

#         records = records_lttng.compose_variable_passing_records(
#             write_callback, read_callback
#         )

#         assert len(records.data) == 1
#         columns_expect = {
#             COLUMN_NAME.CALLBACK_START_TIMESTAMP,
#             COLUMN_NAME.CALLBACK_END_TIMESTAMP,
#         }
#         assert records.columns == columns_expect
#         datum = records.data[0]
#         assert datum.columns == columns_expect
#         assert datum.get(COLUMN_NAME.CALLBACK_END_TIMESTAMP) == 5
#         assert datum.get(COLUMN_NAME.CALLBACK_START_TIMESTAMP) == 6
