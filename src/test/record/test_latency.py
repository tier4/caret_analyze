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

from caret_analyze.record import ColumnValue
from caret_analyze.record.latency import Latency
from caret_analyze.record.record_factory import RecordFactory, RecordsFactory


def create_records(records_raw, columns):
    records = RecordsFactory.create_instance()
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        record = RecordFactory.create_instance(record_raw)
        records.append(record)
    return records


def to_dict(records):
    return [record.data for record in records]


class TestLatencyRecords:

    def test_callback_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_latency = Latency(records, target='callback')

        expect_raw = [
        ]
        result = to_dict(callback_latency.to_records())
        assert result == expect_raw

    def test_callback_normal_case(self):
        records_raw = [
            {'/node/callback/callback_start_timestamp': 0,
             '/node/callback/callback_end_timestamp': 2},
            {'/node/callback/callback_start_timestamp': 2,
             '/node/callback/callback_end_timestamp': 3},
            {'/node/callback/callback_start_timestamp': 3,
             '/node/callback/callback_end_timestamp': 4},
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_latency = Latency(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 0, 'latency': 2},
            {'callback_start_timestamp': 2, 'latency': 1},
            {'callback_start_timestamp': 3, 'latency': 1},
        ]
        result = to_dict(callback_latency.to_records())
        assert result == expect_raw

    def test_callback_drop_case(self):
        records_raw = [
            {'/node/callback/callback_start_timestamp': 0},
            {'/node/callback/callback_start_timestamp': 2,
             '/node/callback/callback_end_timestamp': 3},
            {'/node/callback/callback_start_timestamp': 3,
             '/node/callback/callback_end_timestamp': 4},
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_latency = Latency(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 2, 'latency': 1},
            {'callback_start_timestamp': 3, 'latency': 1}
        ]
        result = to_dict(callback_latency.to_records())
        assert result == expect_raw

    def test_communication_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/pub_node/rclcpp_publish_timestamp'),
                   ColumnValue('/pub_node/rcl_publish_timestamp'),
                   ColumnValue('/pub_node/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_latency = Latency(records, target='communication')

        expect_raw = [
        ]
        result = to_dict(communication_latency.to_records())
        assert result == expect_raw

    def test_communication_normal_case(self):
        records_raw = [
            {'/pub_node/rclcpp_publish_timestamp': 0,
             '/pub_node/rcl_publish_timestamp': 1,
             '/pub_node/dds_write_timestamp': 2,
             '/sub_node/callback/callback_start_timestamp': 3},
            {'/pub_node/rclcpp_publish_timestamp': 4,
             '/pub_node/rcl_publish_timestamp': 5,
             '/pub_node/dds_write_timestamp': 6,
             '/sub_node/callback/callback_start_timestamp': 7},
            {'/pub_node/rclcpp_publish_timestamp': 8,
             '/pub_node/rcl_publish_timestamp': 9,
             '/pub_node/dds_write_timestamp': 10,
             '/sub_node/callback/callback_start_timestamp': 12}
        ]
        columns = [ColumnValue('/pub_node/rclcpp_publish_timestamp'),
                   ColumnValue('/pub_node/rcl_publish_timestamp'),
                   ColumnValue('/pub_node/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_latency = Latency(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 0, 'latency': 3},
            {'rclcpp_publish_timestamp': 2, 'latency': 3},
            {'rclcpp_publish_timestamp': 3, 'latency': 4},
        ]
        result = to_dict(communication_latency.to_records())
        assert result == expect_raw

    def test_communication_drop_case(self):
        records_raw = [
            {'/pub_node/rclcpp_publish_timestamp': 0,
             '/pub_node/rcl_publish_timestamp': 1,
             '/pub_node/dds_write_timestamp': 2},
            {'/pub_node/rclcpp_publish_timestamp': 4,
             '/pub_node/rcl_publish_timestamp': 5,
             '/pub_node/dds_write_timestamp': 6,
             '/sub_node/callback/callback_start_timestamp': 7},
            {'/pub_node/rclcpp_publish_timestamp': 8,
             '/pub_node/rcl_publish_timestamp': 9,
             '/pub_node/dds_write_timestamp': 10,
             '/sub_node/callback/callback_start_timestamp': 12}
        ]
        columns = [ColumnValue('/pub_node/rclcpp_publish_timestamp'),
                   ColumnValue('/pub_node/rcl_publish_timestamp'),
                   ColumnValue('/pub_node/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_latency = Latency(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 2, 'latency': 3},
            {'rclcpp_publish_timestamp': 3, 'latency': 4},
        ]
        result = to_dict(communication_latency.to_records())
        assert result == expect_raw
