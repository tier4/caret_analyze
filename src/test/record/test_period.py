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
from caret_analyze.record.period import Period
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


class TestPeriodRecords:

    def test_callback_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_period = Period(records, target='callback')

        expect_raw = [
        ]
        result = to_dict(callback_period.to_records())
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

        callback_period = Period(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 0, 'period': 2},
            {'callback_start_timestamp': 2, 'period': 1}
        ]
        result = to_dict(callback_period.to_records())
        assert result == expect_raw

    def test_callback_drop_case(self):
        records_raw = [
            {'/node/callback/callback_end_timestamp': 2},
            {'/node/callback/callback_start_timestamp': 2,
             '/node/callback/callback_end_timestamp': 3},
            {'/node/callback/callback_start_timestamp': 3,
             '/node/callback/callback_end_timestamp': 4},
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_period = Period(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 3, 'period': 1}
        ]
        result = to_dict(callback_period.to_records())
        assert result == expect_raw

    def test_communication_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_period = Period(records, target='communication')

        expect_raw = [
        ]
        result = to_dict(communication_period.to_records())
        assert result == expect_raw

    def test_communication_normal_case(self):
        records_raw = [
            {'/topic/rclcpp_publish_timestamp': 0,
             '/topic/rcl_publish_timestamp': 1,
             '/topic/dds_write_timestamp': 2,
             '/sub_node/callback/callback_start_timestamp': 3},
            {'/topic/rclcpp_publish_timestamp': 4,
             '/topic/rcl_publish_timestamp': 5,
             '/topic/dds_write_timestamp': 6,
             '/sub_node/callback/callback_start_timestamp': 7},
            {'/topic/rclcpp_publish_timestamp': 8,
             '/topic/rcl_publish_timestamp': 9,
             '/topic/dds_write_timestamp': 10,
             '/sub_node/callback/callback_start_timestamp': 12}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_period = Period(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 4, 'period': 4},
            {'rclcpp_publish_timestamp': 8, 'period': 4}
        ]
        result = to_dict(communication_period.to_records())
        assert result == expect_raw

    def test_communication_drop_case(self):
        records_raw = [
            {'/topic/rcl_publish_timestamp': 1,
             '/topic/dds_write_timestamp': 2,
             '/sub_node/callback/callback_start_timestamp': 3},
            {'/topic/rclcpp_publish_timestamp': 4,
             '/topic/rcl_publish_timestamp': 5,
             '/topic/dds_write_timestamp': 6,
             '/sub_node/callback/callback_start_timestamp': 7},
            {'/topic/rclcpp_publish_timestamp': 8,
             '/topic/rcl_publish_timestamp': 9,
             '/topic/dds_write_timestamp': 10,
             '/sub_node/callback/callback_start_timestamp': 12}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_period = Period(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 8, 'period': 4}
        ]
        result = to_dict(communication_period.to_records())
        assert result == expect_raw

    def test_subscription_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_period = Period(records, target='subscription')

        expect_raw = [
        ]
        result = to_dict(subscription_period.to_records())
        assert result == expect_raw

    def test_subscription_normal_case(self):
        records_raw = [
            {'/sub_node/callback/callback_start_timestamp': 0,
             '/topic/message_timestamp': 1,
             '/topic/source_timestamp': 2},
            {'/sub_node/callback/callback_start_timestamp': 4,
             '/topic/message_timestamp': 5,
             '/topic/source_timestamp': 6},
            {'/sub_node/callback/callback_start_timestamp': 8,
             '/topic/message_timestamp': 9,
             '/topic/source_timestamp': 10}
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_period = Period(records, target='subscription')

        expect_raw = [
            {'/sub_node/callback/callback_start_timestamp': 4, 'period': 4},
            {'/sub_node/callback/callback_start_timestamp': 8, 'period': 4}
        ]
        result = to_dict(subscription_period.to_records())
        assert result == expect_raw

    def test_subscription_drop_case(self):
        records_raw = [
            {'/topic/message_timestamp': 1,
             '/topic/source_timestamp': 2},
            {'/sub_node/callback/callback_start_timestamp': 4,
             '/topic/message_timestamp': 5,
             '/topic/source_timestamp': 6},
            {'/sub_node/callback/callback_start_timestamp': 8,
             '/topic/message_timestamp': 9,
             '/topic/source_timestamp': 10}
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_period = Period(records, target='subscription')

        expect_raw = [
            {'/sub_node/callback/callback_start_timestamp': 8, 'period': 4}
        ]
        result = to_dict(subscription_period.to_records())
        assert result == expect_raw

    def test_publisher_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        publisher_period = Period(records, target='publisher')

        expect_raw = [
        ]
        result = to_dict(publisher_period.to_records())
        assert result == expect_raw

    def test_publisher_normal_case(self):
        records_raw = [
            {'/topic/rclcpp_publish_timestamp': 0,
             '/topic/rcl_publish_timestamp': 1,
             '/topic/dds_write_timestamp': 2,
             '/topic/message_timestamp': 3,
             '/topic/source_timestamp': 4},
            {'/topic/rclcpp_publish_timestamp': 5,
             '/topic/rcl_publish_timestamp': 6,
             '/topic/dds_write_timestamp': 7,
             '/topic/message_timestamp': 8,
             '/topic/source_timestamp': 9},
            {'/topic/rclcpp_publish_timestamp': 11,
             '/topic/rcl_publish_timestamp': 12,
             '/topic/dds_write_timestamp': 13,
             '/topic/message_timestamp': 14,
             '/topic/source_timestamp': 16}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        publisher_period = Period(records, target='publisher')

        expect_raw = [
            {'/topic/rclcpp_publish_timestamp': 5, 'period': 5},
            {'/topic/rclcpp_publish_timestamp': 11, 'period': 6}
        ]
        result = to_dict(publisher_period.to_records())
        assert result == expect_raw

    def test_publisher_drop_case(self):
        records_raw = [
            {'/topic/rcl_publish_timestamp': 1,
             '/topic/dds_write_timestamp': 2,
             '/topic/message_timestamp': 3,
             '/topic/source_timestamp': 4},
            {'/topic/rclcpp_publish_timestamp': 5,
             '/topic/rcl_publish_timestamp': 6,
             '/topic/dds_write_timestamp': 7,
             '/topic/message_timestamp': 8,
             '/topic/source_timestamp': 9},
            {'/topic/rclcpp_publish_timestamp': 11,
             '/topic/rcl_publish_timestamp': 12,
             '/topic/dds_write_timestamp': 13,
             '/topic/message_timestamp': 14,
             '/topic/source_timestamp': 16}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        publisher_period = Period(records, target='publisher')

        expect_raw = [
            {'/topic/rclcpp_publish_timestamp': 11, 'period': 6}
        ]
        result = to_dict(publisher_period.to_records())
        assert result == expect_raw
