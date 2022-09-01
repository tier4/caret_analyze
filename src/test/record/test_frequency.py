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
from caret_analyze.record.frequency import Frequency
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


class TestFrequencyRecords:

    def test_callback_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_frequency = Frequency(records, target='callback')

        expect_raw = [
        ]
        result = to_dict(callback_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_callback_normal_case(self):
        records_raw = [
            {'/node/callback/callback_start_timestamp': 1000000000,
             '/node/callback/callback_end_timestamp': 1000001000},
            {'/node/callback/callback_start_timestamp': 1000500000,
             '/node/callback/callback_end_timestamp': 1000501000},
            {'/node/callback/callback_start_timestamp': 2000000000,
             '/node/callback/callback_end_timestamp': 2000001000},
            {'/node/callback/callback_start_timestamp': 2000300000,
             '/node/callback/callback_end_timestamp': 2000301000},
            {'/node/callback/callback_start_timestamp': 2000500000,
             '/node/callback/callback_end_timestamp': 2000501000},
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_frequency = Frequency(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 1000000000, 'frequency': 2},
            {'callback_start_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(callback_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_callback_drop_case(self):
        records_raw = [
            {'/node/callback/callback_end_timestamp': 1000001000},
            {'/node/callback/callback_start_timestamp': 1000500000,
             '/node/callback/callback_end_timestamp': 1000501000},
            {'/node/callback/callback_start_timestamp': 2000000000,
             '/node/callback/callback_end_timestamp': 2000001000},
            {'/node/callback/callback_start_timestamp': 2000300000,
             '/node/callback/callback_end_timestamp': 2000301000},
            {'/node/callback/callback_start_timestamp': 2000500000,
             '/node/callback/callback_end_timestamp': 2000501000},
        ]
        columns = [ColumnValue('/node/callback/callback_start_timestamp'),
                   ColumnValue('/node/callback/callback_end_timestamp')]
        records = create_records(records_raw, columns)

        callback_frequency = Frequency(records, target='callback')

        expect_raw = [
            {'callback_start_timestamp': 1000000000, 'frequency': 1},
            {'callback_start_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(callback_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_communication_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_frequency = Frequency(records, target='communication')

        expect_raw = [
        ]
        result = to_dict(communication_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_communication_normal_case(self):
        records_raw = [
            {'/topic/rclcpp_publish_timestamp': 1000000000,
             '/topic/rcl_publish_timestamp': 1000001000,
             '/topic/dds_write_timestamp': 1000002000,
             '/sub_node/callback/callback_start_timestamp': 1000090000},
            {'/topic/rclcpp_publish_timestamp': 1000500000,
             '/topic/rcl_publish_timestamp': 1000501000,
             '/topic/dds_write_timestamp': 1000502000,
             '/sub_node/callback/callback_start_timestamp': 1000590000},
            {'/topic/rclcpp_publish_timestamp': 2000000000,
             '/topic/rcl_publish_timestamp': 2000001000,
             '/topic/dds_write_timestamp': 2000002000,
             '/sub_node/callback/callback_start_timestamp': 2000090000},
            {'/topic/rclcpp_publish_timestamp': 2000500000,
             '/topic/rcl_publish_timestamp': 2000501000,
             '/topic/dds_write_timestamp': 2000502000,
             '/sub_node/callback/callback_start_timestamp': 2000590000},
            {'/topic/rclcpp_publish_timestamp': 2000800000,
             '/topic/rcl_publish_timestamp': 2000801000,
             '/topic/dds_write_timestamp': 2000802000,
             '/sub_node/callback/callback_start_timestamp': 2000890000}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_frequency = Frequency(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 1000000000, 'frequency': 2},
            {'rclcpp_publish_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(communication_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_communication_drop_case(self):
        records_raw = [
            {'/topic/rcl_publish_timestamp': 1000001000,
             '/topic/dds_write_timestamp': 1000002000,
             '/sub_node/callback/callback_start_timestamp': 1000090000},
            {'/topic/rclcpp_publish_timestamp': 1000500000,
             '/topic/rcl_publish_timestamp': 1000501000,
             '/topic/dds_write_timestamp': 1000502000,
             '/sub_node/callback/callback_start_timestamp': 1000590000},
            {'/topic/rclcpp_publish_timestamp': 2000000000,
             '/topic/rcl_publish_timestamp': 2000001000,
             '/topic/dds_write_timestamp': 2000002000,
             '/sub_node/callback/callback_start_timestamp': 2000090000},
            {'/topic/rclcpp_publish_timestamp': 2000500000,
             '/topic/rcl_publish_timestamp': 2000501000,
             '/topic/dds_write_timestamp': 2000502000,
             '/sub_node/callback/callback_start_timestamp': 2000590000},
            {'/topic/rclcpp_publish_timestamp': 2000800000,
             '/topic/rcl_publish_timestamp': 2000801000,
             '/topic/dds_write_timestamp': 2000802000,
             '/sub_node/callback/callback_start_timestamp': 2000890000}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/sub_node/callback/callback_start_timestamp')]
        records = create_records(records_raw, columns)

        communication_frequency = Frequency(records, target='communication')

        expect_raw = [
            {'rclcpp_publish_timestamp': 1000000000, 'frequency': 1},
            {'rclcpp_publish_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(communication_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_subscription_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_frequency = Frequency(records, target='subscription')

        expect_raw = [
        ]
        result = to_dict(subscription_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_subscription_normal_case(self):
        records_raw = [
            {'/sub_node/callback/callback_start_timestamp': 1000000000,
             '/topic/message_timestamp': 1000001000,
             '/topic/source_timestamp': 1000002000},
            {'/sub_node/callback/callback_start_timestamp': 1000500000,
             '/topic/message_timestamp': 1000501000,
             '/topic/source_timestamp': 1000502000},
            {'/sub_node/callback/callback_start_timestamp': 2000000000,
             '/topic/message_timestamp': 2000001000,
             '/topic/source_timestamp': 2000002000},
            {'/sub_node/callback/callback_start_timestamp': 2000500000,
             '/topic/message_timestamp': 2000501000,
             '/topic/source_timestamp': 2000502000},
            {'/sub_node/callback/callback_start_timestamp': 2000800000,
             '/topic/message_timestamp': 2000801000,
             '/topic/source_timestamp': 2000802000}
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_frequency = Frequency(records, target='subscription')

        expect_raw = [
            {'/sub_node/callback/callback_start_timestamp': 1000000000,
             'frequency': 2},
            {'/sub_node/callback/callback_start_timestamp': 2000000000,
             'frequency': 3}
        ]
        result = to_dict(subscription_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_subscription_drop_case(self):
        records_raw = [
            {'/topic/message_timestamp': 1000001000,
             '/topic/source_timestamp': 1000002000},
            {'/sub_node/callback/callback_start_timestamp': 1000500000,
             '/topic/message_timestamp': 1000501000,
             '/topic/source_timestamp': 1000502000},
            {'/sub_node/callback/callback_start_timestamp': 2000000000,
             '/topic/message_timestamp': 2000001000,
             '/topic/source_timestamp': 2000002000},
            {'/sub_node/callback/callback_start_timestamp': 2000500000,
             '/topic/message_timestamp': 2000501000,
             '/topic/source_timestamp': 2000502000},
            {'/sub_node/callback/callback_start_timestamp': 2000800000,
             '/topic/message_timestamp': 2000801000,
             '/topic/source_timestamp': 2000802000}
        ]
        columns = [ColumnValue('/sub_node/callback/callback_start_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        subscription_frequency = Frequency(records, target='subscription')

        expect_raw = [
            {'/sub_node/callback/callback_start_timestamp': 1000000000,
             'frequency': 1},
            {'/sub_node/callback/callback_start_timestamp': 2000000000,
             'frequency': 3}
        ]
        result = to_dict(subscription_frequency.to_records(
            initial_timestamp=1000000000))
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

        publisher_frequency = Frequency(records, target='publisher')

        expect_raw = [
        ]
        result = to_dict(publisher_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_publisher_normal_case(self):
        records_raw = [
            {'/topic/rclcpp_publish_timestamp': 1000000000,
             '/topic/rcl_publish_timestamp': 1000001000,
             '/topic/dds_write_timestamp': 1000002000,
             '/topic/message_timestamp': 1000003000,
             '/topic/source_timestamp': 1000004000},
            {'/topic/rclcpp_publish_timestamp': 1000500000,
             '/topic/rcl_publish_timestamp': 1000501000,
             '/topic/dds_write_timestamp': 1000502000,
             '/topic/message_timestamp': 1000503000,
             '/topic/source_timestamp': 1000504000},
            {'/topic/rclcpp_publish_timestamp': 2000000000,
             '/topic/rcl_publish_timestamp': 2000001000,
             '/topic/dds_write_timestamp': 2000002000,
             '/topic/message_timestamp': 2000003000,
             '/topic/source_timestamp': 2000004000},
            {'/topic/rclcpp_publish_timestamp': 2000500000,
             '/topic/rcl_publish_timestamp': 2000501000,
             '/topic/dds_write_timestamp': 2000502000,
             '/topic/message_timestamp': 2000503000,
             '/topic/source_timestamp': 2000504000},
            {'/topic/rclcpp_publish_timestamp': 2000800000,
             '/topic/rcl_publish_timestamp': 2000801000,
             '/topic/dds_write_timestamp': 2000802000,
             '/topic/message_timestamp': 2000803000,
             '/topic/source_timestamp': 2000804000}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        publisher_frequency = Frequency(records, target='publisher')

        expect_raw = [
            {'/topic/rclcpp_publish_timestamp': 1000000000, 'frequency': 2},
            {'/topic/rclcpp_publish_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(publisher_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw

    def test_publisher_drop_case(self):
        records_raw = [
            {'/topic/rcl_publish_timestamp': 1000001000,
             '/topic/dds_write_timestamp': 1000002000,
             '/topic/message_timestamp': 1000003000,
             '/topic/source_timestamp': 1000004000},
            {'/topic/rclcpp_publish_timestamp': 1000500000,
             '/topic/rcl_publish_timestamp': 1000501000,
             '/topic/dds_write_timestamp': 1000502000,
             '/topic/message_timestamp': 1000503000,
             '/topic/source_timestamp': 1000504000},
            {'/topic/rclcpp_publish_timestamp': 2000000000,
             '/topic/rcl_publish_timestamp': 2000001000,
             '/topic/dds_write_timestamp': 2000002000,
             '/topic/message_timestamp': 2000003000,
             '/topic/source_timestamp': 2000004000},
            {'/topic/rclcpp_publish_timestamp': 2000500000,
             '/topic/rcl_publish_timestamp': 2000501000,
             '/topic/dds_write_timestamp': 2000502000,
             '/topic/message_timestamp': 2000503000,
             '/topic/source_timestamp': 2000504000},
            {'/topic/rclcpp_publish_timestamp': 2000800000,
             '/topic/rcl_publish_timestamp': 2000801000,
             '/topic/dds_write_timestamp': 2000802000,
             '/topic/message_timestamp': 2000803000,
             '/topic/source_timestamp': 2000804000}
        ]
        columns = [ColumnValue('/topic/rclcpp_publish_timestamp'),
                   ColumnValue('/topic/rcl_publish_timestamp'),
                   ColumnValue('/topic/dds_write_timestamp'),
                   ColumnValue('/topic/message_timestamp'),
                   ColumnValue('/topic/source_timestamp')]
        records = create_records(records_raw, columns)

        publisher_frequency = Frequency(records, target='publisher')

        expect_raw = [
            {'/topic/rclcpp_publish_timestamp': 1000000000, 'frequency': 1},
            {'/topic/rclcpp_publish_timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(publisher_frequency.to_records(
            initial_timestamp=1000000000))
        assert result == expect_raw
