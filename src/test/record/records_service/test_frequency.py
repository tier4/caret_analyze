# Copyright 2021 TIER IV, Inc.
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

from caret_analyze.common import ClockConverter
from caret_analyze.record import ColumnValue, Frequency, RecordInterface
from caret_analyze.record.record_factory import RecordsFactory

import pytest


def create_records(records_raw, columns):
    records = RecordsFactory.create_instance()
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        records.append(record_raw)
    return records


def to_dict(records):
    return [record.data for record in records]


class Converter:

    def __init__(self):
        self._system_times = [0, 1, 2, 3, 4]
        self._sim_times = [20, 21, 22, 23, 24]
        self._converter = ClockConverter.create_from_series(
            self._system_times, self._sim_times)

    def convert(self, v):
        return self._converter.convert(v)

    def round_convert(self, v):
        return round(self._converter.convert(v))

    def get_converter(self):
        return self._converter


@pytest.fixture(scope='module')
def create_converter():
    return Converter()


class TestFrequencyRecords:

    def test_create_frequency(self):
        records_raw = [
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)
        frequency = Frequency(records)
        frequency_with_target_column = Frequency(records, target_column='timestamp')
        frequency_with_row_filter = Frequency(records, row_filter=lambda: True)
        assert to_dict(frequency.to_records()) \
            == to_dict(frequency_with_target_column.to_records())
        assert to_dict(frequency.to_records()) == to_dict(frequency_with_row_filter.to_records())

    def test_empty_case(self, create_converter):
        records_raw = [
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

        result = to_dict(frequency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_one_column_default_case(self, create_converter):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 11},
            {'timestamp': 12},
            {'timestamp': 14},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 0, 'frequency': 2},
            {'timestamp': 10, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(0), 'frequency': 2},
            {'timestamp': create_converter.round_convert(10), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_interval_ns_case(self, create_converter):
        records_raw = [
            {'timestamp': 1000000000},
            {'timestamp': 1000500000},
            {'timestamp': 2000000000},
            {'timestamp': 2000300000},
            {'timestamp': 3000000000},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 1000000000, 'frequency': 4},
            {'timestamp': 3000000000, 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(interval_ns=2000000000))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(
                1000000000), 'frequency': 4},
            {'timestamp': create_converter.round_convert(
                3000000000), 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=2000000000, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_base_timestamp_case(self, create_converter):
        records_raw = [
            {'timestamp': 5},
            {'timestamp': 6},
            {'timestamp': 11},
            {'timestamp': 12},
            {'timestamp': 15},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 4, 'frequency': 4},
            {'timestamp': 14, 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(interval_ns=10,
                                              base_timestamp=4))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(4), 'frequency': 4},
            {'timestamp': create_converter.round_convert(14), 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(interval_ns=10,
                                              base_timestamp=4,
                                              converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_until_timestamp_case(self, create_converter):
        records_raw = [
            {'timestamp': 5},
            {'timestamp': 6},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 5, 'frequency': 2},
            {'timestamp': 15, 'frequency': 0},
        ]
        result = to_dict(frequency.to_records(interval_ns=10, until_timestamp=20))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(5), 'frequency': 2},
            {'timestamp': create_converter.round_convert(15), 'frequency': 0},
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, until_timestamp=20, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3, 'timestamp2': 4},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14},
            {'timestamp1': 15, 'timestamp2': 16},
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp1': 0, 'frequency': 2},
            {'timestamp1': 10, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp1': create_converter.round_convert(0), 'frequency': 2},
            {'timestamp1': create_converter.round_convert(10), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_target_column_case(self, create_converter):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3, 'timestamp2': 4},
            {'timestamp1': 5, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14},
            {'timestamp1': 15, 'timestamp2': 16},
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records, target_column='timestamp2')

        expect_raw = [
            {'timestamp2': 2, 'frequency': 2},
            {'timestamp2': 12, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp2': create_converter.round_convert(2), 'frequency': 2},
            {'timestamp2': create_converter.round_convert(12), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_invalid_target_column_case(self):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 11},
            {'timestamp': 12},
            {'timestamp': 14},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records, 'not_exist')

        expect_raw = []
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

    def test_drop_case(self, create_converter):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp2': 4},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14},
            {'timestamp1': 15, 'timestamp2': 16},
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp1': 0, 'frequency': 1},
            {'timestamp1': 10, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp1': create_converter.round_convert(0), 'frequency': 1},
            {'timestamp1': create_converter.round_convert(10), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_one_interval_contains_all_timestamps_case(self, create_converter):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 2},
            {'timestamp': 3},
            {'timestamp': 4},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 0, 'frequency': 5}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(0), 'frequency': 5}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_exist_zero_frequency_case(self, create_converter):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 21},
            {'timestamp': 22},
            {'timestamp': 24},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 0, 'frequency': 2},
            {'timestamp': 10, 'frequency': 0},
            {'timestamp': 20, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(0), 'frequency': 2},
            {'timestamp': create_converter.round_convert(10), 'frequency': 0},
            {'timestamp': create_converter.round_convert(20), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_base_ts_greater_than_min_ts_case(self, create_converter):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 11},
            {'timestamp': 12},
            {'timestamp': 24},
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 10, 'frequency': 2},
            {'timestamp': 20, 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, base_timestamp=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp': create_converter.round_convert(10), 'frequency': 2},
            {'timestamp': create_converter.round_convert(20), 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, base_timestamp=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_drop_non_target_column_case(self, create_converter):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14},
            {'timestamp1': 15, 'timestamp2': 16},
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        frequency = Frequency(records, target_column='timestamp1')

        expect_raw = [
            {'timestamp1': 0, 'frequency': 2},
            {'timestamp1': 10, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp1': create_converter.round_convert(0), 'frequency': 2},
            {'timestamp1': create_converter.round_convert(10), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_apply_row_filter_case(self, create_converter):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14},
            {'timestamp1': 15, 'timestamp2': 16},
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        def row_filter(record: RecordInterface) -> bool:
            start_column = 'timestamp1'
            end_column = 'timestamp2'
            if (record.data.get(start_column) is not None
                    and record.data.get(end_column) is not None):
                return True
            else:
                return False

        frequency = Frequency(
            records, target_column='timestamp1', row_filter=row_filter)

        expect_raw = [
            {'timestamp1': 0, 'frequency': 1},
            {'timestamp1': 10, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(interval_ns=10))
        assert result == expect_raw

        expect_raw = [
            {'timestamp1': create_converter.round_convert(0), 'frequency': 1},
            {'timestamp1': create_converter.round_convert(10), 'frequency': 3}
        ]
        result = to_dict(frequency.to_records(
            interval_ns=10, converter=create_converter.get_converter()))
        assert result == expect_raw
