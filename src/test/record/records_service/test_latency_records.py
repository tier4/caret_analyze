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
from caret_analyze.record import ColumnValue
from caret_analyze.record import Latency
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


class TestLatencyRecords:

    def test_empty_case(self, create_converter):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        latency = Latency(records)

        expect_raw = [
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        latency = Latency(records)

        expect_raw = [
            {'start': 0, 'latency': 2},
            {'start': 3, 'latency': 1},
            {'start': 11, 'latency': 1}
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(0), 'latency': 2},
            {'start': create_converter.round_convert(3), 'latency': 1},
            {'start': create_converter.round_convert(11), 'latency': 1}
        ]
        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_three_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'ts': 1, 'end': 2},
            {'start': 3, 'ts': 4, 'end': 5},
            {'start': 11, 'ts': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('ts'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        latency = Latency(records)

        expect_raw = [
            {'start': 0, 'latency': 2},
            {'start': 3, 'latency': 2},
            {'start': 11, 'latency': 2}
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(0), 'latency': 2},
            {'start': create_converter.round_convert(3), 'latency': 2},
            {'start': create_converter.round_convert(11), 'latency': 2}
        ]
        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_specify_target_column_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 1, 'ts': 2},
            {'start': 3, 'end': 4, 'ts': 5},
            {'start': 11, 'end': 12, 'ts': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('end'), ColumnValue('ts')]
        records = create_records(records_raw, columns)

        latency = Latency(records, start_column='start', end_column='end')

        expect_raw = [
            {'start': 0, 'latency': 1},
            {'start': 3, 'latency': 1},
            {'start': 11, 'latency': 1}
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(0), 'latency': 1},
            {'start': create_converter.round_convert(3), 'latency': 1},
            {'start': create_converter.round_convert(11), 'latency': 1}
        ]
        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_drop_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        latency = Latency(records)

        expect_raw = [
            {'start': 0, 'latency': 2},
            {'start': 11, 'latency': 1}
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(0), 'latency': 2},
            {'start': create_converter.round_convert(11), 'latency': 1}
        ]
        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_colapsed_time_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 5, 'end': 4},
            {'start': 10, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        latency = Latency(records)

        expect_raw = [
            {'start': 0, 'latency': 2},
            {'start': 5, 'latency': 0},
            {'start': 10, 'latency': 2}
        ]
        result = to_dict(latency.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(0), 'latency': 2},
            {'start': create_converter.round_convert(5), 'latency': 0},
            {'start': create_converter.round_convert(10), 'latency': 2}
        ]
        result = to_dict(latency.to_records(converter=create_converter.get_converter()))
        assert result == expect_raw
