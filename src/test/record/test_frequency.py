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

    def test_empty_case(self):
        records_raw = [
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

    def test_one_column_default_case(self):
        records_raw = [
            {'timestamp': 1000000000},
            {'timestamp': 1000500000},
            {'timestamp': 2000000000},
            {'timestamp': 2000300000},
            {'timestamp': 2000500000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 1000000000, 'frequency': 2},
            {'timestamp': 2000000000, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

    def test_specify_interval_ns_case(self):
        records_raw = [
            {'timestamp': 1000000000},
            {'timestamp': 1000500000},
            {'timestamp': 2000000000},
            {'timestamp': 2000300000},
            {'timestamp': 3000000000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records, interval_ns=2000000000)

        expect_raw = [
            {'timestamp': 1000000000, 'frequency': 4},
            {'timestamp': 3000000000, 'frequency': 1}
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

    def test_specify_initial_timestamp_case(self):
        records_raw = [
            {'timestamp': 1000000000},
            {'timestamp': 1000500000},
            {'timestamp': 2000000000},
            {'timestamp': 2000300000},
            {'timestamp': 2000500000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp': 400000, 'frequency': 1},
            {'timestamp': 1000400000, 'frequency': 3},
            {'timestamp': 2000400000, 'frequency': 1}
        ]
        result = to_dict(frequency.to_records(initial_timestamp=400000))
        assert result == expect_raw

    def test_two_column_default_case(self):
        records_raw = [
            {'timestamp1': 1000000000, 'timestamp2': 1000001000},
            {'timestamp1': 1000500000, 'timestamp2': 1000501000},
            {'timestamp1': 2000000000, 'timestamp2': 2000001000},
            {'timestamp1': 2000300000, 'timestamp2': 2000301000},
            {'timestamp1': 2000500000, 'timestamp2': 2000501000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp1': 1000000000, 'frequency': 2},
            {'timestamp1': 2000000000, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

    def test_specify_target_column_case(self):
        records_raw = [
            {'timestamp1': 1000000000, 'timestamp2': 1000001000},
            {'timestamp1': 1000500000, 'timestamp2': 1000501000},
            {'timestamp1': 2000000000, 'timestamp2': 2000001000},
            {'timestamp1': 2000300000, 'timestamp2': 2000301000},
            {'timestamp1': 2000500000, 'timestamp2': 2000501000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records, target_column='timestamp2')

        expect_raw = [
            {'timestamp2': 1000001000, 'frequency': 2},
            {'timestamp2': 2000001000, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'timestamp1': 1000000000, 'timestamp2': 1000001000},
            {'timestamp2': 1000501000},
            {'timestamp1': 2000000000, 'timestamp2': 2000001000},
            {'timestamp1': 2000300000, 'timestamp2': 2000301000},
            {'timestamp1': 2000500000, 'timestamp2': 2000501000},
        ]
        columns = ColumnValue('timestamp')
        records = create_records(records_raw, columns)

        frequency = Frequency(records)

        expect_raw = [
            {'timestamp1': 1000000000, 'frequency': 1},
            {'timestamp1': 2000000000, 'frequency': 3}
        ]
        result = to_dict(frequency.to_records())
        assert result == expect_raw
