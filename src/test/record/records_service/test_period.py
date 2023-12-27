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

from caret_analyze.record import ColumnValue, Period, RecordInterface
from caret_analyze.record.record_factory import RecordsFactory


def create_records(records_raw, columns):
    records = RecordsFactory.create_instance()
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        records.append(record_raw)
    return records


def to_dict(records):
    return [record.data for record in records]


class TestPeriodRecords:

    def test_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        period = Period(records)

        expect_raw = [
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_one_column_default_case(self):
        records_raw = [
            {'timestamp': 0},
            {'timestamp': 1},
            {'timestamp': 11},
            {'timestamp': 12}
        ]
        columns = [ColumnValue('timestamp')]
        records = create_records(records_raw, columns)

        period = Period(records)

        expect_raw = [
            {'timestamp': 0, 'period': 1},
            {'timestamp': 1, 'period': 10},
            {'timestamp': 11, 'period': 1}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_two_column_default_case(self):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3, 'timestamp2': 4},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14}
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        period = Period(records)

        expect_raw = [
            {'timestamp1': 0, 'period': 3},
            {'timestamp1': 3, 'period': 8},
            {'timestamp1': 11, 'period': 2}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_specify_target_column_case(self):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3, 'timestamp2': 4},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14}
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        period = Period(records, target_column='timestamp2')

        expect_raw = [
            {'timestamp2': 2, 'period': 2},
            {'timestamp2': 4, 'period': 8},
            {'timestamp2': 12, 'period': 2}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp2': 4},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14}
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        period = Period(records)

        expect_raw = [
            {'timestamp1': 0, 'period': 11},
            {'timestamp1': 11, 'period': 2}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_two_column_drop_non_target_column_case(self):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14}
        ]
        columns = [ColumnValue('timestamp1'), ColumnValue('timestamp2')]
        records = create_records(records_raw, columns)

        period = Period(records, target_column='timestamp1')

        expect_raw = [
            {'timestamp1': 0, 'period': 3},
            {'timestamp1': 3, 'period': 8},
            {'timestamp1': 11, 'period': 2}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw

    def test_two_column_apply_row_filter_case(self):
        records_raw = [
            {'timestamp1': 0, 'timestamp2': 2},
            {'timestamp1': 3},
            {'timestamp1': 11, 'timestamp2': 12},
            {'timestamp1': 13, 'timestamp2': 14}
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

        period = Period(records, row_filter=row_filter)

        expect_raw = [
            {'timestamp1': 0, 'period': 11},
            {'timestamp1': 11, 'period': 2}
        ]
        result = to_dict(period.to_records())
        assert result == expect_raw
