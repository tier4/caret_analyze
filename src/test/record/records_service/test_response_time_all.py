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
from caret_analyze.record import ResponseTime
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


class TestResponseTimeAll:

    def test_empty_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

    def test_two_column_default_case(self):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'response_time': 2},
            {'start': 3, 'response_time': 1},
            {'start': 11, 'response_time': 1}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

    def test_three_column_default_case(self):
        records_raw = [
            {'start': 0, 'middle': 1, 'end': 2},
            {'start': 3, 'middle': 4, 'end': 6},
            {'start': 11, 'middle': 13, 'end': 16}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'response_time': 2},
            {'start': 3, 'response_time': 3},
            {'start': 11, 'response_time': 5}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

    def test_single_input_multi_output_case(self):
        records_raw = [
            {'start': 0, 'middle': 4, 'end': 5},
            {'start': 0, 'middle': 4, 'end': 6},
            {'start': 0, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'response_time': 5}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

    def test_multi_input_single_output_case(self):
        records_raw = [
            {'start': 0, 'middle': 4, 'end': 13},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'response_time': 13},
            {'start': 1, 'response_time': 12},
            {'start': 5, 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'start': 0, 'middle': 4, 'end': 13},
            {'start': 1, 'middle': 4},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'response_time': 13},
            {'start': 1, 'response_time': 12},
            {'start': 5, 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw


class TestAllStackedBar:

    @property
    def columns(self) -> list[ColumnValue]:
        return [ColumnValue('start'), ColumnValue('middle0'),
                ColumnValue('middle1'), ColumnValue('end')]

    @property
    def column_names(self) -> list[str]:
        return ['start', 'middle0', 'middle1', 'end']

    def test_empty_case(self):
        records_raw = []
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = []
        result = to_dict(response_time.to_all_stacked_bar())
        assert result == expect_raw

    def test_single_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 4, 'middle0': 5, 'middle1': 7, 'end': 8},
            {'start': 6, 'middle0': 7, 'middle1': 8, 'end': 9}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 4, 'middle0': 5, 'middle1': 7, 'end': 8},
            {'start': 6, 'middle0': 7, 'middle1': 8, 'end': 9}
        ]
        result = to_dict(response_time.to_all_stacked_bar())
        assert result == expect_raw

    def test_multi_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 5, 'end': 6},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 5, 'end': 6},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        result = to_dict(response_time.to_all_stacked_bar())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 4},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 6},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        result = to_dict(response_time.to_all_stacked_bar())
        assert result == expect_raw


class TestWorstInInputStackedBar:

    @property
    def columns(self) -> list[ColumnValue]:
        return [ColumnValue('start'), ColumnValue('middle0'),
                ColumnValue('middle1'), ColumnValue('end')]

    @property
    def column_names(self) -> list[str]:
        return ['start', 'middle0', 'middle1', 'end']

    def test_empty_case(self):
        records_raw = []
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = []
        result = to_dict(response_time.to_worst_in_input_case_stacked_bar())
        assert result == expect_raw

    def test_single_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 4, 'middle0': 5, 'middle1': 7, 'end': 8},
            {'start': 6, 'middle0': 7, 'middle1': 8, 'end': 9}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 4, 'middle0': 5, 'middle1': 7, 'end': 8},
            {'start': 6, 'middle0': 7, 'middle1': 8, 'end': 9}
        ]
        result = to_dict(response_time.to_worst_in_input_case_stacked_bar())
        assert result == expect_raw

    def test_multi_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 5, 'end': 6},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 5, 'end': 6},
        ]
        result = to_dict(response_time.to_worst_in_input_case_stacked_bar())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1},
            {'start': 1, 'middle0': 2, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 4},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 3, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 6},
        ]
        result = to_dict(response_time.to_worst_in_input_case_stacked_bar())
        assert result == expect_raw
