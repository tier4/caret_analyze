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

from caret_analyze.record import ColumnValue, ResponseTime
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


def check(records_raw, expect_raw, columns):
    records = create_records(records_raw, columns)
    response = ResponseTime(records).to_records()
    output_dict = to_dict(response)
    assert output_dict == expect_raw


class TestResponseRecords:

    def test_empty_flow_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = []
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

    def test_single_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

    def test_double_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 2, 'end': 3},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

        expect_raw = [
            {'start': 2, 'end': 3},
        ]
        result = to_dict(response.to_best_case_stacked_bar())
        assert result == expect_raw

    # NOTE: Is this test up to specification?
    def test_cross_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 8},
            {'start': 6, 'end': 6},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start_min': 0, 'start_max': 3, 'end': 4},
            {'start_min': 3, 'start_max': 6, 'end': 6},
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

        expect_raw = [
            {'start': 3, 'end': 4},
            {'start': 6, 'end': 6},
        ]
        result = to_dict(response.to_best_case_stacked_bar())
        assert result == expect_raw

    def test_triple_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 2, 'end': 3},
            {'start': 10, 'end': 11},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
            {'start_min': 2, 'start_max': 10, 'end': 11},
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

        expect_raw = [
            {'start': 2, 'end': 3},
            {'start': 10, 'end': 11},
        ]
        result = to_dict(response.to_best_case_stacked_bar())
        assert result == expect_raw

    # NOTE: Is this test up to specification?
    def test_double_flow_cross_case(self):
        records_raw = [
            {'start': 0, 'end': 5},
            {'start': 2, 'end': 3},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

    def test_drop_case(self):
        records_raw = [
            {'start': 0},
            {'start': 2, 'end': 3},
            {'start': 3, 'end': 4},
        ]
        columns = ['start', 'end']
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start_min': 2, 'start_max': 3, 'end': 4},
        ]
        result = to_dict(response.to_worst_with_external_latency_case_stacked_bar())
        assert result == expect_raw

    def test_reversed_timestamp_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 5, 'end': 4},
            {'start': 7, 'end': 8},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        assert response._has_invalid_timestamps() is True

        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 4, 'end': 5},
            {'start': 7, 'end': 8},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        assert response._has_invalid_timestamps() is False

    class TestMultiColumnCase:
        records_raw = [
            {'column0': 5, 'column1': 10, 'column2': 15},  # flow 1 [used as first data]
            {'column0': 5, 'column1': 12, 'column2': 17},  # flow 2 [used]
            {'column0': 6, 'column1': 15},  # flow 3. Dropped case [ignored]
            {'column0': 7, 'column2': 20},  # flow 4. Crossed & dropped case [ignored]
            {'column0': 8, 'column1': 21, 'column2': 50},  # flow 5, too old output [ignored]
            {'column0': 9, 'column1': 25, 'column2': 30},  # flow  6. too early input [ignored]
            {'column0': 20, 'column1': 25, 'column2': 30},  # flow  7. [used]
            {'column0': 35, 'column1': 40, 'column2': 45},  # flow  8. [used]
        ]

        columns = [
            ColumnValue('column0'),
            ColumnValue('column1'),
            ColumnValue('column2'),
        ]
        column_names = ['column0', 'column1', 'column2']

        def test_to_response_records(self):
            response = ResponseTime(
                create_records(self.records_raw, self.columns),
                columns=self.column_names
            )

            records = response.to_worst_with_external_latency_case_stacked_bar()

            expect = [
                # flow 1 input ~ flow 7 output
                {'column0_min': 5, 'column0_max': 20, 'column1': 25, 'column2': 30},
                # flow 7 input ~ flow 8 output
                {'column0_min': 20, 'column0_max': 35, 'column1': 40, 'column2': 45},
            ]

            output_dict = to_dict(records)
            assert output_dict == expect
