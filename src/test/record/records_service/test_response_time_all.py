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

import warnings

from caret_analyze.common import ClockConverter
from caret_analyze.record import ColumnValue
from caret_analyze.record import ResponseTime
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


class TestResponseTimeAll:

    def test_empty_case(self, create_converter):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 1},
            {'start': 11, 'response_time': 1}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 1},
            {'start': create_converter.round_convert(11), 'response_time': 1}
        ]
        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_three_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 1, 'end': 2},
            {'start': 3, 'middle': 4, 'end': 6},
            {'start': 11, 'middle': 13, 'end': 16}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 3},
            {'start': 11, 'response_time': 5}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 3},
            {'start': create_converter.round_convert(11), 'response_time': 5}
        ]
        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_single_input_multi_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 2, 'end': 4},
            {'start': 1, 'middle': 4, 'end': 5},
            {'start': 1, 'middle': 4, 'end': 6},
            {'start': 1, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 4}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 4}
        ]
        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_multi_input_single_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4, 'end': 13},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12},
            {'start': 2, 'response_time': 11},
            {'start': 5, 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12},
            {'start': create_converter.round_convert(2), 'response_time': 11},
            {'start': create_converter.round_convert(5), 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_drop_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12},
            {'start': 2, 'response_time': 11},
            {'start': 5, 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12},
            {'start': create_converter.round_convert(2), 'response_time': 11},
            {'start': create_converter.round_convert(5), 'response_time': 8}
        ]
        result = to_dict(response_time.to_all_records(converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_cross_flow_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 10},
            {'start': 6, 'end': 6},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        response_time = response.to_all_records()
        expect = [
            {'start': 0, 'response_time': 10},
            {'start': 4, 'response_time': 6},
            {'start': 6, 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

        response_time = response.to_all_records(converter=create_converter.get_converter())
        expect = [
            {'start': create_converter.round_convert(0), 'response_time': 10},
            {'start': create_converter.round_convert(4), 'response_time': 6},
            {'start': create_converter.round_convert(6), 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

    def test_invalid_value_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12},
            {'start': 13, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            warning_message =\
                'Trace data has reversed timestamps. ' \
                'These data entries will be ignored for accurate analysis.'

            response_time = ResponseTime(records)

            assert issubclass(w[0].category, UserWarning)
            assert str(w[0].message) == warning_message

            expect_raw = [
                {'start': 3, 'response_time': 1},
                {'start': 11, 'response_time': 1}
            ]
            result = to_dict(response_time.to_all_records())
            assert result == expect_raw

            expect_raw = [
                {'start': create_converter.round_convert(3), 'response_time': 1},
                {'start': create_converter.round_convert(11), 'response_time': 1}
            ]
            result = to_dict(response_time.to_all_records(
                converter=create_converter.get_converter()))
            assert result == expect_raw


class TestResponseTimeBest:

    def test_empty_case(self, create_converter):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response_time.to_best_case_records())
        assert result == expect_raw

        expect_raw = [
        ]
        result = to_dict(response_time.to_best_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 1},
            {'start': 11, 'response_time': 1}
        ]
        result = to_dict(response_time.to_best_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 1},
            {'start': create_converter.round_convert(11), 'response_time': 1}
        ]
        result = to_dict(response_time.to_best_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_three_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 1, 'end': 2},
            {'start': 3, 'middle': 4, 'end': 6},
            {'start': 11, 'middle': 13, 'end': 16}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 3},
            {'start': 11, 'response_time': 5}
        ]
        result = to_dict(response_time.to_best_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 3},
            {'start': create_converter.round_convert(11), 'response_time': 5}
        ]
        result = to_dict(response_time.to_best_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_single_input_multi_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 2, 'end': 4},
            {'start': 1, 'middle': 4, 'end': 5},
            {'start': 1, 'middle': 4, 'end': 6},
            {'start': 1, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 4}
        ]
        result = to_dict(response_time.to_best_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 4}
        ]
        result = to_dict(response_time.to_best_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_multi_input_single_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4, 'end': 13},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 5, 'response_time': 8}
        ]
        result = to_dict(response_time.to_best_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(5), 'response_time': 8}
        ]
        result = to_dict(response_time.to_best_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_cross_flow_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 10},
            {'start': 6, 'end': 6},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        response_time = response.to_best_case_records()
        expect = [
            {'start': 4, 'response_time': 6},
            {'start': 6, 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

        response_time = response.to_best_case_records(converter=create_converter.get_converter())
        expect = [
            {'start': create_converter.round_convert(4), 'response_time': 6},
            {'start': create_converter.round_convert(6), 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

    def test_invalid_value_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12},
            {'start': 13, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            warning_message =\
                'Trace data has reversed timestamps. ' \
                'These data entries will be ignored for accurate analysis.'

            response_time = ResponseTime(records)

            assert issubclass(w[0].category, UserWarning)
            assert str(w[0].message) == warning_message

            expect_raw = [
                {'start': 3, 'response_time': 1},
                {'start': 11, 'response_time': 1},
            ]
            result = to_dict(response_time.to_best_case_records())
            assert result == expect_raw

            expect_raw = [
                {'start': create_converter.round_convert(3), 'response_time': 1},
                {'start': create_converter.round_convert(11), 'response_time': 1},
            ]
            result = to_dict(response_time.to_best_case_records(
                converter=create_converter.get_converter()))
            assert result == expect_raw


class TestResponseTimeWorstWithExternalLatency:

    def test_empty_case(self, create_converter):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 9}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 9}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_three_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 1, 'end': 2},
            {'start': 3, 'middle': 4, 'end': 6},
            {'start': 11, 'middle': 13, 'end': 16}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 13}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 13}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_single_input_multi_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 2, 'end': 3},
            {'start': 1, 'middle': 4, 'end': 5},
            {'start': 1, 'middle': 4, 'end': 6},
            {'start': 3, 'middle': 10, 'end': 11},
            {'start': 3, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 10},
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 10},
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_multi_input_single_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4, 'end': 13},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_drop_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_cross_flow_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 10},
            {'start': 6, 'end': 6},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        response_time = response.to_worst_with_external_latency_case_records()
        expect = [
            {'start': 0, 'response_time': 10},
            {'start': 4, 'response_time': 2}
        ]
        assert to_dict(response_time) == expect

        response_time = response.to_worst_with_external_latency_case_records(
            converter=create_converter.get_converter())
        expect = [
            {'start': create_converter.round_convert(0), 'response_time': 10},
            {'start': create_converter.round_convert(4), 'response_time': 2}
        ]
        assert to_dict(response_time) == expect

    def test_invalid_value_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12},
            {'start': 13, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            warning_message =\
                'Trace data has reversed timestamps. ' \
                'These data entries will be ignored for accurate analysis.'

            response_time = ResponseTime(records)

            assert issubclass(w[0].category, UserWarning)
            assert str(w[0].message) == warning_message

            expect_raw = [
                {'start': 3, 'response_time': 9}
            ]
            result = to_dict(response_time.to_worst_with_external_latency_case_records())
            assert result == expect_raw

            expect_raw = [
                {'start': create_converter.round_convert(3), 'response_time': 9}
            ]
            result = to_dict(response_time.to_worst_with_external_latency_case_records(
                converter=create_converter.get_converter()))
            assert result == expect_raw


class TestResponseTimeWorst:

    def test_empty_case(self, create_converter):
        records_raw = []
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = []
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = []
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_two_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 1},
            {'start': 11, 'response_time': 1}
        ]
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 1},
            {'start': create_converter.round_convert(11), 'response_time': 1}
        ]
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_three_column_default_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 1, 'end': 2},
            {'start': 3, 'middle': 4, 'end': 6},
            {'start': 11, 'middle': 13, 'end': 16}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 3, 'response_time': 3},
            {'start': 11, 'response_time': 5}
        ]
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(3), 'response_time': 3},
            {'start': create_converter.round_convert(11), 'response_time': 5}
        ]
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_single_input_multi_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 2, 'end': 4},
            {'start': 1, 'middle': 4, 'end': 5},
            {'start': 1, 'middle': 4, 'end': 6},
            {'start': 1, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 4}
        ]
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 4}
        ]
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_multi_input_single_output_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 3, 'end': 12},
            {'start': 1, 'middle': 4, 'end': 13},
            {'start': 2, 'middle': 4, 'end': 13},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_drop_case(self, create_converter):
        records_raw = [
            {'start': 0, 'middle': 4, 'end': 8},
            {'start': 1, 'middle': 4},
            {'start': 5, 'middle': 12, 'end': 13}
        ]
        columns = [ColumnValue('start'), ColumnValue('middle'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        response_time = ResponseTime(records)

        expect_raw = [
            {'start': 1, 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_case_records())
        assert result == expect_raw

        expect_raw = [
            {'start': create_converter.round_convert(1), 'response_time': 12}
        ]
        result = to_dict(response_time.to_worst_case_records(
            converter=create_converter.get_converter()))
        assert result == expect_raw

    def test_cross_flow_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 10},
            {'start': 6, 'end': 6},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        response_time = response.to_worst_case_records()
        expect = [
            {'start': 0, 'response_time': 10},
            {'start': 6, 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

        response_time = response.to_worst_case_records(converter=create_converter.get_converter())
        expect = [
            {'start': create_converter.round_convert(0), 'response_time': 10},
            {'start': create_converter.round_convert(6), 'response_time': 0}
        ]
        assert to_dict(response_time) == expect

    def test_invalid_value_case(self, create_converter):
        records_raw = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12},
            {'start': 13, 'end': 12}
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            warning_message =\
                'Trace data has reversed timestamps. ' \
                'These data entries will be ignored for accurate analysis.'
            response_time = ResponseTime(records)

            assert issubclass(w[0].category, UserWarning)
            assert str(w[0].message) == warning_message

            expect_raw = [
                {'start': 3, 'response_time': 1},
                {'start': 11, 'response_time': 1},
            ]
            result = to_dict(response_time.to_worst_case_records())
            assert result == expect_raw

            expect_raw = [
                {'start': create_converter.round_convert(3), 'response_time': 1},
                {'start': create_converter.round_convert(11), 'response_time': 1},
            ]
            result = to_dict(response_time.to_worst_case_records(
                converter=create_converter.get_converter()))
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
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 6},
            {'start': 3, 'middle0': 4, 'middle1': 5, 'end': 6}
        ]
        result = to_dict(response_time.to_all_stacked_bar())
        assert result == expect_raw

    def test_invalid_case(self):
        records_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 1, 'middle0': 3, 'middle1': 2, 'end': 4},
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 5}
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 0, 'middle0': 1, 'middle1': 2, 'end': 3},
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 5}
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
        result = to_dict(response_time.to_worst_case_stacked_bar())
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
            {'start': 4, 'middle0': 5, 'middle1': 7, 'end': 8},
            {'start': 6, 'middle0': 7, 'middle1': 8, 'end': 9}
        ]
        result = to_dict(response_time.to_worst_case_stacked_bar())
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
            {'start': 2, 'middle0': 3, 'middle1': 5, 'end': 6},
        ]
        result = to_dict(response_time.to_worst_case_stacked_bar())
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
            {'start': 2, 'middle0': 3, 'middle1': 4, 'end': 6},
        ]
        result = to_dict(response_time.to_worst_case_stacked_bar())
        assert result == expect_raw

    def test_invalid_value_case(self):
        records_raw = [
            {'start': 1, 'middle0': 3, 'middle1': 5, 'end': 7},
            {'start': 2, 'middle0': 5, 'middle1': 4, 'end': 7},
            {'start': 5, 'middle0': 7, 'middle1': 9, 'end': 11},
        ]
        records = create_records(records_raw, self.columns)

        response_time = ResponseTime(records, columns=self.column_names)

        expect_raw = [
            {'start': 1, 'middle0': 3, 'middle1': 5, 'end': 7},
            {'start': 5, 'middle0': 7, 'middle1': 9, 'end': 11},
        ]
        result = to_dict(response_time.to_worst_case_stacked_bar())
        assert result == expect_raw
