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

from caret_analyze.exceptions import InvalidRecordsError
from caret_analyze.record import ColumnValue
from caret_analyze.record.record_factory import RecordsFactory
from caret_analyze.record.response_time import ResponseTime

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


def check(records_raw, expect_raw, columns):
    records = create_records(records_raw, columns)
    response = ResponseTime(records).to_records()
    d = to_dict(response)
    assert d == expect_raw


class TestResponseRecords:

    def test_empty_flow_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = []
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = []
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = []
        assert to_dict(response.to_records(all_pattern=True)) == expect_raw

    def test_single_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 0, 'end': 1}
        ]
        assert to_dict(response.to_records(all_pattern=True)) == expect_raw

    def test_double_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 2, 'end': 3},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'end': 3},
            {'start': 2, 'end': 3},
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 0, 'end': 1},
            {'start': 0, 'end': 3},
            {'start': 2, 'end': 3},
        ]
        result = to_dict(response.to_records(all_pattern=True))
        assert result == expect_raw

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
            {'start': 0, 'end': 4},
            {'start': 3, 'end': 4},
            {'start': 3, 'end': 6},
            {'start': 6, 'end': 6},
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start_min': 0, 'start_max': 3, 'end': 4},
            {'start_min': 3, 'start_max': 6, 'end': 6},
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 0, 'end': 4},
            {'start': 0, 'end': 6},
            {'start': 0, 'end': 8},
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 8},
            {'start': 6, 'end': 6},
        ]
        result = to_dict(response.to_records(all_pattern=True))
        assert result == expect_raw

        expect_raw = [
            {'start': 3, 'end': 4},
            {'start': 6, 'end': 6},
        ]

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
            {'start': 0, 'end': 3},
            {'start': 2, 'end': 3},
            {'start': 2, 'end': 11},
            {'start': 10, 'end': 11}
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
            {'start_min': 2, 'start_max': 10, 'end': 11},
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 0, 'end': 1},
            {'start': 0, 'end': 3},
            {'start': 0, 'end': 11},
            {'start': 2, 'end': 3},
            {'start': 10, 'end': 11},
        ]

        result = to_dict(response.to_records(all_pattern=True))
        assert result == expect_raw

        expect_raw = [
            {'start': 2, 'end': 3},
            {'start': 10, 'end': 11},
        ]

    def test_double_flow_cross_case(self):
        records_raw = [
            {'start': 0, 'end': 5},
            {'start': 2, 'end': 3},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        expect_raw = [
            {'start': 0, 'end': 3},
            {'start': 2, 'end': 3}
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start_min': 0, 'start_max': 2, 'end': 3},
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 0, 'end': 3},
            {'start': 0, 'end': 5},
            {'start': 2, 'end': 3},
        ]

        result = to_dict(response.to_records(all_pattern=True))
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
            {'start': 2, 'end': 4},
            {'start': 3, 'end': 4}
        ]
        result = to_dict(response.to_records())
        assert result == expect_raw

        expect_raw = [
            {'start_min': 2, 'start_max': 3, 'end': 4},
        ]
        result = to_dict(response.to_response_records())
        assert result == expect_raw

        expect_raw = [
            {'start': 2, 'end': 3},
            {'start': 2, 'end': 4},
            {'start': 3, 'end': 4},
        ]

        result = to_dict(response.to_records(all_pattern=True))
        assert result == expect_raw


class TestResponseHistogram:

    def test_empty(self):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        with pytest.raises(InvalidRecordsError):
            response.to_histogram()

        with pytest.raises(InvalidRecordsError):
            response.to_best_case_histogram()

        with pytest.raises(InvalidRecordsError):
            response.to_worst_case_histogram()

    def test_single_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        with pytest.raises(InvalidRecordsError):
            response.to_histogram()

        with pytest.raises(InvalidRecordsError):
            response.to_best_case_histogram()

        with pytest.raises(InvalidRecordsError):
            response.to_worst_case_histogram()

    def test_double_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 2, 'end': 3},  # latency: 1~3
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        hist, latency = response.to_histogram(1)
        assert list(hist) == [1, 1, 1]
        assert list(latency) == [1, 2, 3, 4]

        hist, latency = response.to_best_case_histogram(1)
        assert list(hist) == [1]
        assert list(latency) == [1, 2]

        hist, latency = response.to_worst_case_histogram(1)
        assert list(hist) == [1]
        assert list(latency) == [3, 4]

    def test_cross_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 10},
            {'start': 3, 'end': 4},
            {'start': 4, 'end': 8},
            {'start': 6, 'end': 6},
            # latency: 1~4
            # latency: 0~3
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        hist, latency = response.to_histogram(1)
        assert list(hist) == [
            1,  # [0, 1)
            2,  # [1, 2)
            2,  # [2, 3)
            2,  # [3, 4)
            1,  # [4, 5]
        ]
        assert list(latency) == [0, 1, 2, 3, 4, 5]

        hist, latency = response.to_best_case_histogram(1)
        assert list(hist) == [1, 1]
        assert list(latency) == [0, 1, 2]

        hist, latency = response.to_worst_case_histogram(1)
        assert list(hist) == [1, 1]
        assert list(latency) == [3, 4, 5]

    def test_triple_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},  # latency:
            {'start': 2, 'end': 3},  # 1~3
            {'start': 10, 'end': 11},  # 1~9
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        hist, latency = response.to_histogram(1)
        assert list(hist) == [
            2,  # [1, 2)
            2,  # [2, 3)
            2,  # [3, 4)
            1,  # [4, 5)
            1,  # [5, 6)
            1,  # [6, 7)
            1,  # [7, 8)
            1,  # [8, 9)
            1,  # [9, 10]
        ]
        assert list(latency) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        hist, latency = response.to_best_case_histogram(1)
        assert list(hist) == [
            2,  # [1, 2]
        ]
        assert list(latency) == [1, 2]

        hist, latency = response.to_worst_case_histogram(1)
        assert list(hist) == [
            1,  # [3, 4)
            0,  # [4, 5)
            0,  # [5, 6)
            0,  # [6, 7)
            0,  # [7, 8)
            0,  # [8, 9)
            1,  # [9, 10]
        ]
        assert list(latency) == [3, 4, 5, 6, 7, 8, 9, 10]

    def test_double_flow_cross_case(self):
        records_raw = [
            {'start': 0, 'end': 5},
            {'start': 2, 'end': 3},
            # latency: 1~3
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        hist, latency = response.to_histogram(1)
        assert list(hist) == [
            1,  # [0, 1)
            1,  # [1, 2)
            1,  # [2, 3]
        ]
        assert list(latency) == [1, 2, 3, 4]

        hist, latency = response.to_best_case_histogram(1)
        assert list(hist) == [1]
        assert list(latency) == [1, 2]

        hist, latency = response.to_worst_case_histogram(1)
        assert list(hist) == [1]
        assert list(latency) == [3, 4]

    def test_hist_bin_size(self):
        records_raw = [
            {'start': 0, 'end': 0},
            {'start': 20, 'end': 30},  # latency: 10~30
            {'start': 30, 'end': 40},  # latency: 10~20
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        latency_min, latency_max = 10, 30

        hist, latency = response.to_histogram(1, False)
        hist_expected = [
            2,  # [10, 11)
            2,  # [11, 12)
            2,  # [12, 13)
            2,  # [13, 14)
            2,  # [14, 15)
            2,  # [15, 16)
            2,  # [16, 17)
            2,  # [17, 18)
            2,  # [18, 19)
            2,  # [19, 20)
            2,  # [20, 21)
            1,  # [21, 22)
            1,  # [22, 23)
            1,  # [23, 24)
            1,  # [24, 25)
            1,  # [25, 26)
            1,  # [26, 27)
            1,  # [27, 28)
            1,  # [28, 29)
            1,  # [29, 30)
            1,  # [30, 31]
        ]

        latency_expected = list(range(latency_min, latency_max + 2))
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

        hist, latency = response.to_histogram(2, False)
        hist_expected = [
            2,  # [10, 12)
            2,  # [12, 14)
            2,  # [14, 16)
            2,  # [16, 18)
            2,  # [18, 20)
            2,  # [20, 22)
            1,  # [22, 24)
            1,  # [24, 26)
            1,  # [26, 28)
            1,  # [28, 30)
            1,  # [30, 32]
        ]

        latency_expected = list(range(latency_min, latency_max+2 + 2, 2))
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

        hist, latency = response.to_histogram(3)
        hist_expected = [
            2,  # [9, 12)
            2,  # [12, 15)
            2,  # [15, 18)
            2,  # [18, 21)
            1,  # [21, 24)
            1,  # [24, 27)
            1,  # [27, 30)
            1,  # [30, 33]
        ]
        latency_expected = [9, 12, 15, 18, 21, 24, 27, 30, 33]
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

        hist, latency = response.to_histogram(5)
        hist_expected = [
            2,  # [10, 15)
            2,  # [15, 20)
            2,  # [20, 25)
            1,  # [25, 30)
            1,  # [30, 35]
        ]
        latency_expected = [10, 15, 20, 25, 30, 35]
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

        hist, latency = response.to_best_case_histogram(2)

        hist, latency = response.to_histogram(100)
        hist_expected = [
            2,  # [0, 100]
        ]
        latency_expected = [0, 100]
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

        hist, latency = response.to_best_case_histogram(3)
        hist_expected = [
            2, 0
        ]
        latency_expected = [9, 12, 15]
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected

    def test_hist_count(self):
        records_raw = [
            {'start': 1, 'end': 2},  # latency:
            {'start': 2, 'end': 3},  # 1~2
            {'start': 3, 'end': 4},  # 1~2
            {'start': 4, 'end': 5},  # 1~2
            {'start': 5, 'end': 6},  # 1~2
            {'start': 6, 'end': 8},  # 2~3
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)
        hist, latency = response.to_histogram(1, False)

        hist_expected = [
            4,  # [1, 2)
            5,  # [2, 3)
            1,  # [3, 4]
        ]
        latency_expected = [
            1, 2, 3, 4
        ]
        assert list(hist) == hist_expected
        assert list(latency) == latency_expected


class TestResponseTimeseries:

    def test_empty_flow_case(self):
        records_raw = [
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        t, latency = response.to_best_case_timeseries()
        t_expect = []
        latency_expect = []
        assert list(t) == t_expect
        assert list(latency) == latency_expect

        t, latency = response.to_worst_case_timeseries()
        t_expect = []
        latency_expect = []
        assert list(t) == t_expect
        assert list(latency) == latency_expect

    def test_single_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        t, latency = response.to_best_case_timeseries()
        t_expect = []
        latency_expect = []
        assert list(t) == t_expect
        assert list(latency) == latency_expect

        t, latency = response.to_worst_case_timeseries()
        t_expect = []
        latency_expect = []
        assert list(t) == t_expect
        assert list(latency) == latency_expect

    def test_double_flow_case(self):
        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 2, 'end': 3},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]

        records = create_records(records_raw, columns)
        response = ResponseTime(records)

        t, latency = response.to_best_case_timeseries()
        t_expect = [2]
        latency_expect = [1]
        assert list(t) == t_expect
        assert list(latency) == latency_expect

        t, latency = response.to_worst_case_timeseries()
        t_expect = [0]
        latency_expect = [3]
        assert list(t) == t_expect
        assert list(latency) == latency_expect

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

        t, latency = response.to_best_case_timeseries()
        t_expect = [3, 6]
        latency_expect = [1, 0]
        assert list(t) == t_expect
        assert list(latency) == latency_expect

        t, latency = response.to_worst_case_timeseries()
        t_expect = [0, 3]
        latency_expect = [4, 3]
        assert list(t) == t_expect
        assert list(latency) == latency_expect
