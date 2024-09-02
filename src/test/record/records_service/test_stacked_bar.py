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

from caret_analyze.record import ColumnValue
from caret_analyze.record import RecordsFactory, RecordsInterface
from caret_analyze.record import StackedBar

import pytest


def create_records(data, columns):
    columns = [ColumnValue(column) for column in columns]
    records = RecordsFactory.create_instance(
        data, columns=columns)
    return records


def to_dict(records):
    return [record.data for record in records]


def get_data_set():
    columns = [
        '/columns_0/rclcpp_publish_timestamp/0_min',
        '/columns_1/rclcpp_publish_timestamp/0_max',
        '/columns_2/rcl_publish_timestamp/0',
        '/columns_3/dds_write_timestamp/0',
        '/columns_4/callback_0/callback_start_timestamp/0',
        '/columns_5/rclcpp_publish_timestamp/0_max',
        '/columns_6/rcl_publish_timestamp/0',
        '/columns_7/dds_write_timestamp/0',
        '/columns_8/callback_0/callback_start_timestamp/0',
        '/columns_9/callback_0/callback_end_timestamp/0',
    ]

    # create input and expect data
    # # columns | c0 | c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 |
    # # ===========================================================
    # # data    | 0  | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  |
    # #         | 1  | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 |
    # #         | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9  | 10 | 11 |

    expect_dict = {
        '[worst - best] response time': [1, 1, 1],  # c1 - c0
        '/columns_1':                   [3, 3, 3],  # c4 - c1
        '/columns_4/callback_0':        [1, 1, 1],  # c5 - c4
        '/columns_5':                   [3, 3, 3],  # c8 - c5
        '/columns_8/callback_0':        [1, 1, 1],  # c9 - c8
        'start time':                   [0, 1, 2],  # c0
    }
    expect_columns = [
        '[worst - best] response time',
        '/columns_1',
        '/columns_4/callback_0',
        '/columns_5',
        '/columns_8/callback_0',
    ]

    data_num = 3
    data = []
    for i in range(data_num):
        d = {}
        for j in range(len(columns)):
            d[columns[j]] = i + j
        data.append(d)

    return columns, data, expect_columns, expect_dict


def get_data_set_no_data():
    columns = [
        '/columns_8/callback_0/callback_start_timestamp/0',
        '/columns_9/callback_0/callback_end_timestamp/0',
        'invalid_timestamps',
    ]
    expect_dict = {
    }
    expect_columns = [
        '/columns_8/callback_0',
    ]
    data = []

    return columns, data, expect_columns, expect_dict


class TestStackedBar:

    def test_empty_case(self):
        records: RecordsInterface = create_records([], [])
        with pytest.raises(ValueError):
            StackedBar(records)

    def test_columns(self):
        columns, data, expect_columns, _ = get_data_set()
        records: RecordsInterface = create_records(data, columns)

        stacked_bar = StackedBar(records)
        assert stacked_bar.columns == expect_columns

    def test_to_dict(self):
        columns, data, _, expect_dict = get_data_set()
        records: RecordsInterface = create_records(data, columns)

        stacked_bar = StackedBar(records)
        assert stacked_bar.to_dict() == expect_dict

    def test_records(self):
        columns, data, expect_columns, pre_expect_dict = get_data_set()
        records: RecordsInterface = create_records(data, columns)
        expect_columns += ['start time']
        expect_dict = []
        for i in range(len(pre_expect_dict[expect_columns[0]])):
            d = {}
            for column in expect_columns:
                d[column] = pre_expect_dict[column][i]
            expect_dict.append(d)

        stacked_bar = StackedBar(records)
        result = to_dict(stacked_bar.records)
        assert result == expect_dict

    def test_invalid_timestamps(self):
        columns, data, expect_columns, _ = get_data_set_no_data()
        records: RecordsInterface = create_records(data, columns)

        stacked_bar = StackedBar(records)
        assert stacked_bar.columns == expect_columns
        assert len(stacked_bar.records.data) == 0
