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

from caret_analyze.plot.stacked_bar import LatencyStackedBar
from caret_analyze.record import ColumnValue, RecordsFactory
from caret_analyze.record import RecordsInterface
from caret_analyze.runtime import Path

import pandas as pd
import pytest


@pytest.fixture
def create_mock(mocker):
    def _create_mock(data, columns):
        records_columns = [ColumnValue(column) for column in columns]
        records = RecordsFactory.create_instance(data, columns=records_columns)
        target_objects = mocker.Mock(spec=Path)
        stacked_bar_plot = LatencyStackedBar(target_objects)
        column_map = {}
        for column in columns:
            column_map[column] = column + '_renamed'
        mocker.patch.object(stacked_bar_plot, '_get_response_time_record', return_value=records)
        return stacked_bar_plot
    return _create_mock


def create_records(records_raw, columns):
    records = RecordsFactory.create_instance()
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        records.append(record_raw)
    return records


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

    return data, columns, expect_dict, expect_columns


class TestLatencyStackedBar:

    def test_empty_case(self, create_mock):
        data = []
        columns = []
        stacked_bar_plot: LatencyStackedBar = create_mock(data, columns)
        with pytest.raises(ValueError):
            stacked_bar_plot.to_stacked_bar_data()

    def test_default_case(self, create_mock):
        data, columns, expect_dict, expect_columns = get_data_set()
        # create mock
        stacked_bar_plot: LatencyStackedBar = create_mock(data, columns)

        # create stacked bar data
        output_dict, output_columns = stacked_bar_plot.to_stacked_bar_data()

        assert output_dict == expect_dict
        assert output_columns == expect_columns

    def test_to_dataframe(self, create_mock):
        data, columns, expect_dict, _ = get_data_set()
        for column in expect_dict.keys():
            expect_dict[column] = [timestamp * 1e-6 for timestamp in expect_dict[column]]
        expect_df = pd.DataFrame(expect_dict)

        # create mock
        stacked_bar_plot: LatencyStackedBar = create_mock(data, columns)

        # create stacked bar data
        output_df = stacked_bar_plot.to_dataframe()
        assert output_df.equals(expect_df)

    @pytest.mark.parametrize(
        'case',
        ['all', 'best', 'worst', 'worst-with-external-latency']
    )
    def test_invalid_timestamps(self, mocker, case):
        data = []
        columns = []
        records_columns = [ColumnValue(column) for column in columns]
        records = RecordsFactory.create_instance(data, columns=records_columns)
        target_objects = mocker.Mock(spec=Path)
        records_Interface_mock = mocker.Mock(spec=RecordsInterface)

        records_raw = [
            {'start': 0, 'end': 1},
            {'start': 5, 'end': 4},
            {'start': 7, 'end': 8},
        ]
        columns = [ColumnValue('start'), ColumnValue('end')]
        records = create_records(records_raw, columns)

        mocker.patch.object(target_objects, 'column_names', ['start', 'end'])
        mocker.patch.object(target_objects, 'to_records', return_value=records)

        mock_path = 'caret_analyze.record.records_service.response_time.ResponseTime.'
        mocker.patch(mock_path+'to_all_stacked_bar',
                     return_value=records_Interface_mock)
        mocker.patch(mock_path+'to_best_case_stacked_bar',
                     return_value=records_Interface_mock)
        mocker.patch(mock_path+'to_worst_case_stacked_bar',
                     return_value=records_Interface_mock)
        mocker.patch(mock_path+'to_worst_with_external_latency_case_stacked_bar',
                     return_value=records_Interface_mock)

        stacked_bar_plot = LatencyStackedBar(target_objects)
        stacked_bar_plot._case = case
        response_records: RecordsInterface = \
            stacked_bar_plot._get_response_time_record(stacked_bar_plot._target_objects)
        assert response_records.columns[2] == 'invalid_timestamps'
        assert len(response_records.data) == 0
