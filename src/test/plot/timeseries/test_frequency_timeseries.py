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

from caret_analyze.plot.timeseries.frequency_timeseries import FrequencyTimeSeries
from caret_analyze.record import ColumnValue
from caret_analyze.record.record_factory import RecordFactory, RecordsFactory
from caret_analyze.runtime.callback import CallbackBase
import pandas as pd


def create_expect_records(records_raw):
    records = RecordsFactory.create_instance()
    columns = [ColumnValue('first'), ColumnValue('last')]
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        record = RecordFactory.create_instance(record_raw)
        records.append(record)
    return records


class TestGetTimestampRange:

    def test_get_timestamp_range_normal(self, mocker):
        records0 = create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
        ])
        records1 = create_expect_records([
                {'first': 4, 'last': 5},
                {'first': 5, 'last': 6}
        ])

        min_ts, max_ts = \
            FrequencyTimeSeries._get_timestamp_range([records0, records1])

        assert min_ts == 1
        assert max_ts == 5

    def test_get_timestamp_range_empty_input(self):
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range([])

        assert min_ts == 0
        assert max_ts == 1

    def test_get_timestamp_range_exist_empty_records(self, mocker):
        records0 = create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
        ])
        records1 = create_expect_records([{}])

        min_ts, max_ts = \
            FrequencyTimeSeries._get_timestamp_range([records0, records1])

        assert min_ts == 1
        assert max_ts == 2

    def test_get_timestamp_range_drop(self, mocker):
        records0 = create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
        ])
        records1 = create_expect_records([
                {'last': 5},
                {'first': 5, 'last': 6}
        ])

        min_ts, max_ts = \
            FrequencyTimeSeries._get_timestamp_range([records0, records1])

        assert min_ts == 1
        assert max_ts == 5

    def test_get_timestamp_range_len_timestamp_is_0(self, mocker):
        records0 = create_expect_records([{}])
        records1 = create_expect_records([{}])
        min_ts, max_ts = \
            FrequencyTimeSeries._get_timestamp_range([records0, records1])

        assert min_ts == 0
        assert max_ts == 1


class TestFrequencyTimeSeries:

    @staticmethod
    def create_sample_record(record_raw_list: list[dict]) -> RecordsFactory:
        records = RecordsFactory.create_instance()
        columns = [ColumnValue(key) for key in record_raw_list[0].keys()]
        for column in columns:
            records.append_column(column, [])

        for record_raw in record_raw_list:
            record = RecordFactory.create_instance(record_raw)
            records.append(record)
        return records

    def get_frequency_dataframe(self, mocker, input_records: list[dict]):
        records = TestFrequencyTimeSeries.create_sample_record(input_records)
        cb_mock = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(cb_mock, 'to_records', return_value=records)
        mocker.patch.object(cb_mock, 'column_names', ['timestamp', 'some_data'])

        # Check if mock works as expected
        assert cb_mock.to_records() == records
        assert cb_mock.column_names == ['timestamp', 'some_data']

        frequency_timeseries = FrequencyTimeSeries([cb_mock])
        actual_df = frequency_timeseries.to_dataframe('timestamp')
        actual_df.columns = actual_df.columns.droplevel(0)

        return actual_df

    def test_to_dataframe_remove_last_period(self, mocker):
        records = [
            {'timestamp': 1, 'some_data': 2},
            {'timestamp': 2, 'some_data': 3},
            {'timestamp': 1000000001, 'some_data': 4},
        ]
        except_freq_df = pd.DataFrame(
            data=[
                {'timestamp [ns]': 1, 'frequency [Hz]': 2}
            ]
        )

        actual_df = self.get_frequency_dataframe(mocker, records)
        actual_df = actual_df.astype(except_freq_df.dtypes.to_dict())  # Type conversion

        assert actual_df.equals(except_freq_df)
