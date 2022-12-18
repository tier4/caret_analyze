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

from caret_analyze.plot.timeseries.frequency_timeseries import FrequencyTimeSeries
from caret_analyze.record import ColumnValue
from caret_analyze.record.record_factory import RecordFactory, RecordsFactory
from caret_analyze.runtime.callback import CallbackBase


def create_expect_records(records_raw):
    records = RecordsFactory.create_instance()
    columns = [ColumnValue('first'), ColumnValue('last')]
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        record = RecordFactory.create_instance(record_raw)
        records.append(record)
    return records


class TestFrequencyTimeSeries:

    def test_get_timestamp_range_normal(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_records',
            return_value=create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
            ])
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_records',
            return_value=create_expect_records([
                {'first': 4, 'last': 5},
                {'first': 5, 'last': 6}
            ])
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 5

    # TODO: Even if target_objects.to_records is empty, we want to display an empty graph.
    # def test_get_timestamp_range_empty_input(self):
    #     min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range([])
    #     assert min_ts == 0
    #     assert max_ts == 1

    def test_get_timestamp_range_exist_empty_records(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_records',
            return_value=create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
            ])
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_records',
            return_value=create_expect_records([{}])
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 2

    def test_get_timestamp_range_drop(self, mocker):
        object_mock0 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock0, 'to_records',
            return_value=create_expect_records([
                {'first': 1, 'last': 2},
                {'first': 2, 'last': 3}
            ])
        )
        object_mock1 = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(
            object_mock1, 'to_records',
            return_value=create_expect_records([
                {'last': 5},
                {'first': 5, 'last': 6}
            ])
        )
        min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
            [object_mock0, object_mock1])
        assert min_ts == 1
        assert max_ts == 5

    # TODO: Even if target_objects.to_records is empty, we want to display an empty graph.
    # def test_get_timestamp_range_len_timestamp_is_0(self, mocker):
    #     object_mock0 = mocker.Mock(spec=CallbackBase)
    #     mocker.patch.object(
    #         object_mock0, 'to_records',
    #         return_value=create_expect_records([{}])
    #     )
    #     object_mock1 = mocker.Mock(spec=CallbackBase)
    #     mocker.patch.object(
    #         object_mock1, 'to_records',
    #         return_value=create_expect_records([{}])
    #     )
    #     min_ts, max_ts = FrequencyTimeSeries._get_timestamp_range(
    #         [object_mock0, object_mock1])
    #
    #     assert min_ts == 0
    #     assert max_ts == 1
