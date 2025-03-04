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

from logging import WARNING

from caret_analyze.record import ColumnValue, Range
from caret_analyze.record.record_factory import RecordsFactory


def create_records(records_raw):
    columns = [ColumnValue('start'), ColumnValue('end')]
    records = RecordsFactory.create_instance()
    for column in columns:
        records.append_column(column, [])

    for record_raw in records_raw:
        records.append(record_raw)
    return records


class TestRange:

    def test_get_range_normal(self):
        records_raw1 = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        records_raw2 = [
            {'start': 1, 'end': 3},
            {'start': 4, 'end': 5},
            {'start': 12, 'end': 13}
        ]
        records_list = [create_records(records_raw1), create_records(records_raw2)]
        record_range = Range(records_list)
        assert record_range.get_range() == (0, 12)

    def test_get_range_reversed_order_records_case(self):
        records_raw = [
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12},
            {'start': 0, 'end': 2},
        ]
        records_list = [create_records(records_raw)]
        record_range = Range(records_list)
        assert record_range.get_range() == (0, 11)

    def test_get_range_drop_other_column(self):
        records_raw1 = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11}
        ]
        records_raw2 = [
            {'start': 1, 'end': 3},
            {'start': 4, 'end': 5},
            {'start': 12}
        ]
        records_list = [create_records(records_raw1), create_records(records_raw2)]
        record_range = Range(records_list)
        assert record_range.get_range() == (0, 12)

    def test_get_range_drop_target_column(self):
        records_raw1 = [
            {'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        records_raw2 = [
            {'start': 1, 'end': 3},
            {'start': 4, 'end': 5},
            {'end': 13}
        ]
        records_list = [create_records(records_raw1), create_records(records_raw2)]
        record_range = Range(records_list)
        assert record_range.get_range() == (1, 11)

    def test_get_range_exist_empty_records(self):
        records_raw1 = [
            {'start': 0, 'end': 2},
            {'start': 3, 'end': 4},
            {'start': 11, 'end': 12}
        ]
        records_raw2 = [
        ]
        records_list = [create_records(records_raw1), create_records(records_raw2)]
        record_range = Range(records_list)
        assert record_range.get_range() == (0, 11)

    def test_get_range_no_input(self, caplog):
        record_range = Range([])

        assert record_range.get_range() == (0, 1)
        assert 'No valid' in caplog.text
        assert caplog.records[0].levelno == WARNING

    def test_get_range_only_empty_records(self, caplog):
        records_raw1 = [
        ]
        records_raw2 = [
        ]
        records_list = [create_records(records_raw1), create_records(records_raw2)]
        record_range = Range(records_list)

        assert record_range.get_range() == (0, 1)
        assert 'No valid' in caplog.text
        assert caplog.records[0].levelno == WARNING
