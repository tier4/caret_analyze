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

import pytest

import pandas as pd

from trace_analysis.trace.record import (
    Record,
    Records,
    RecordsIterator,
    merge,
    merge_sequencial,
    merge_sequencial_with_copy
)


def test_Record_equals():
    record = Record({'stamp': 0})
    record_ = Record({'stamp': 0})
    record__ = Record({'stamp': 1})

    assert record.equals(record_) is True
    assert record.equals(record__) is False


def test_Records_drop_columns():
    key = 'stamp'
    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
        Record({key: 2}),
    ])

    drop_keys = [key]
    for record in records:
        assert key in record.keys()

    dropped_records = records.drop_columns(drop_keys)
    for dropped_record in dropped_records:
        assert key not in dropped_record.keys()

    records.drop_columns(drop_keys, inplace=True)
    for record in records:
        assert key not in record.keys()

    records_empty = Records([
        Record()
    ])
    records_empty.drop_columns(drop_keys, inplace=True)
    assert len(records_empty) == 1


def test_Records_rename_columns():
    key_before = 'stamp'
    key_after = 'stamp_'

    records: Records = Records([
        Record({key_before: 0}),
        Record({key_before: 1}),
        Record({key_before: 2}),
    ])

    rename_keys = {key_before: key_after}

    for record in records:
        assert key_before in record.keys()
        assert key_after not in record.keys()

    renamed_records = records.rename_columns(rename_keys)
    for renamed_record in renamed_records:
        assert key_before not in renamed_record.keys()
        assert key_after in renamed_record.keys()

    records.rename_columns(rename_keys, inplace=True)
    for record in records:
        assert key_before not in record.keys()
        assert key_after in record.keys()


def test_Records_filter():
    key = 'stamp'

    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
        Record({key: 2}),
    ])
    assert len(records) == 3

    filtered_records = records.filter(lambda record: record[key] == 1)
    assert len(filtered_records) == 1
    assert filtered_records[0][key] == 1

    records.filter(lambda record: record[key] == 1, inplace=True)
    assert len(records) == 1
    assert records[0][key] == 1


def test_Records_to_df():
    key = 'stamp'
    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
        Record({key: 2}),
    ])

    df = records.to_df()
    assert df.equals(pd.DataFrame.from_dict(records))


def test_Records_equals():
    key = 'stamp'
    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
    ])
    records_: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
    ])
    records__: Records = Records([
        Record({key: 1}),
        Record({key: 0}),
    ])

    assert records.equals(records_) is True
    assert records.equals(records__) is False



def test_Records_iterrows():
    records: Records = Records()
    it = records.iterrows()
    assert isinstance(it, RecordsIterator)


def test_RecordsIterator_next():
    key = 'stamp'

    records: Records = Records()
    for i in range(3):
        records.append(Record({key: i}))

    for i, record in enumerate(records.iterrows()):
        assert record[key] == i


def test_RecordsIterator_copy():
    key = 'stamp'

    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
        Record({key: 2}),
    ])

    it = records.iterrows()
    it_ = it.copy()
    assert len(list(it_)) == 3
    assert len(list(it)) == 3

    it = records.iterrows()
    next(it)
    it_ = it.copy()
    assert len(list(it_)) == 2
    assert len(list(it)) == 2

    it = records.iterrows()
    next(it)
    next(it)
    it_ = it.copy()
    assert len(list(it_)) == 1
    assert len(list(it)) == 1


def test_RecordsIterator_has_get():
    key = 'stamp'
    records: Records = Records([
        Record({key: 0}),
        Record({key: 1})
    ])
    it = records.iterrows()
    next(it)

    item = it.current_value()
    assert item[key] == 0
    next_item = it.next_value()
    assert next_item[key] == 1
    next(it)

    item = it.current_value()
    assert item[key] == 1
    next_item = it.next_value()
    assert next_item == None

    records_empty: Records = Records([])
    it_ = records_empty.iterrows()

    assert it_.current_value() == None
    next_item = it_.next_value()
    assert next_item == None


def test_RecordsIterator_forward_while():
    key = 'stamp'
    records: Records = Records([
        Record({key: 0}),
        Record({key: 1}),
        Record({key: 2})
    ])
    it = records.iterrows()
    it.forward_while(lambda record: record[key] <= 1)
    item = it.current_value()
    assert item[key] == 2


def test_RecordsIterator_find_item():
    records: Records = Records([
        Record({'stamp': 0, 'value': 0}),
        Record({'stamp': 1, 'value': 1}),
        Record({'stamp': 2, 'value': 1}),
        Record({'stamp': 3, 'value': 2})
    ])
    it = records.iterrows()

    item = it.find_item(lambda record: record['stamp'] == 2)
    assert item['stamp'] == 2

    item = it.find_item(lambda record: record['value'] == 1, item_index=1)
    assert item['stamp'] == 2

    item = it.find_item(lambda record: record['value'] == 1, item_index=3)
    assert item is None

    it.find_item(lambda record: record['stamp'] == 2, inplace_iter_count=True)
    item = it.current_value()
    assert item['stamp'] == 2


def test_merge():
    records_left: Records = Records([
        Record({'stamp': 0, 'value': 1}),
        Record({'stamp': 2, 'value': 2}),
        Record({'stamp': 3, 'value': 3}),
    ])

    records_right: Records = Records([
        Record({'stamp_': 1, 'value': 1}),
        Record({'stamp_': 4, 'value': 2}),
    ])

    merged = merge(records_left, records_right, 'value')

    records_expect: Records = Records([
        Record({'stamp': 0, 'stamp_': 1, 'value': 1}),
        Record({'stamp': 2, 'stamp_': 4, 'value': 2}),
    ])

    merged.sort(key=lambda record: record['stamp'])
    records_expect.sort(key=lambda record: record['stamp'])
    assert merged.equals(records_expect) == True

    records_expect.sort(key=lambda record: record['stamp'], reverse=True)
    assert merged.equals(records_expect) == False


def test_merge_sequencial():
    records: Records = Records([
        Record({'key': 1, 'stamp': 0}),
        Record({'key': 3, 'stamp': 2}),
        Record({'key': 1, 'stamp': 3}),
    ])

    sub_records: Records = Records([
        Record({'key': 1, 'sub_stamp': 1}),
        Record({'key': 1, 'sub_stamp': 4}),
        Record({'key': 3, 'sub_stamp': 5}),
        Record({'key': 1, 'sub_stamp': 7}),
        Record({'key': 5, 'sub_stamp': 8}),
    ])

    merged = merge_sequencial(
        records=records,
        sub_records=sub_records,
        record_stamp_key='stamp',
        sub_record_stamp_key='sub_stamp',
        join_key='key')

    records_expect: Records = Records([
        Record({'key': 1, 'stamp': 0, 'sub_stamp': 1}),
        Record({'key': 1, 'stamp': 3, 'sub_stamp': 4}),
        Record({'key': 3, 'stamp': 2, 'sub_stamp': 5}),
    ])

    merged.sort(key=lambda record: record['stamp'])
    records_expect.sort(key=lambda record: record['stamp'])

    assert merged.equals(records_expect)
    merged = (Records(), Records(), '', '','')


def test_merge_sequencial_with_copy():
    source_records: Records = Records([
        Record({'source_addr': 1, 'source_stamp': 0}),
        Record({'source_addr': 1, 'source_stamp': 10}),
        Record({'source_addr': 3, 'source_stamp': 20}),
    ])

    copy_records: Records = Records([
        Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 1}),
        Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 11}),
        Record({'addr_from': 3, 'addr_to': 13, 'copy_stamp': 21}),
    ])

    sink_records: Records = Records([
        Record({'sink_addr': 13, 'sink_stamp': 2}),
        Record({'sink_addr':  1, 'sink_stamp': 3}),
        Record({'sink_addr': 13, 'sink_stamp': 12}),
        Record({'sink_addr': 13, 'sink_stamp': 22}),
    ])

    merged = merge_sequencial_with_copy(
        source_records=source_records,
        source_stamp_key='source_stamp',
        source_key='source_addr',
        copy_records=copy_records,
        copy_stamp_key='copy_stamp',
        copy_from_key='addr_from',
        copy_to_key='addr_to',
        sink_records=sink_records,
        sink_stamp_key='sink_stamp',
        sink_from_key='sink_addr')

    records_expect: Records = Records([
        Record({'sink_addr': {1, 13},
                'sink_stamp': 2,
                'source_addr': 1,
                'source_stamp': 0}),
        Record({'sink_addr': {1, 13},
                'sink_stamp': 3,
                'source_addr': 1,
                'source_stamp': 0}),
        Record({'sink_addr': {1, 13},
                'sink_stamp': 12,
                'source_addr': 1,
                'source_stamp': 10}),
        Record({'sink_addr': {3, 13},
                'sink_stamp': 22,
                'source_addr': 3,
                'source_stamp': 20}),
    ])


    merged.sort(key=lambda record: record['sink_stamp'])
    records_expect.sort(key=lambda record: record['sink_stamp'])

    assert merged.equals(records_expect)
