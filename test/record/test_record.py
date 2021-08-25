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

from trace_analysis.record.record import (
    Record,
    Records,
    RecordsIterator,
    merge,
    merge_sequencial,
    merge_sequencial_with_copy,
)


class TestRecord:
    @pytest.mark.parametrize(
        "record, record_, equal",
        [
            (Record({"stamp": 0}), Record({"stamp": 0}), True),
            (Record({"stamp": 0}), Record({"stamp": 1}), False),
            (Record({"stamp": 0, "stamp_": 1}), Record({"stamp": 0, "stamp_": 1}), True),
            (Record({"stamp": 0, "stamp_": 1}), Record({"stamp_": 1, "stamp": 0}), True),
            (Record({"stamp": 0, "stamp_": 1}), Record({"stamp": 1, "stamp_": 0}), False),
        ],
    )
    def test_equals(self, record, record_, equal):
        assert record.equals(record_) is equal


class TestRecords:
    def test_drop_columns(self):
        key = "stamp"
        records: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )
        assert records.columns == set([key])

        drop_keys = [key]
        for record in records:
            assert key in record.keys()

        dropped_records = records.drop_columns(drop_keys)
        for dropped_record in dropped_records:
            assert key not in dropped_record.keys()
        assert dropped_records.columns == set()

        records.drop_columns(drop_keys, inplace=True)
        for record in records:
            assert key not in record.keys()
        assert records.columns == set()

        records_empty = Records([Record()])
        assert records.columns == set()
        records_empty.drop_columns(drop_keys, inplace=True)
        assert records.columns == set()
        assert len(records_empty) == 1

    def test_rename_columns(self):
        key_before = "stamp"
        key_after = "stamp_"

        records: Records = Records(
            [
                Record({key_before: 0}),
                Record({key_before: 1}),
                Record({key_before: 2}),
            ]
        )

        rename_keys = {key_before: key_after}

        for record in records:
            assert key_before in record.keys()
            assert key_after not in record.keys()
        assert records.columns == set([key_before])

        renamed_records = records.rename_columns(rename_keys)
        for renamed_record in renamed_records:
            assert key_before not in renamed_record.keys()
            assert key_after in renamed_record.keys()
        assert renamed_records.columns == set([key_after])

        records.rename_columns(rename_keys, inplace=True)
        for record in records:
            assert key_before not in record.keys()
            assert key_after in record.keys()
        assert records.columns == set([key_after])

    def test_filter(self):
        key = "stamp"

        records: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )
        assert len(records) == 3
        init_columns = records.columns

        filtered_records = records.filter(lambda record: record[key] == 1)
        assert len(filtered_records) == 1
        assert filtered_records[0][key] == 1
        assert init_columns == filtered_records.columns

        records.filter(lambda record: record[key] == 1, inplace=True)
        assert init_columns == records.columns
        assert len(records) == 1
        assert records[0][key] == 1

        records.filter(lambda _: False, inplace=True)
        assert init_columns == records.columns
        assert len(records) == 0

    def test_to_df(self):
        key = "stamp"
        records: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )

        df = records.to_dataframe()
        assert df.equals(pd.DataFrame.from_dict(records))

    @pytest.mark.parametrize(
        "records, records_, equal",
        [
            (
                Records(
                    [
                        Record({"stamp": 0}),
                        Record({"stamp": 1}),
                    ]
                ),
                Records(
                    [
                        Record({"stamp": 0}),
                        Record({"stamp": 1}),
                    ]
                ),
                True,
            ),
            (
                Records(
                    [
                        Record({"stamp": 0, "stamp_": 1}),
                        Record({"stamp": 5, "stamp_": 6}),
                    ]
                ),
                Records(
                    [
                        Record({"stamp_": 1, "stamp": 0}),
                        Record({"stamp_": 6, "stamp": 5}),
                    ]
                ),
                True,
            ),
        ],
    )
    def test_equals(self, records, records_, equal):
        assert records.equals(records_) is equal

    def test_iterrows(self):
        records: Records = Records()
        it = records.iterrows()
        assert isinstance(it, RecordsIterator)


class TestRecordsIterator:
    def test_next(self):
        key = "stamp"

        records: Records = Records()
        for i in range(3):
            records.append(Record({key: i}))

        for i, record in enumerate(records.iterrows()):
            assert record[key] == i

    def test_copy(self):
        key = "stamp"

        records: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )

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

    def test_has_get(self):
        key = "stamp"
        records: Records = Records([Record({key: 0}), Record({key: 1})])
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
        assert next_item is None

        records_empty: Records = Records([])
        it_ = records_empty.iterrows()

        assert it_.current_value() is None
        next_item = it_.next_value()
        assert next_item is None

    def test_forward_while(self):
        key = "stamp"
        records: Records = Records([Record({key: 0}), Record({key: 1}), Record({key: 2})])
        it = records.iterrows()
        it.forward_while(lambda record: record[key] <= 1)
        item = it.current_value()
        assert item[key] == 2

    def test_find_item(self):
        records: Records = Records(
            [
                Record({"stamp": 0, "value": 0}),
                Record({"stamp": 1, "value": 1}),
                Record({"stamp": 2, "value": 1}),
                Record({"stamp": 3, "value": 2}),
            ]
        )
        it = records.iterrows()

        item = it.find_item(lambda record: record["stamp"] == 2)
        assert item["stamp"] == 2

        item = it.find_item(lambda record: record["value"] == 1, item_index=1)
        assert item["stamp"] == 2

        item = it.find_item(lambda record: record["value"] == 1, item_index=3)
        assert item is None

        it.find_item(lambda record: record["stamp"] == 2, inplace_iter_count=True)
        item = it.current_value()
        assert item["stamp"] == 2


@pytest.mark.parametrize(
    "how, records_expect",
    [
        (
            "inner",
            Records(
                [
                    Record({"value": 2, "stamp": 2, "stamp_": 4}),
                    Record({"value": 3, "stamp": 3, "stamp_": 5}),
                ]
            ),
        ),
        (
            "left",
            Records(
                [
                    Record({"value": 1, "stamp": 0}),
                    Record({"value": 2, "stamp": 2, "stamp_": 4}),
                    Record({"value": 3, "stamp": 3, "stamp_": 5}),
                ]
            ),
        ),
        (
            "right",
            Records(
                [
                    Record({"value": 2, "stamp": 2, "stamp_": 4}),
                    Record({"value": 3, "stamp": 3, "stamp_": 5}),
                    Record({"value": 4, "stamp_": 6}),
                ]
            ),
        ),
        (
            "outer",
            Records(
                [
                    Record({"value": 1, "stamp": 0}),
                    Record({"value": 2, "stamp": 2, "stamp_": 4}),
                    Record({"value": 3, "stamp": 3, "stamp_": 5}),
                    Record({"value": 4, "stamp_": 6}),
                ]
            ),
        ),
    ],
)
def test_merge(how: str, records_expect: Records):
    records_left: Records = Records(
        [
            Record({"stamp": 0, "value": 1}),
            Record({"stamp": 2, "value": 2}),
            Record({"stamp": 3, "value": 3}),
        ]
    )

    records_right: Records = Records(
        [
            Record({"stamp_": 4, "value": 2}),
            Record({"stamp_": 5, "value": 3}),
            Record({"stamp_": 6, "value": 4}),
        ]
    )

    merged = merge(records_left, records_right, "value", how=how)

    merged.sort(key=lambda record: record["value"])
    records_expect.sort(key=lambda record: record["value"])
    assert merged.equals(records_expect) is True


def test_merge_sequencial():
    records: Records = Records(
        [
            Record({"key": 1, "stamp": 0}),
            Record({"key": 3, "stamp": 2}),
            Record({"key": 1, "stamp": 3}),
        ]
    )

    sub_records: Records = Records(
        [
            Record({"key": 1, "sub_stamp": 1}),
            Record({"key": 1, "sub_stamp": 4}),
            Record({"key": 3, "sub_stamp": 5}),
            Record({"key": 1, "sub_stamp": 7}),
            Record({"key": 5, "sub_stamp": 8}),
        ]
    )

    merged = merge_sequencial(
        records=records,
        sub_records=sub_records,
        record_stamp_key="stamp",
        sub_record_stamp_key="sub_stamp",
        join_key="key",
    )

    records_expect: Records = Records(
        [
            Record({"key": 1, "stamp": 0, "sub_stamp": 1}),
            Record({"key": 1, "stamp": 3, "sub_stamp": 4}),
            Record({"key": 3, "stamp": 2, "sub_stamp": 5}),
        ]
    )

    merged.sort(key=lambda record: record["stamp"])
    records_expect.sort(key=lambda record: record["stamp"])

    assert merged.equals(records_expect)


def test_merge_sequencial_with_drop():
    # ココを考える。
    records: Records = Records(
        [
            Record({"key": 1, "stamp": 0}),
            Record({"key": 1, "stamp": 4}),
            Record({"key": 1, "stamp": 5}),
            Record({"key": 1, "stamp": 6}),
        ]
    )

    sub_records: Records = Records(
        [
            Record({"key": 3, "sub_stamp": 1}),
            Record({"key": 1, "sub_stamp": 2}),
            Record({"key": 1, "sub_stamp": 3}),
            Record({"key": 1, "sub_stamp": 7}),
        ]
    )

    # merged = merge_sequencial_with_drop(
    merged = merge_sequencial(
        records=records,
        sub_records=sub_records,
        record_stamp_key="stamp",
        sub_record_stamp_key="sub_stamp",
        join_key="key",
    )

    records_expect: Records = Records(
        [
            Record({"key": 1, "stamp": 0, "sub_stamp": 2}),
            Record({"key": 1, "stamp": 4}),
            Record({"key": 1, "stamp": 5}),
            Record({"key": 1, "stamp": 6, "sub_stamp": 7}),
        ]
    )

    merged.sort(key=lambda record: record["stamp"])
    records_expect.sort(key=lambda record: record["stamp"])

    assert merged.equals(records_expect)


def test_merge_sequencial_with_copy():
    source_records: Records = Records(
        [
            Record({"source_addr": 1, "source_stamp": 0}),
            Record({"source_addr": 1, "source_stamp": 10}),
            Record({"source_addr": 3, "source_stamp": 20}),
        ]
    )

    copy_records: Records = Records(
        [
            Record({"addr_from": 1, "addr_to": 13, "copy_stamp": 1}),
            Record({"addr_from": 1, "addr_to": 13, "copy_stamp": 11}),
            Record({"addr_from": 3, "addr_to": 13, "copy_stamp": 21}),
        ]
    )

    sink_records: Records = Records(
        [
            Record({"sink_addr": 13, "sink_stamp": 2}),
            Record({"sink_addr": 1, "sink_stamp": 3}),
            Record({"sink_addr": 13, "sink_stamp": 12}),
            Record({"sink_addr": 13, "sink_stamp": 22}),
        ]
    )

    merged = merge_sequencial_with_copy(
        source_records=source_records,
        source_stamp_key="source_stamp",
        source_key="source_addr",
        copy_records=copy_records,
        copy_stamp_key="copy_stamp",
        copy_from_key="addr_from",
        copy_to_key="addr_to",
        sink_records=sink_records,
        sink_stamp_key="sink_stamp",
        sink_from_key="sink_addr",
    )

    records_expect: Records = Records(
        [
            Record({"sink_addr": {1, 13}, "sink_stamp": 2, "source_addr": 1, "source_stamp": 0}),
            Record({"sink_addr": {1, 13}, "sink_stamp": 3, "source_addr": 1, "source_stamp": 0}),
            Record({"sink_addr": {1, 13}, "sink_stamp": 12, "source_addr": 1, "source_stamp": 10}),
            Record({"sink_addr": {3, 13}, "sink_stamp": 22, "source_addr": 3, "source_stamp": 20}),
        ]
    )

    merged.sort(key=lambda record: record["sink_stamp"])
    records_expect.sort(key=lambda record: record["sink_stamp"])

    assert merged.equals(records_expect)
