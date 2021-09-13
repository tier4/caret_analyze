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

from copy import deepcopy
from typing import Any, Optional

from caret_analyze.record.record import merge
from caret_analyze.record.record import Record
from caret_analyze.record.record import Records

import pandas as pd
import pytest

RecordCppImpl: Any
RecordsCppImpl: Any

try:
    import caret_analyze.record.record_cpp_impl as cpp_impl

    RecordCppImpl = cpp_impl.RecordCppImpl
    RecordsCppImpl = cpp_impl.RecordsCppImpl
    CppImplValid = True
except ModuleNotFoundError:
    RecordCppImpl = None
    RecordsCppImpl = None
    CppImplValid = False


def to_cpp_record(record: Record) -> Optional[RecordCppImpl]:
    if not CppImplValid:
        return None

    return RecordCppImpl(record.data)


def to_cpp_records(records: Records) -> Optional[RecordsCppImpl]:
    if not CppImplValid:
        return None

    return RecordsCppImpl([to_cpp_record(dic) for dic in records.data])


def to_py_record(record: RecordCppImpl) -> Optional[Record]:
    if not CppImplValid:
        return None

    return Record(record.data)


def to_py_records(records: RecordsCppImpl) -> Optional[Records]:
    if not CppImplValid:
        return None

    return Records([Record(dic.data) for dic in records.data])


class TestRecord:

    @pytest.mark.parametrize(
        'record_py, record_py_, expect',
        [
            (Record({'stamp': 0}), Record({'stamp': 0}), True),
            (Record({'stamp': 0}), Record({'stamp': 1}), False),
            (Record({'stamp': 0, 'stamp_': 1}), Record(
                {'stamp': 0, 'stamp_': 1}), True),
            (Record({'stamp': 0, 'stamp_': 1}), Record(
                {'stamp_': 1, 'stamp': 0}), True),
            (Record({'stamp': 0, 'stamp_': 1}), Record(
                {'stamp': 1, 'stamp_': 0}), False),
        ],
    )
    def test_equals(self, record_py, record_py_, expect):
        for record, record_ in zip(
            [record_py, to_cpp_record(record_py)], [
                record_py_, to_cpp_record(record_py_)]
        ):
            if not CppImplValid:
                continue

            assert record.equals(record_) is expect

    def test_data(self):
        dic = {'stamp': 0, 'value': 18446744073709551615}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue

            record = record_type(dic)
            assert record.data == dic

    def test_columns(self):
        dic = {'stamp': 0, 'value': 1}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue

            record = record_type(dic)
            assert record.columns == set(dic.keys())

    def test_get(self):
        dic = {'stamp': 0, 'value': 1}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue
            record = record_type(dic)
            assert record.get('stamp') == 0
            assert record.get('value') == 1

    def test_add(self):
        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue
            expect = {'stamp': 0, 'value': 1}

            record = record_type()
            record.add('stamp', 0)
            record.add('value', 1)
            assert record.data == expect
            assert record.columns == expect.keys()

    def test_change_dict_key(self):
        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue
            record = record_type({'key_before': 0, 'value': 1})
            expect = record_type({'key_after': 0, 'value': 1})

            record.change_dict_key('key_before', 'key_after')
            assert record.equals(expect)
            assert record.columns == expect.columns

    def test_drop_columns(self):
        dic = {'stamp': 0, 'value': 1}

        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue
            record = record_type(dic)
            assert record.columns == set(dic.keys())

            dropped = record.drop_columns(['stamp'])
            assert dropped.columns == {'value'}

            record.drop_columns(['stamp'], inplace=True)
            assert record.columns == {'value'}

    def test_merge(self):
        left_dict = {'a': 1, 'b': 2}
        right_dict = {'c': 3, 'd': 4}
        merged_dict = deepcopy(left_dict)
        merged_dict.update(right_dict)

        for record_type in [Record, RecordCppImpl]:
            if not CppImplValid:
                continue
            left = record_type(left_dict)
            right = record_type(right_dict)
            merged = left.merge(right, inplace=False)
            assert left.data == left_dict
            assert right.data == right_dict
            assert merged.data == merged_dict

            left.merge(right, inplace=True)
            assert left.data == merged_dict
            assert right.data == right_dict


class TestRecords:

    def test_data(self):
        expects_py = Records(
            [
                Record({'value': 0, 'stamp': 1}),
                Record({'value': 2, 'stamp': 4}),
            ]
        )
        expects_cpp = to_cpp_records(expects_py)

        for expects, records_type in zip([expects_py, expects_cpp], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue
            records = records_type(expects.data)
            assert records.equals(expects)

    def test_append(self):
        expects_py = Records(
            [
                Record({'value': 0, 'stamp': 1}),
                Record({'value': 2, 'stamp': 4}),
            ]
        )
        expects_cpp = to_cpp_records(expects_py)
        for expects, records_type in zip([expects_py, expects_cpp], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue
            records = records_type()
            records.append(expects.data[0])
            records.append(expects.data[1])
            assert records.equals(expects)
            assert records.columns == expects.columns

    def test_drop_columns(self):
        key = 'stamp'
        records_py: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )

        records_cpp = to_cpp_records(records_py)

        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            drop_keys = [key]
            for record in records.data:
                assert key in record.columns

            dropped_records = records.drop_columns(drop_keys)
            for dropped_record in dropped_records.data:
                assert key not in dropped_record.columns
            assert dropped_records.columns == set()

            records.drop_columns(drop_keys, inplace=True)
            for record in records.data:
                assert key not in record.columns
            assert records.columns == set()

            records_empty = records_type()
            assert records.columns == set()
            records_empty.drop_columns(drop_keys, inplace=True)
            assert records.columns == set()
            assert len(records_empty.data) == 0

    def test_rename_columns(self):
        key_before = 'stamp'
        key_after = 'stamp_'

        records_py: Records = Records(
            [
                Record({key_before: 0}),
                Record(),
            ]
        )
        records_cpp = to_cpp_records(records_py)

        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            rename_keys = {key_before: key_after}
            assert key_before in records.data[0].columns
            assert key_after not in records.data[0].columns
            assert key_before not in records.data[1].columns
            assert key_after not in records.data[1].columns

            assert records.columns == {key_before}

            renamed_records = records.rename_columns(rename_keys)
            assert key_before not in renamed_records.data[0].columns
            assert key_after in renamed_records.data[0].columns
            assert key_before not in renamed_records.data[1].columns
            assert key_after not in renamed_records.data[1].columns

            assert renamed_records.columns == {key_after}

            records.rename_columns(rename_keys, inplace=True)
            assert key_before not in records.data[0].columns
            assert key_after in records.data[0].columns
            assert key_before not in records.data[1].columns
            assert key_after not in records.data[1].columns
            assert records.columns == {key_after}

    def test_filter_if(self):
        key = 'stamp'

        records_py: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )
        records_cpp = to_cpp_records(records_py)
        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            assert len(records.data) == 3
            init_columns = records.columns

            filtered_records = records.filter_if(
                lambda record: record.get(key) == 1)
            assert len(filtered_records.data) == 1
            assert filtered_records.data[0].get(key) == 1
            assert init_columns == filtered_records.columns

            records.filter_if(lambda record: record.get(key) == 1, inplace=True)
            assert init_columns == records.columns
            assert len(records.data) == 1
            assert records.data[0].get(key) == 1

            records.filter_if(lambda _: False, inplace=True)
            assert init_columns == records.columns
            assert len(records.data) == 0

    def test_concat(self):
        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            records_left = records_type(
                [
                    record_type({'a': 0}),
                    record_type({'a': 1}),
                ]
            )
            records_right = records_type(
                [
                    record_type({'b': 3}),
                    record_type({'b': 4}),
                    record_type({'b': 5}),
                ]
            )

            records_expect = records_type(
                records_left.data + records_right.data)

            records_concat = records_left.concat(records_right)
            assert records_concat.equals(records_expect)

            records_left.concat(records_right, inplace=True)
            assert records_left.equals(records_expect)

    def test_to_df(self):
        key = 'stamp'
        records_py: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ]
        )
        records_cpp = to_cpp_records(records_py)
        expect_dict = [record.data for record in records_py.data]
        expect_df = pd.DataFrame.from_dict(expect_dict)

        for records in [records_py, records_cpp]:
            if not CppImplValid:
                continue

            df = records.to_dataframe()
            assert df.equals(expect_df)

    def test_sort(self):
        key = 'stamp'

        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            records = records_type(
                [
                    record_type({key: 2}),
                    record_type({key: 0}),
                    record_type({key: 1}),
                ]
            )
            records_asc = records_type(
                [
                    record_type({key: 0}),
                    record_type({key: 1}),
                    record_type({key: 2}),
                ]
            )
            records_desc = records_type(
                [
                    record_type({key: 2}),
                    record_type({key: 1}),
                    record_type({key: 0}),
                ]
            )
            records_sorted = records.sort(key, inplace=False, ascending=True)
            assert records_sorted.equals(records_asc)

            records_sorted = records.sort(key, inplace=False, ascending=False)
            assert records_sorted.equals(records_desc)

            records_ = records.clone()
            records_.sort(key, inplace=True, ascending=True)
            assert records_.equals(records_asc)

            records_ = records.clone()
            records_.sort(key, inplace=True, ascending=False)
            assert records_.equals(records_desc)

    def test_sort_with_sub_key(self):
        key = 'stamp'
        sub_key = 'stamp_'

        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if not CppImplValid:
                continue

            records = records_type(
                [
                    record_type({key: 2, sub_key: 2}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 0, sub_key: 5}),
                    record_type({key: 1, sub_key: 3}),
                ]
            )
            records_asc = records_type(
                [
                    record_type({key: 0, sub_key: 5}),
                    record_type({key: 1, sub_key: 3}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 2, sub_key: 2}),
                ]
            )
            records_desc = records_type(
                [
                    record_type({key: 2, sub_key: 2}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 1, sub_key: 3}),
                    record_type({key: 0, sub_key: 5}),
                ]
            )
            records_sorted = records.sort(
                key, sub_key=sub_key, inplace=False, ascending=True)
            assert records_sorted.equals(records_asc)

            records_sorted = records.sort(
                key, sub_key=sub_key, inplace=False, ascending=False)
            assert records_sorted.equals(records_desc)

            records_ = records.clone()
            records_.sort(key, sub_key=sub_key, inplace=True, ascending=True)
            assert records_.equals(records_asc)

            records_ = records.clone()
            records_.sort(key, sub_key=sub_key, inplace=True, ascending=False)
            assert records_.equals(records_desc)

    @pytest.mark.parametrize(
        'records_py, records_py_, expect',
        [
            (
                Records(
                    [
                        Record({'stamp': 0}),
                        Record({'stamp': 1}),
                    ]
                ),
                Records(
                    [
                        Record({'stamp': 0}),
                        Record({'stamp': 1}),
                    ]
                ),
                True,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 5, 'stamp_': 6}),
                    ]
                ),
                Records(
                    [
                        Record({'stamp_': 1, 'stamp': 0}),
                        Record({'stamp_': 6, 'stamp': 5}),
                    ]
                ),
                True,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 5, 'stamp_': 7}),
                    ]
                ),
                Records(
                    [
                        Record({'stamp_': 1, 'stamp': 0}),
                        Record({'stamp_': 6, 'stamp': 5}),
                    ]
                ),
                False,
            ),
        ],
    )
    def test_equals(self, records_py, records_py_, expect):
        records_cpp = to_cpp_records(records_py)
        records_cpp_ = to_cpp_records(records_py_)

        for records, records_ in zip([records_py, records_cpp], [records_py_, records_cpp_]):
            if not CppImplValid:
                continue

            assert records.equals(records_) is expect

    @pytest.mark.parametrize(
        'how, records_expect_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'value': 10, 'stamp': 1, 'stamp_': 2}),
                        Record({'value': 20, 'stamp': 3, 'stamp_': 4}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'value': 10, 'stamp': 1, 'stamp_': 2}),
                        Record({'value': 20, 'stamp': 3, 'stamp_': 4}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 40, 'stamp': 7}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'value': 10, 'stamp': 1, 'stamp_': 2}),
                        Record({'value': 20, 'stamp': 3, 'stamp_': 4}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 50, 'stamp_': 10}),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'value': 10, 'stamp': 1, 'stamp_': 2}),
                        Record({'value': 20, 'stamp': 3, 'stamp_': 4}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 30, 'stamp': 5, 'stamp_': 6}),
                        Record({'value': 40, 'stamp': 7}),
                        Record({'value': 50, 'stamp_': 10}),
                    ]
                ),
            ),
        ],
    )
    def test_merge(self, how: str, records_expect_py: Records):
        records_left_py: Records = Records(
            [
                Record({'stamp': 1, 'value': 10}),
                Record({'stamp': 3, 'value': 20}),
                Record({'stamp': 5, 'value': 30}),
                Record({'stamp': 7, 'value': 40}),
            ]
        )

        records_right_py: Records = Records(
            [
                Record({'stamp_': 2, 'value': 10}),
                Record({'stamp_': 4, 'value': 20}),
                Record({'stamp_': 6, 'value': 30}),
                Record({'stamp_': 6, 'value': 30}),
                Record({'stamp_': 10, 'value': 50}),
            ]
        )

        records_left_cpp = to_cpp_records(records_left_py)
        records_right_cpp = to_cpp_records(records_right_py)
        records_expect_cpp = to_cpp_records(records_expect_py)

        for records_left, records_right, records_expect in zip(
            [records_left_py, records_left_cpp],
            [records_right_py, records_right_cpp],
            [records_expect_py, records_expect_cpp],
        ):
            if not CppImplValid:
                continue
            merged = records_left.merge(records_right, 'value', how=how)  # type: ignore
            merged.sort(key='value', inplace=True)
            assert merged.equals(records_expect) is True  # type: ignore

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value': 2,
                            }
                        ),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value': 2,
                            }
                        ),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value': 2,
                            }
                        ),
                        Record({'other_stamp_': 10}),
                        Record({'other_stamp_': 14}),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value': 2,
                            }
                        ),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                        Record({'other_stamp_': 10}),
                        Record({'other_stamp_': 14}),
                    ]
                ),
            ),
        ],
    )
    def test_merge_with_loss(self, how, expect_records_py):
        left_records_py = Records(
            [
                Record({'other_stamp': 4, 'stamp': 1, 'value': 1}),
                Record({'other_stamp': 8}),
                Record({'other_stamp': 12, 'stamp': 9, 'value': 2}),
                Record({'other_stamp': 16}),
            ]
        )

        right_records_py = Records(
            [
                Record({'other_stamp_': 2, 'stamp_': 3, 'value': 2}),
                Record({'other_stamp_': 6, 'stamp_': 7, 'value': 1}),
                Record({'other_stamp_': 10}),
                Record({'other_stamp_': 14}),
            ]
        )

        left_records_cpp = to_cpp_records(left_records_py)
        right_records_cpp = to_cpp_records(right_records_py)
        expect_records_cpp = to_cpp_records(expect_records_py)

        for left_records, right_records, expect_records in zip(
            [left_records_py, left_records_cpp],
            [right_records_py, right_records_cpp],
            [expect_records_py, expect_records_cpp],
        ):
            if not CppImplValid:
                continue

            merged = merge(
                left_records=left_records, right_records=right_records, join_key='value', how=how
            )

            assert merged.equals(expect_records) is True

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key': 1, 'stamp': 6}),
                        Record({'key': 2, 'stamp': 7}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key': 1, 'sub_stamp': 4}),
                        Record({'key': 2, 'sub_stamp': 5}),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key': 1, 'sub_stamp': 4}),
                        Record({'key': 2, 'sub_stamp': 5}),
                        Record({'key': 1, 'stamp': 6}),
                        Record({'key': 2, 'stamp': 7}),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_key(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'key': 1, 'stamp': 0}),
                Record({'key': 2, 'stamp': 1}),
                Record({'key': 1, 'stamp': 6}),
                Record({'key': 2, 'stamp': 7}),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'key': 2, 'sub_stamp': 2}),
                Record({'key': 1, 'sub_stamp': 3}),
                Record({'key': 1, 'sub_stamp': 4}),
                Record({'key': 2, 'sub_stamp': 5}),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if not CppImplValid:
                continue

            merged_records = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_key='key',
                how=how,
            )

            assert merged_records.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'sub_stamp': 6}),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'sub_stamp': 6}),
                        Record({'stamp': 4}),
                        Record({'stamp': 5}),
                        Record({'stamp': 8}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'sub_stamp': 6}),
                        Record({'sub_stamp': 7}),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'sub_stamp': 6}),
                        Record({'stamp': 4}),
                        Record({'stamp': 5}),
                        Record({'sub_stamp': 7}),
                        Record({'stamp': 8}),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_without_key(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'stamp': 0}),
                Record({'stamp': 3}),
                Record({'stamp': 4}),
                Record({'stamp': 5}),
                Record({'stamp': 8}),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'sub_stamp': 1}),
                Record({'sub_stamp': 6}),
                Record({'sub_stamp': 7}),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if not CppImplValid:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_key=None,
                how=how,
            )

            assert merged.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key': 1, 'stamp': 4, 'sub_stamp': 7}),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key': 1, 'stamp': 4, 'sub_stamp': 7}),
                        Record({'key': 1, 'stamp': 5}),
                        Record({'key': 1, 'stamp': 6}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key': 3, 'sub_stamp': 1}),
                        Record({'key': 1, 'sub_stamp': 3}),
                        Record({'key': 1, 'stamp': 4, 'sub_stamp': 7}),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key': 3, 'sub_stamp': 1}),  # stamp lost
                        Record({'key': 1, 'sub_stamp': 3}),  # stamp lost
                        Record({'key': 1, 'stamp': 4, 'sub_stamp': 7}),
                        Record({'key': 1, 'stamp': 5}),  # sub_stamp lost
                        Record({'key': 1, 'stamp': 6}),  # sub_stamp lost
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_drop(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'key': 1, 'stamp': 0}),
                Record({'key': 1, 'stamp': 4}),
                Record({'key': 1, 'stamp': 5}),
                Record({'key': 1, 'stamp': 6}),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'key': 3, 'sub_stamp': 1}),
                Record({'key': 1, 'sub_stamp': 2}),
                Record({'key': 1, 'sub_stamp': 3}),
                Record({'key': 1, 'sub_stamp': 7}),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if not CppImplValid:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_key='key',
                how=how,
            )

            assert merged.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value': 1,
                            }
                        )
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value': 1,
                            }
                        ),
                        Record({'other_stamp': 12, 'stamp': 9, 'value': 1}),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value': 1,
                            }
                        ),
                        Record({'other_stamp_': 6, 'stamp_': 7, 'value': 1}),
                        Record({'other_stamp_': 10, 'stamp_': 10}),
                        Record({'other_stamp_': 14, 'value': 2}),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_loss(self, how, expect_records_py):
        left_records_py = Records(
            [
                Record({'other_stamp': 4, 'stamp': 1, 'value': 1}),
                Record({'other_stamp': 8}),
                Record({'other_stamp': 12, 'stamp': 9, 'value': 1}),
                Record({'other_stamp': 16}),
            ]
        )

        right_records_py = Records(
            [
                Record({'other_stamp_': 2, 'stamp_': 3, 'value': 1}),
                Record({'other_stamp_': 6, 'stamp_': 7, 'value': 1}),
                Record({'other_stamp_': 10, 'stamp_': 10}),
                Record({'other_stamp_': 14, 'value': 2}),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if not CppImplValid:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='stamp_',
                join_key='value',
                how=how,
            )

            assert merged.equals(expect_records) is True

    def test_merge_sequencial_for_addr_track(self):
        source_records: Records = Records(
            [
                Record({'source_addr': 1, 'source_stamp': 0}),
                Record({'source_addr': 1, 'source_stamp': 10}),
                Record({'source_addr': 3, 'source_stamp': 20}),
            ]
        )

        copy_records: Records = Records(
            [
                Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 1}),
                Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 11}),
                Record({'addr_from': 3, 'addr_to': 13, 'copy_stamp': 21}),
            ]
        )

        sink_records: Records = Records(
            [
                Record({'sink_addr': 13, 'sink_stamp': 2}),
                Record({'sink_addr': 1, 'sink_stamp': 3}),
                Record({'sink_addr': 13, 'sink_stamp': 12}),
                Record({'sink_addr': 13, 'sink_stamp': 22}),
            ]
        )

        expect_records = Records(
            [
                Record({'sink_stamp': 2, 'source_addr': 1, 'source_stamp': 0}),
                Record({'sink_stamp': 3, 'source_addr': 1, 'source_stamp': 0}),
                Record({'sink_stamp': 12, 'source_addr': 1, 'source_stamp': 10}),
                Record({'sink_stamp': 22, 'source_addr': 3, 'source_stamp': 20}),
            ]
        )

        for source_records, copy_records, sink_records, expect_records in zip(
            [source_records, to_cpp_records(source_records)],
            [copy_records, to_cpp_records(copy_records)],
            [sink_records, to_cpp_records(sink_records)],
            [expect_records, to_cpp_records(expect_records)],
        ):
            if not CppImplValid:
                continue

            merged = source_records.merge_sequencial_for_addr_track(
                source_stamp_key='source_stamp',
                source_key='source_addr',
                copy_records=copy_records,
                copy_stamp_key='copy_stamp',
                copy_from_key='addr_from',
                copy_to_key='addr_to',
                sink_records=sink_records,
                sink_stamp_key='sink_stamp',
                sink_from_key='sink_addr',
            )

            merged.sort(key='sink_stamp', inplace=True)
            expect_records.sort(key='sink_stamp', inplace=True)

            assert merged.equals(expect_records)
