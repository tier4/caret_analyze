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

from caret_analyze.exceptions import InvalidArgumentError, ItemNotFoundError
from caret_analyze.record import (
    ColumnValue,
    merge,
    Record,
    RecordInterface,
    Records,
    RecordsInterface,
)

from caret_analyze.record.column import ColumnMapper

import pandas as pd
import pytest


RecordCppImpl: Any
RecordsCppImpl: Any

try:
    import caret_analyze.record.record_cpp_impl as cpp_impl

    RecordCppImpl = cpp_impl.RecordCppImpl
    RecordsCppImpl = cpp_impl.RecordsCppImpl
    CppImplEnabled = True
except ModuleNotFoundError:
    RecordCppImpl = None
    RecordsCppImpl = None
    CppImplEnabled = False


def to_cpp_record(record: RecordInterface) -> Optional[RecordCppImpl]:
    assert isinstance(record, Record)
    if not CppImplEnabled:
        return None

    return RecordCppImpl(record.data)


def to_cpp_records(records: RecordsInterface) -> RecordsCppImpl:
    assert isinstance(records, Records)
    if not CppImplEnabled:
        return None

    return RecordsCppImpl(
        [to_cpp_record(record) for record in records.data], records.columns.to_value()
    )


def to_py_record(record: RecordCppImpl) -> Optional[Record]:
    if not CppImplEnabled:
        return None

    return Record(record.data)


def to_py_records(records: RecordsCppImpl) -> Optional[Records]:
    if not CppImplEnabled:
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
            if not CppImplEnabled:
                continue

            assert record.equals(record_) is expect

    def test_data(self):
        dic = {'stamp': 0, 'value': 18446744073709551615}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue

            record = record_type(dic)
            assert record.data == dic

    def test_columns(self):
        dic = {'stamp': 0, 'value': 1}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue

            record = record_type(dic)
            assert record.columns == set(dic.keys())

    def test_get(self):
        dic = {'stamp': 0, 'value': 1}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            record = record_type(dic)
            assert record.get('stamp') == 0
            assert record.get('value') == 1

    def test_get_with_default(self):
        dic = {'stamp': 0}
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            record = record_type(dic)
            assert record.get_with_default('stamp', 0) == 0
            assert record.get_with_default('value', 0) == 0
            assert record.get_with_default('value', 1) == 1

    def test_add(self):
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            expect = {'stamp': 0, 'value': 1}

            record = record_type()
            record.add('stamp', 0)
            record.add('value', 1)
            assert record.data == expect
            assert record.columns == expect.keys()

    def test_change_dict_key(self):
        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            record = record_type({'key_before': 0, 'value': 1})
            expect = record_type({'key_after': 0, 'value': 1})

            record.change_dict_key('key_before', 'key_after')
            assert record.equals(expect)
            assert record.columns == expect.columns

    def test_drop_columns(self):
        dic = {'stamp': 0, 'value': 1}

        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            record = record_type(dic)
            assert record.columns == set(dic.keys())

            record.drop_columns(['stamp'])
            assert record.columns == {'value'}

    def test_merge(self):
        left_dict = {'a': 1, 'b': 2}
        right_dict = {'c': 3, 'd': 4}
        merged_dict = deepcopy(left_dict)
        merged_dict.update(right_dict)

        for record_type in [Record, RecordCppImpl]:
            if not CppImplEnabled:
                continue
            left = record_type(left_dict)
            right = record_type(right_dict)

            left.merge(right)
            assert left.data == merged_dict
            assert right.data == right_dict


class TestRecords:

    def test_init(self):
        Records()
        with pytest.raises(InvalidArgumentError):
            Records([Record({'a': 1})], None)

        with pytest.raises(InvalidArgumentError):
            Records(None, [ColumnValue('a'), ColumnValue('a')])

        RecordsCppImpl()
        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl([RecordCppImpl({'a': 1})], None)

        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl(None, [ColumnValue('a'), ColumnValue('a')])

    def test_columns(self):
        columns = (
            ColumnValue('value'),
            ColumnValue('stamp'),
        )

        for records_type in [Records, RecordsCppImpl]:
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue
            records = records_type([], columns)
            assert records.columns.to_value() == columns
            assert records.column_names == ['value', 'stamp']

    def test_data(self):
        expects_py = Records(
            [
                Record({'value': 0, 'stamp': 1}),
                Record({'value': 2, 'stamp': 4}),
            ],
            [
                ColumnValue('value'),
                ColumnValue('stamp')
            ]
        )
        expects_cpp = to_cpp_records(expects_py)

        for expects, records_type in zip([expects_py, expects_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue
            records = records_type(expects.data, expects.columns.to_value())
            assert records.equals(expects)

    def test_len(self):
        expects_py = Records(
            [
                Record({'value': 0, 'stamp': 1}),
                Record({'value': 2, 'stamp': 4}),
            ],
            [
                ColumnValue('value'),
                ColumnValue('stamp')
            ]
        )
        expects_cpp = to_cpp_records(expects_py)

        for expects, records_type in zip([expects_py, expects_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue
            assert len(expects) == 2

    def test_append(self):
        expects_py = Records(
            [
                Record({'value': 0, 'stamp': 1}),
                Record({'value': 2, 'stamp': 4}),
            ],
            [
                ColumnValue('value'),
                ColumnValue('stamp')
            ]
        )
        expects_cpp = to_cpp_records(expects_py)
        for expects, record_type, records_type in \
                zip([expects_py, expects_cpp], [Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue
            records = records_type(None, [ColumnValue('value'), ColumnValue('stamp')])
            records.append(expects.data[0])
            records.append(expects.data[1])
            assert records.equals(expects)
            assert records.columns.to_value() == expects.columns.to_value()

    def test_drop_columns(self):
        key = 'stamp'
        value = 'value'
        records_py: Records = Records(
            [
                Record({key: 0, value: 3}),
                Record({key: 1, value: 4}),
                Record({key: 2, value: 5}),
            ],
            [
                ColumnValue(key),
                ColumnValue(value)
            ]
        )

        records_cpp = to_cpp_records(records_py)
        column_names_expect = [value]

        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if records_type is RecordsCppImpl and not CppImplEnabled:
                continue

            drop_keys = [key]
            records.columns.get(key)
            records.columns.drop(drop_keys)
            for record in records.data:
                assert key not in record.columns
            with pytest.raises(ItemNotFoundError):
                records.columns.get(key)
            assert records.column_names == column_names_expect
            for datum in records.data:
                assert datum.columns == set(column_names_expect)
            assert records.data[0].get(value) == 3
            assert records.data[1].get(value) == 4
            assert records.data[2].get(value) == 5

            records_empty = records_type()
            assert records_empty.columns == []
            records_empty._drop_columns(drop_keys)
            assert records_empty.columns == []
            assert len(records_empty.data) == 0

            with pytest.raises(InvalidArgumentError):
                records.columns.drop('')

    def test_rename_columns(self):
        records_py: Records = Records(
            [
                Record({'stamp': 0, 'aaa': 1}),
                Record(),
            ],
            [
                ColumnValue('stamp'),
                ColumnValue('aaa')
            ]
        )

        records_cpp = to_cpp_records(records_py)

        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            assert 'stamp' in records.data[0].columns
            assert 'stamp_' not in records.data[0].columns
            assert 'stamp' not in records.data[1].columns
            assert 'stamp_' not in records.data[1].columns

            assert 'aaa' in records.data[0].columns
            assert 'aaa_' not in records.data[0].columns
            assert 'aaa' not in records.data[1].columns
            assert 'aaa_' not in records.data[1].columns

            assert records.column_names == ['stamp', 'aaa']
            for column, column_name in zip(records.columns, ['stamp', 'aaa']):
                assert column.column_name == column_name

            rename_keys = {
                'aaa': 'aaa_',
                'stamp': 'stamp_',
            }

            records.columns.rename(rename_keys)
            assert records.column_names == ['stamp_', 'aaa_']
            for column, column_name in zip(records.columns, ['stamp_', 'aaa_']):
                assert column.column_name == column_name
            assert 'stamp' not in records.data[0].columns
            assert 'stamp_' in records.data[0].columns
            assert 'stamp' not in records.data[1].columns
            assert 'stamp_' not in records.data[1].columns

            records.columns.get('stamp_')
            with pytest.raises(ItemNotFoundError):
                records.columns.get('stamp')

            assert 'aaa' not in records.data[0].columns
            assert 'aaa_' in records.data[0].columns
            assert 'aaa' not in records.data[1].columns
            assert 'aaa_' not in records.data[1].columns

            assert records.column_names == ['stamp_', 'aaa_']

            with pytest.raises(InvalidArgumentError):
                records.columns.rename({'stamp_': 'aaa_'})
            with pytest.raises(InvalidArgumentError):
                records.columns.rename({'not_exist': 'aaa_'})

    def test_rename_colums_validate_argument(self):
        records_py: Records = Records(
            [
                Record({'AAA': 0, 'BBB': 0}),
            ],
            [
                ColumnValue('AAA'),
                ColumnValue('BBB')
            ]
        )
        records_cpp = to_cpp_records(records_py)

        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            rename_keys = {'AAA': 'AAA_'}
            records.columns.rename(rename_keys)

            with pytest.raises(InvalidArgumentError):
                rename_keys = {'AAA': 'AAA'}
                records.columns.rename(rename_keys)

            # overwrite columns
            with pytest.raises(InvalidArgumentError):
                rename_keys = {'AAA': 'AAA_', 'BBB': 'AAA'}
                records.columns.rename(rename_keys)

            with pytest.raises(InvalidArgumentError):
                rename_keys = {'AAA': 'BBB', 'BBB': 'BBB_'}
                records.columns.rename(rename_keys)

            # Duplicate columns after change
            rename_keys = {'AAA': 'AAA_', 'BBB': 'AAA_'}
            with pytest.raises(InvalidArgumentError):
                records.columns.rename(rename_keys)

    def test_clone(self):
        records_py: Records = Records(
            [
                Record({'stamp': 0}),
            ],
            [
                ColumnValue('stamp')
            ]
        )
        records_cpp = to_cpp_records(records_py)
        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records_ = records.clone()
            with pytest.raises(ItemNotFoundError):
                records_.columns.get('aaa')
            records_.append_column(ColumnValue('aaa'), [9])
            records_.columns.get('aaa')
            assert records_.column_names == ['stamp', 'aaa']
            assert records.column_names == ['stamp']

    def test_filter_if(self):
        key = 'stamp'

        records_py: Records = Records(
            [
                Record({key: 0}),
                Record({key: 1}),
                Record({key: 2}),
            ],
            [
                ColumnValue(key)
            ]
        )
        records_cpp = to_cpp_records(records_py)
        for records, records_type in zip([records_py, records_cpp], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            assert len(records.data) == 3
            init_columns = records.columns

            records.filter_if(lambda record: record.get(key) == 1)
            assert init_columns == records.columns
            assert len(records.data) == 1
            assert records.data[0].get(key) == 1

            records.filter_if(lambda _: False)
            assert init_columns == records.columns
            assert len(records.data) == 0

    def test_concat(self):
        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records_left = records_type(
                [
                    record_type({'a': 0}),
                    record_type({'a': 1}),
                ],
                [
                    ColumnValue('a')
                ]
            )
            records_right = records_type(
                [
                    record_type({'b': 3}),
                    record_type({'b': 4}),
                    record_type({'b': 5}),
                ],
                [
                    ColumnValue('b')
                ]
            )

            records_expect = records_type(
                records_left.data + records_right.data,
                [
                    ColumnValue('a'),
                    ColumnValue('b')
                ]
            )

            records_concat = Records(None, [ColumnValue('a'), ColumnValue('b')])
            records_concat.concat(records_left)
            records_concat.concat(records_right)
            assert records_concat.equals(records_expect)

            records_concat = Records(None, [ColumnValue('a')])
            with pytest.raises(InvalidArgumentError):
                records_concat.concat(records_right)

    def test_bind_drop_as_delay(self):
        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records = records_type(
                [
                    record_type({'sort_key': 1, 'stamp': 0, 'stamp_': 1}),
                    record_type({'sort_key': 2, 'stamp': 2}),
                    record_type({'sort_key': 3, 'stamp_': 5}),
                    record_type({'sort_key': 4, 'stamp': 6}),
                    record_type({'sort_key': 5}),
                ],
                [
                    ColumnValue('sort_key'),
                    ColumnValue('stamp'),
                    ColumnValue('stamp_')
                ]
            )
            records.bind_drop_as_delay()
            records_expect = records_type(
                [
                    record_type({'sort_key': 1, 'stamp': 0, 'stamp_': 1}),
                    record_type({'sort_key': 2, 'stamp': 2, 'stamp_': 5}),
                    record_type({'sort_key': 3, 'stamp': 6, 'stamp_': 5}),
                    record_type({'sort_key': 4, 'stamp': 6}),
                    record_type({'sort_key': 5}),
                ],
                [
                    ColumnValue('sort_key'),
                    ColumnValue('stamp'),
                    ColumnValue('stamp_')
                ]
            )

            assert records.equals(records_expect)

    def test_to_dataframe(self):
        records_py: Records = Records(
            [
                Record({'a': 0}),
                Record({'b': 1, 'a': 3}),
                Record({'c': 2, 'a': 5}),
            ],
            [
                ColumnValue('b'),
                ColumnValue('a'),
                ColumnValue('c')
            ]
        )
        records_cpp = to_cpp_records(records_py)
        expect_dict = [record.data for record in records_py.data]
        expect_df = pd.DataFrame.from_dict(
            expect_dict, dtype='object'
        ).reindex(columns=['b', 'a', 'c'])

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            df = records.to_dataframe()
            assert df.equals(expect_df)

            column = records.columns.get('a')
            assert column.mapper is None

    def test_to_dataframe_with_column_mapper(self):
        mapper = ColumnMapper()
        mapper.add(0, 'test0')
        mapper.add(3, 'test3')

        records_py: Records = Records(
            [
                Record({'a': 0, 'b': 2}),
                Record({'a': 3}),
            ],
            [
                ColumnValue('a', mapper=mapper),
                ColumnValue('b')
            ]
        )
        records_cpp = to_cpp_records(records_py)
        expect_dict = [
            {'a': 'test0', 'b': 2},
            {'a': 'test3'}
        ]
        expect_df = pd.DataFrame.from_dict(
            expect_dict, dtype='object'
        ).reindex(columns=['a', 'b'])

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            column = records.columns.get('a')
            df = records.to_dataframe()
            assert df.equals(expect_df)
            assert column.mapper is not None

    def test_iter(self):
        records_py: Records = Records(
            [
                Record({'a': 0}),
                Record({'a': 3}),
                Record({'a': 5}),
            ],
            [ColumnValue('a')]
        )
        records_cpp = to_cpp_records(records_py)

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            for i, record in enumerate(records):
                assert record.get('a') == records_py.data[i].get('a')

    def test_get_column_series(self):
        records_py: Records = Records(
            [
                Record({'a': 0, 'b': 1}),
                Record({'b': 3}),
                Record({'a': 5}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )
        records_cpp = to_cpp_records(records_py)

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            assert records.get_column_series('a') == [0, None, 5]
            assert records.get_column_series('b') == [1, 3, None]

            with pytest.raises(InvalidArgumentError):
                records.get_column_series('x')

    def test_get_row_series(self):
        records_py: Records = Records(
            [
                Record({'a': 0, 'b': 1}),
                Record({'b': 3}),
                Record({'a': 5}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )
        records_cpp = to_cpp_records(records_py)

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            assert records.get_row_series(0).equals(records.data[0])
            assert records.get_row_series(1).equals(records.data[1])
            assert records.get_row_series(2).equals(records.data[2])

            with pytest.raises(InvalidArgumentError):
                records.get_row_series(100)

    def test_groupby_1key(self):
        records_py = Records(
            [
                Record({'a': 0, 'b': 1}),
                Record({'a': 0, 'b': 2}),
                Record({'a': 5, 'b': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )
        expect = {}
        expect[(0,)] = Records(
            [
                Record({'a': 0, 'b': 1}),
                Record({'a': 0, 'b': 2}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )

        expect[(5,)] = Records(
            [
                Record({'a': 5, 'b': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )

        group = records_py.groupby(['a'])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

        if CppImplEnabled:
            records_cpp = to_cpp_records(records_py)
            expect_cpp = {}
            for k, v in expect.items():
                expect_cpp[k] = to_cpp_records(v)
            group_cpp = records_cpp.groupby(['a'])

            assert group_cpp.keys() == expect_cpp.keys()

            for k, v in group_cpp.items():
                assert v.equals(expect_cpp[k])

    def test_groupby_1key_key_missing(self):
        records_py = Records(
            [
                Record({'a': 0, 'b': 1}),
                Record({'b': 2}),
                Record({'a': 5, 'b': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )
        expect = {}
        expect[(0,)] = Records(
            [
                Record({'a': 0, 'b': 1}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )

        expect[(5,)] = Records(
            [
                Record({'a': 5, 'b': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )
        expect[(2**64-1,)] = Records(
            [
                Record({'b': 2}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b')
            ]
        )

        group = records_py.groupby(['a'])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

        if CppImplEnabled:
            records_cpp = to_cpp_records(records_py)
            expect_cpp = {}
            for k, v in expect.items():
                expect_cpp[k] = to_cpp_records(v)
            group_cpp = records_cpp.groupby(['a'])

            assert group_cpp.keys() == expect_cpp.keys()

            for k, v in group_cpp.items():
                assert v.equals(expect_cpp[k])

    def test_groupby_2key(self):
        records_py = Records(
            [
                Record({'a': 0, 'b': 1, 'c': 2}),
                Record({'a': 0, 'b': 2, 'c': 3}),
                Record({'a': 5, 'b': 3, 'c': 3}),
                Record({'a': 5, 'b': 4, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )

        expect = {}
        expect[(0, 2)] = Records(
            [
                Record({'a': 0, 'b': 1, 'c': 2}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )
        expect[(0, 3)] = Records(
            [
                Record({'a': 0, 'b': 2, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )
        expect[(5, 3)] = Records(
            [
                Record({'a': 5, 'b': 3, 'c': 3}),
                Record({'a': 5, 'b': 4, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )

        group = records_py.groupby(['a', 'c'])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

        if CppImplEnabled:
            records_cpp = to_cpp_records(records_py)
            expect_cpp = {}
            for k, v in expect.items():
                expect_cpp[k] = to_cpp_records(v)
            group_cpp = records_cpp.groupby(['a', 'c'])

            assert group_cpp.keys() == expect_cpp.keys()

            for k, v in group_cpp.items():
                assert v.equals(expect_cpp[k])

    def test_groupby_3key(self):
        records_py = Records(
            [
                Record({'a': 0, 'b': 1, 'c': 2}),
                Record({'a': 0, 'b': 2, 'c': 3}),
                Record({'a': 5, 'b': 3, 'c': 3}),
                Record({'a': 5, 'b': 4, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )

        expect = {}
        expect[(0, 1, 2)] = Records(
            [
                Record({'a': 0, 'b': 1, 'c': 2}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )
        expect[(0, 2, 3)] = Records(
            [
                Record({'a': 0, 'b': 2, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )
        expect[(5, 3, 3)] = Records(
            [
                Record({'a': 5, 'b': 3, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )
        expect[(5, 4, 3)] = Records(
            [
                Record({'a': 5, 'b': 4, 'c': 3}),
            ],
            [
                ColumnValue('a'),
                ColumnValue('b'),
                ColumnValue('c')
            ]
        )

        group = records_py.groupby(['a', 'b', 'c'])

        assert group.keys() == expect.keys()
        for k, v in group.items():
            assert v.equals(expect[k])

        if CppImplEnabled:
            records_cpp = to_cpp_records(records_py)
            expect_cpp = {}
            for k, v in expect.items():
                expect_cpp[k] = to_cpp_records(v)
            group_cpp = records_cpp.groupby(['a', 'b', 'c'])

            assert group_cpp.keys() == expect_cpp.keys()

            for k, v in group_cpp.items():
                assert v.equals(expect_cpp[k])

    def test_append_column(self):
        expects_py = Records(
            [
                Record({'value': 0}),
            ],
            [ColumnValue('value')]
        )

        expects_cpp = to_cpp_records(expects_py)
        for expects, record_type, records_type in \
                zip([expects_py, expects_cpp], [Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue
            records = records_type([record_type()], [])
            assert records.columns == []
            records.append_column(ColumnValue('value'), [0])
            assert records.equals(expects)

    def test_get_column(self):
        records_py = Records(
            [
                Record({'value': 0}),
            ],
            [ColumnValue('value')]
        )

        test_records = [records_py]
        if CppImplEnabled:
            test_records.append(to_cpp_records(records_py))

        for records in test_records:
            column = records.columns.get('value')
            assert column.column_name == 'value'
            with pytest.raises(ItemNotFoundError):
                records.columns.get('not exist')

    def test_sort(self):
        key = 'stamp'

        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records = records_type(
                [
                    record_type({key: 2}),
                    record_type({key: 0}),
                    record_type({key: 1}),
                ],
                [ColumnValue(key)]
            )
            records_asc = records_type(
                [
                    record_type({key: 0}),
                    record_type({key: 1}),
                    record_type({key: 2}),
                ],
                [ColumnValue(key)]
            )
            records_desc = records_type(
                [
                    record_type({key: 2}),
                    record_type({key: 1}),
                    record_type({key: 0}),
                ],
                [ColumnValue(key)]
            )
            records_ = records.clone()
            records_.sort(key, ascending=True)
            assert records_.equals(records_asc)

            records_ = records.clone()
            records_.sort(key, ascending=False)
            assert records_.equals(records_desc)

    def test_sort_column_order(self):
        key = 'stamp'
        key_ = 'stamp_'
        key__ = 'stamp__'

        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records = records_type(
                [
                    record_type({key: 2, key_: 2, key__: 6}),
                    record_type({key: 2, key_: 2}),
                    record_type({key: 1, key__: 5}),
                    record_type({key: 2, key_: 1, key__: 5}),
                    record_type({key: 0, key_: 5, key__: 5}),
                    record_type({key: 1}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(key_),
                    ColumnValue(key__)
                ]
            )
            records_asc = records_type(
                [
                    record_type({key: 0, key_: 5, key__: 5}),
                    record_type({key: 1, key__: 5}),
                    record_type({key: 1}),
                    record_type({key: 2, key_: 1, key__: 5}),
                    record_type({key: 2, key_: 2, key__: 6}),
                    record_type({key: 2, key_: 2}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(key_),
                    ColumnValue(key__)
                ]
            )
            records_desc = records_type(
                [
                    record_type({key: 2, key_: 2}),
                    record_type({key: 2, key_: 2, key__: 6}),
                    record_type({key: 2, key_: 1, key__: 5}),
                    record_type({key: 1}),
                    record_type({key: 1, key__: 5}),
                    record_type({key: 0, key_: 5, key__: 5}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(key_),
                    ColumnValue(key__)
                ]
            )
            records_ = records.clone()
            records_.sort(records_.column_names, ascending=True)
            assert records_.equals(records_asc)

            records_ = records.clone()
            records_.sort(records_.column_names, ascending=False)
            assert records_.equals(records_desc)

    def test_sort_with_multi_keys(self):
        key = 'stamp'
        sub_key = 'stamp_'

        for record_type, records_type in zip([Record, RecordCppImpl], [Records, RecordsCppImpl]):
            if records_type == RecordsCppImpl and not CppImplEnabled:
                continue

            records = records_type(
                [
                    record_type({key: 2, sub_key: 2}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 0, sub_key: 5}),
                    record_type({key: 1, sub_key: 3}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(sub_key)
                ]
            )
            records_asc = records_type(
                [
                    record_type({key: 0, sub_key: 5}),
                    record_type({key: 1, sub_key: 3}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 2, sub_key: 2}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(sub_key)
                ]
            )
            records_desc = records_type(
                [
                    record_type({key: 2, sub_key: 2}),
                    record_type({key: 2, sub_key: 1}),
                    record_type({key: 1, sub_key: 3}),
                    record_type({key: 0, sub_key: 5}),
                ],
                [
                    ColumnValue(key),
                    ColumnValue(sub_key)
                ]
            )
            records_ = records.clone()
            records_.sort([key, sub_key], ascending=True)
            assert records_.equals(records_asc)

            records_ = records.clone()
            records_.sort([key, sub_key], ascending=False)
            assert records_.equals(records_desc)

    @pytest.mark.parametrize(
        'records_py, records_py_, expect',
        [
            (
                Records(
                    [
                        Record({'stamp': 0}),
                        Record({'stamp': 1}),
                    ],
                    [ColumnValue('stamp')]
                ),
                Records(
                    [
                        Record({'stamp': 0}),
                        Record({'stamp': 1}),
                    ],
                    [ColumnValue('stamp')]
                ),
                True,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 5, 'stamp_': 6}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                Records(
                    [
                        Record({'stamp_': 1, 'stamp': 0}),
                        Record({'stamp_': 6, 'stamp': 5}),
                    ], [ColumnValue('stamp_'), ColumnValue('stamp')]
                ),
                False,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 5, 'stamp_': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                Records(
                    [
                        Record({'stamp_': 1, 'stamp': 0}),
                        Record({'stamp_': 6, 'stamp': 5}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                False,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 5, 'stamp_': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_'), ColumnValue('stamp__')]
                ),
                Records(
                    [
                        Record({'stamp_': 1, 'stamp': 0}),
                        Record({'stamp_': 6, 'stamp': 5}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                False,
            ),
            (
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                Records(
                    [
                        Record({'stamp': 0, 'stamp_': 1}),
                        Record({'stamp': 0, 'stamp_': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('stamp_')]
                ),
                False,
            ),
        ],
    )
    def test_equals(self, records_py, records_py_, expect):
        records_cpp = to_cpp_records(records_py)
        records_cpp_ = to_cpp_records(records_py_)

        for records, records_ in zip([records_py, records_cpp], [records_py_, records_cpp_]):
            if records is None and not CppImplEnabled:
                continue

            assert records.equals(records_) is expect

    def test_reindex(self):
        records_py = Records(
            [
                Record({'a': 1, 'b': 3}),
                Record({'b': 4, 'c': 5}),
            ],
            [
                ColumnValue('b'),
                ColumnValue('c'),
                ColumnValue('d'),
                ColumnValue('a')
            ]
        )
        records_cpp = to_cpp_records(records_py)

        for records in [records_py, records_cpp]:
            if records is None and not CppImplEnabled:
                continue

            assert records.column_names == ['b', 'c', 'd', 'a']
            records.columns.reindex(['a', 'b', 'c', 'd'])
            assert records.column_names == ['a', 'b', 'c', 'd']

            with pytest.raises(InvalidArgumentError):
                records.columns.reindex(['a', 'b', 'c', 'd', 'e'])
            with pytest.raises(InvalidArgumentError):
                records.columns.reindex(['a', 'b', 'c'])

    @pytest.mark.parametrize(
        'how, records_expect_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'value_left': 10, 'value_right': 10,
                               'stamp': 1, 'stamp_': 2}),
                        Record({'value_left': 20, 'value_right': 20,
                               'stamp': 3, 'stamp_': 4}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 9, 'stamp_': 11}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 10, 'stamp_': 11}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'value_left': 10, 'value_right': 10,
                               'stamp': 1, 'stamp_': 2}),
                        Record({'value_left': 20, 'value_right': 20,
                               'stamp': 3, 'stamp_': 4}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 9, 'stamp_': 11}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 10, 'stamp_': 11}),
                        Record({'value_left': 40, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'value_left': 10, 'value_right': 10,
                               'stamp': 1, 'stamp_': 2}),
                        Record({'value_left': 20, 'value_right': 20,
                               'stamp': 3, 'stamp_': 4}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 9, 'stamp_': 11}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 10, 'stamp_': 11}),
                        Record({'value_right': 50, 'stamp_': 10}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'value_left': 10, 'value_right': 10,
                               'stamp': 1, 'stamp_': 2}),
                        Record({'value_left': 20, 'value_right': 20,
                               'stamp': 3, 'stamp_': 4}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 30, 'value_right': 30,
                               'stamp': 5, 'stamp_': 6}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 9, 'stamp_': 11}),
                        Record({'value_left': 70, 'value_right': 70,
                               'stamp': 10, 'stamp_': 11}),
                        Record({'value_left': 40, 'stamp': 7}),
                        Record({'value_right': 50, 'stamp_': 10}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
        ],
    )
    def test_merge(self, how: str, records_expect_py: Records):
        records_left_py: Records = Records(
            [
                Record({'stamp': 1, 'value_left': 10}),
                Record({'stamp': 3, 'value_left': 20}),
                Record({'stamp': 5, 'value_left': 30}),
                Record({'stamp': 7, 'value_left': 40}),
                Record({'stamp': 9, 'value_left': 70}),
                Record({'stamp': 10, 'value_left': 70}),
            ],
            [
                ColumnValue('stamp', []),
                ColumnValue('value_left', []),
            ]
        )

        records_right_py: Records = Records(
            [
                Record({'stamp_': 2, 'value_right': 10}),
                Record({'stamp_': 4, 'value_right': 20}),
                Record({'stamp_': 6, 'value_right': 30}),
                Record({'stamp_': 6, 'value_right': 30}),
                Record({'stamp_': 10, 'value_right': 50}),
                Record({'stamp_': 11, 'value_right': 70}),
            ],
            [
                ColumnValue('stamp_', []),
                ColumnValue('value_right', []),
            ]
        )

        records_left_cpp = to_cpp_records(records_left_py)
        records_right_cpp = to_cpp_records(records_right_py)
        records_expect_cpp = to_cpp_records(records_expect_py)

        column_names_expect = [
            'stamp',
            'value_left',
            'stamp_',
            'value_right'
        ]
        for records_left, records_right, records_expect in zip(
            [records_left_py, records_left_cpp],
            [records_right_py, records_right_cpp],
            [records_expect_py, records_expect_cpp],
        ):
            if records_left is None or records_right is None:
                continue

            merged = records_left.merge(
                records_right, 'value_left', 'value_right', how=how)

            assert merged.equals(records_expect)
            assert merged.column_names == column_names_expect
            assert records_left.column_names == ['stamp', 'value_left']
            assert records_right.column_names == ['stamp_', 'value_right']

            with pytest.raises(InvalidArgumentError):
                records_left.merge(
                    records_right,
                    'value_left',
                    'not_exist',
                    how=how)

    @pytest.mark.parametrize(
        'how, records_expect_py',
        [
            (
                'inner',
                Records(
                    [
                        Record(
                            {'k0': 1, 'k1': 10, 'k0_': 1, 'k1_': 10, 'value': 100, 'value_': 110}
                        ),
                        Record(
                            {'k0': 3, 'k1': 20, 'k0_': 3, 'k1_': 20, 'value': 200, 'value_': 210}
                        ),
                        Record(
                            {'k0': 3, 'k1': 20, 'k0_': 3, 'k1_': 20, 'value': 200, 'value_': 310}
                        ),
                    ],
                    [
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('value'),
                        ColumnValue('k0_'),
                        ColumnValue('k1_'),
                        ColumnValue('value_')
                    ]
                ),
            ),
        ],
    )
    def test_merge_multiple_columns(self, how: str, records_expect_py: Records):
        records_left_py: Records = Records(
            [
                Record({'k0': 1, 'k1': 10, 'value': 100}),
                Record({'k0': 3, 'k1': 20, 'value': 200}),
                Record({'k0': 5, 'k1': 30, 'value': 300}),
                Record({'k0': 7, 'k1': 40, 'value': 400}),
                Record({'k0': 9, 'k1': 70}),
                Record({'value': 600}),
            ],
            [
                ColumnValue('k0'),
                ColumnValue('k1'),
                ColumnValue('value'),
            ]
        )

        records_right_py: Records = Records(
            [
                Record({'k0_': 1, 'k1_': 10, 'value_': 110}),
                Record({'k0_': 3, 'k1_': 20, 'value_': 210}),
                Record({'k0_': 3, 'k1_': 20, 'value_': 310}),
                Record({'k0_': 9, 'value_': 410}),
                Record({'k1_': 50}),
                Record({'value_': 610}),
            ],
            [
                ColumnValue('k0_'),
                ColumnValue('k1_'),
                ColumnValue('value_'),
            ]
        )

        records_left_cpp = to_cpp_records(records_left_py)
        records_right_cpp = to_cpp_records(records_right_py)
        records_expect_cpp = to_cpp_records(records_expect_py)

        column_names_expect = [
            'k0',
            'k1',
            'value',
            'k0_',
            'k1_',
            'value_',
        ]
        for records_left, records_right, records_expect in zip(
            [records_left_py, records_left_cpp],
            [records_right_py, records_right_cpp],
            [records_expect_py, records_expect_cpp],
        ):
            if records_left is None or records_right is None:
                continue

            merged = records_left.merge(records_right, ['k0', 'k1'], ['k0_', 'k1_'], how=how)

            assert merged.equals(records_expect)
            assert merged.column_names == column_names_expect
            assert records_left.column_names == ['k0', 'k1', 'value']
            assert records_right.column_names == ['k0_', 'k1_', 'value_']

    @pytest.mark.parametrize(
        'how, records_expect_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'k0': 1, 'k1': 10, 'value': 100, 'value_': 110}),
                        Record({'k0': 3, 'k1': 20, 'value': 200, 'value_': 210}),
                        Record({'k0': 3, 'k1': 20, 'value': 200, 'value_': 310}),
                    ],
                    [
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('value'),
                        ColumnValue('value_')
                    ]
                ),
            ),
        ],
    )
    def test_merge_multiple_columns_duplicated_key(self, how: str, records_expect_py: Records):
        records_left_py: Records = Records(
            [
                Record({'k0': 1, 'k1': 10, 'value': 100}),
                Record({'k0': 3, 'k1': 20, 'value': 200}),
                Record({'k0': 5, 'k1': 30, 'value': 300}),
                Record({'k0': 7, 'k1': 40, 'value': 400}),
                Record({'k0': 9, 'k1': 70}),
                Record({'value': 600}),
            ],
            [
                ColumnValue('k0'),
                ColumnValue('k1'),
                ColumnValue('value'),
            ]
        )

        records_right_py: Records = Records(
            [
                Record({'k0': 1, 'k1': 10, 'value_': 110}),
                Record({'k0': 3, 'k1': 20, 'value_': 210}),
                Record({'k0': 3, 'k1': 20, 'value_': 310}),
                Record({'k0': 9, 'value_': 410}),
                Record({'k1': 50}),
                Record({'value_': 610}),
            ],
            [
                ColumnValue('k0'),
                ColumnValue('k1'),
                ColumnValue('value_'),
            ]
        )

        records_left_cpp = to_cpp_records(records_left_py)
        records_right_cpp = to_cpp_records(records_right_py)
        records_expect_cpp = to_cpp_records(records_expect_py)

        column_names_expect = [
            'k0',
            'k1',
            'value',
            'value_',
        ]
        for records_left, records_right, records_expect in zip(
            [records_left_py, records_left_cpp],
            [records_right_py, records_right_cpp],
            [records_expect_py, records_expect_cpp],
        ):
            if records_left is None or records_right is None:
                continue

            merged = records_left.merge(
                records_right,
                ['k0', 'k1'],
                ['k0', 'k1'],
                how=how)

            assert merged.equals(records_expect)
            assert merged.column_names == column_names_expect
            assert records_left.column_names == ['k0', 'k1', 'value']
            assert records_right.column_names == ['k0', 'k1', 'value_']

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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        Record({'other_stamp_': 10}),
                        Record({'other_stamp_': 14}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                        Record({'other_stamp_': 10}),
                        Record({'other_stamp_': 14}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
        ],
    )
    def test_merge_with_loss(self, how, expect_records_py):
        left_records_py = Records(
            [
                Record({'other_stamp': 4, 'stamp': 1, 'value_left': 1}),
                Record({'other_stamp': 8}),
                Record({'other_stamp': 12, 'stamp': 9, 'value_left': 2}),
                Record({'other_stamp': 16}),
            ],
            [
                ColumnValue('other_stamp'),
                ColumnValue('stamp'),
                ColumnValue('value_left'),
            ]
        )

        right_records_py = Records(
            [
                Record({'other_stamp_': 2, 'stamp_': 3, 'value_right': 2}),
                Record({'other_stamp_': 6, 'stamp_': 7, 'value_right': 1}),
                Record({'other_stamp_': 10}),
                Record({'other_stamp_': 14}),
            ],
            [
                ColumnValue('other_stamp_'),
                ColumnValue('stamp_'),
                ColumnValue('value_right'),
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
            if left_records is None and not CppImplEnabled:
                continue

                # columns=['other_stamp', 'other_stamp_', 'stamp',
                #          'stamp_', 'value_left', 'value_right'],
            merged = merge(
                left_records=left_records,
                right_records=right_records,
                join_left_key='value_left',
                join_right_key='value_right',
                how=how
            )

            assert merged.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records_py',
        [
            (
                'inner',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key_left': 1, 'stamp': 6}),
                        Record({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 4}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 5}),
                        Record({'key_left': 1, 'stamp': 6}),
                        Record({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key_right': 1, 'sub_stamp': 4}),
                        Record({'key_right': 2, 'sub_stamp': 5}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key_right': 1, 'sub_stamp': 4}),
                        Record({'key_right': 2, 'sub_stamp': 5}),
                        Record({'key_left': 1, 'stamp': 6}),
                        Record({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_key(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'key_left': 1, 'stamp': 0}),
                Record({'key_left': 2, 'stamp': 1}),
                Record({'key_left': 1, 'stamp': 6}),
                Record({'key_left': 2, 'stamp': 7}),
            ],
            [
                ColumnValue('key_left'),
                ColumnValue('stamp')
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'key_right': 2, 'sub_stamp': 2}),
                Record({'key_right': 1, 'sub_stamp': 3}),
                Record({'key_right': 1, 'sub_stamp': 4}),
                Record({'key_right': 2, 'sub_stamp': 5}),
            ],
            [
                ColumnValue('key_right'),
                ColumnValue('sub_stamp')
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged_records = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key='key_left',
                join_right_key='key_right',
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
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                    ],
                    [
                        ColumnValue('key'),
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp')
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
                    ],
                    [
                        ColumnValue('key'),
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        Record({'key': 1, 'stamp': 0, 'sub_stamp': 4}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        Record({'key': 2, 'stamp': 1, 'sub_stamp': 5}),
                        Record({'key': 1, 'stamp': 6}),
                        Record({'key': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key'),
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp')
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
                    ],
                    [
                        ColumnValue('key'),
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp')
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
                    ],
                    [
                        ColumnValue('key'),
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_same_key(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'key': 1, 'stamp': 0}),
                Record({'key': 2, 'stamp': 1}),
                Record({'key': 1, 'stamp': 6}),
                Record({'key': 2, 'stamp': 7}),
            ],
            [
                ColumnValue('key'),
                ColumnValue('stamp'),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'key': 2, 'sub_stamp': 2}),
                Record({'key': 1, 'sub_stamp': 3}),
                Record({'key': 1, 'sub_stamp': 4}),
                Record({'key': 2, 'sub_stamp': 5}),
            ],
            [
                ColumnValue('key'),
                ColumnValue('sub_stamp'),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged_records = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key='key',
                join_right_key='key',
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
                        Record({'stamp': 0, 'k0': 0, 'k1': 1, 'sub_stamp': 1}),
                        Record({'stamp': 5, 'k0': 2, 'k1': 3, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'stamp': 0, 'k0': 0, 'k1': 1, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'k0': 1}),
                        Record({'stamp': 4, 'k0': 2, 'k1': 3}),
                        Record({'stamp': 5, 'k0': 2, 'k1': 3, 'sub_stamp': 7}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record({'stamp': 0, 'k0': 0, 'k1': 1, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'k0': 1}),
                        Record({'stamp': 4, 'k0': 2, 'k1': 3}),
                        Record({'stamp': 5, 'k0': 2, 'k1': 3, 'sub_stamp': 7}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'stamp': 0, 'k0': 0, 'k1': 1, 'sub_stamp': 1}),
                        Record({'stamp': 5, 'k0': 2, 'k1': 3, 'sub_stamp': 7}),
                        Record({'sub_stamp': 6, 'k1': 3}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'stamp': 0, 'k0': 0, 'k1': 1, 'sub_stamp': 1}),
                        Record({'stamp': 3, 'k0': 1}),
                        Record({'stamp': 4, 'k0': 2, 'k1': 3}),
                        Record({'stamp': 5, 'k0': 2, 'k1': 3, 'sub_stamp': 7}),
                        Record({'sub_stamp': 6, 'k1': 3}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('k0'),
                        ColumnValue('k1'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_multi_join_key(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'stamp': 0, 'k0': 0, 'k1': 1}),
                Record({'stamp': 3, 'k0': 1}),
                Record({'stamp': 4, 'k0': 2, 'k1': 3}),
                Record({'stamp': 5, 'k0': 2, 'k1': 3}),
                Record({'stamp': 8}),
            ],
            [
                ColumnValue('stamp'),
                ColumnValue('k0'),
                ColumnValue('k1'),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'sub_stamp': 1, 'k0': 0, 'k1': 1}),
                Record({'sub_stamp': 6, 'k1': 3}),
                Record({'sub_stamp': 7, 'k0': 2, 'k1': 3}),
            ],
            [
                ColumnValue('sub_stamp'),
                ColumnValue('k0'),
                ColumnValue('k1'),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key=['k0', 'k1'],
                join_right_key=['k0', 'k1'],
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
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 5, 'sub_stamp': 6}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3}),
                        Record({'stamp': 4}),
                        Record({'stamp': 5, 'sub_stamp': 6}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3}),
                        Record({'stamp': 4}),
                        Record({'stamp': 5, 'sub_stamp': 6}),
                        Record({'stamp': 5, 'sub_stamp': 7}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 5, 'sub_stamp': 6}),
                        Record({'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'stamp': 0, 'sub_stamp': 1}),
                        Record({'stamp': 3}),
                        Record({'stamp': 4}),
                        Record({'stamp': 5, 'sub_stamp': 6}),
                        Record({'sub_stamp': 7}),
                        Record({'stamp': 8}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('sub_stamp'),
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
            ],
            [
                ColumnValue('stamp'),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'sub_stamp': 1}),
                Record({'sub_stamp': 6}),
                Record({'sub_stamp': 7}),
            ],
            [
                ColumnValue('sub_stamp'),
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key=None,
                join_right_key=None,
                how=how,
            )

            with pytest.raises(InvalidArgumentError):
                left_records.merge_sequencial(
                    right_records=right_records,
                    left_stamp_key='stamp',
                    right_stamp_key='not_exist',
                    join_left_key=None,
                    join_right_key=None,
                    how=how,
                )

            with pytest.raises(InvalidArgumentError):
                left_records.merge_sequencial(
                    right_records=right_records,
                    left_stamp_key='stamp',
                    right_stamp_key='sub_stamp',
                    join_left_key='not_exist',
                    join_right_key=None,
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
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key_left': 1, 'stamp': 4}),
                        Record({'key_left': 1, 'stamp': 5}),
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1,
                               'stamp': 0, 'sub_stamp': 2}),
                        Record({'key_left': 1, 'key_right': 1,
                               'stamp': 0, 'sub_stamp': 3}),
                        Record({'key_left': 1, 'stamp': 4}),
                        Record({'key_left': 1, 'stamp': 5}),
                        Record({'key_left': 1, 'key_right': 1,
                               'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'right',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key_right': 3, 'sub_stamp': 1}),
                        Record({'key_right': 1, 'sub_stamp': 3}),
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
            (
                'outer',
                Records(
                    [
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        Record({'key_right': 3, 'sub_stamp': 1}),
                        Record({'key_right': 1, 'sub_stamp': 3}),
                        Record({'key_left': 1, 'stamp': 4}),
                        Record({'key_left': 1, 'stamp': 5}),
                        Record({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp')
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_drop(self, how, expect_records_py):
        left_records_py: Records = Records(
            [
                Record({'key_left': 1, 'stamp': 0}),
                Record({'key_left': 1, 'stamp': 4}),
                Record({'key_left': 1, 'stamp': 5}),
                Record({'key_left': 1, 'stamp': 6}),
            ],
            [
                ColumnValue('key_left'),
                ColumnValue('stamp'),
            ]
        )

        right_records_py: Records = Records(
            [
                Record({'key_right': 3, 'sub_stamp': 1}),
                Record({'key_right': 1, 'sub_stamp': 2}),
                Record({'key_right': 1, 'sub_stamp': 3}),
                Record({'key_right': 1, 'sub_stamp': 7}),
            ],
            [
                ColumnValue('key_right'),
                ColumnValue('sub_stamp')
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key='key_left',
                join_right_key='key_right',
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
            (
                'left_use_latest',
                Records(
                    [
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp_': 6,
                                'stamp_': 7,
                                'value_right': 1,
                            }
                        ),
                        Record({'other_stamp_': 10, 'stamp_': 10}),
                        Record({'other_stamp_': 14, 'value_right': 2}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
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
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        Record(
                            {
                                'other_stamp_': 6,
                                'stamp_': 7,
                                'value_right': 1,
                            }
                        ),
                        Record({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        Record({'other_stamp_': 10, 'stamp_': 10}),
                        Record({'other_stamp': 8}),
                        Record({'other_stamp': 16}),
                        Record({'other_stamp_': 14, 'value_right': 2}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right')
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequencial_with_loss(self, how, expect_records_py):
        left_records_py = Records(
            [
                Record({'other_stamp': 4, 'stamp': 1, 'value_left': 1}),
                Record({'other_stamp': 8}),
                Record({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                Record({'other_stamp': 16}),
            ],
            [
                ColumnValue('other_stamp'),
                ColumnValue('stamp'),
                ColumnValue('value_left'),
            ]
        )

        right_records_py = Records(
            [
                Record({'other_stamp_': 2, 'stamp_': 3, 'value_right': 1}),
                Record({'other_stamp_': 6, 'stamp_': 7, 'value_right': 1}),
                Record({'other_stamp_': 10, 'stamp_': 10}),
                Record({'other_stamp_': 14, 'value_right': 2}),
            ],
            [
                ColumnValue('other_stamp_'),
                ColumnValue('stamp_'),
                ColumnValue('value_right')
            ]
        )

        for left_records, right_records, expect_records in zip(
            [left_records_py, to_cpp_records(left_records_py)],
            [right_records_py, to_cpp_records(right_records_py)],
            [expect_records_py, to_cpp_records(expect_records_py)],
        ):
            if left_records is None and not CppImplEnabled:
                continue

            merged = left_records.merge_sequencial(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='stamp_',
                join_left_key='value_left',
                join_right_key='value_right',
                how=how,
            )

            assert merged.equals(expect_records) is True

    def test_merge_sequencial_for_addr_track(self):
        source_records: Records = Records(
            [
                Record({'source_addr': 1, 'source_stamp': 0}),
                Record({'source_addr': 1, 'source_stamp': 10}),
                Record({'source_addr': 3, 'source_stamp': 20}),
            ],
            [
                ColumnValue('source_addr'),
                ColumnValue('source_stamp')
            ]
        )

        copy_records: Records = Records(
            [
                Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 1}),
                Record({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 11}),
                Record({'addr_from': 3, 'addr_to': 13, 'copy_stamp': 21}),
                Record({'addr_from': 13, 'addr_to': 23, 'copy_stamp': 22}),
            ],
            [
                ColumnValue('addr_from'),
                ColumnValue('addr_to'),
                ColumnValue('copy_stamp')
            ]
        )

        sink_records: Records = Records(
            [
                Record({'sink_addr': 13, 'sink_stamp': 2}),
                Record({'sink_addr': 1, 'sink_stamp': 3}),
                Record({'sink_addr': 13, 'sink_stamp': 12}),
                Record({'sink_addr': 13, 'sink_stamp': 23}),
                Record({'sink_addr': 3, 'sink_stamp': 24}),
                Record({'sink_addr': 13, 'sink_stamp': 25}),
                Record({'sink_addr': 3, 'sink_stamp': 26}),
                Record({'sink_addr': 23, 'sink_stamp': 27}),
            ],
            [
                ColumnValue('sink_addr'),
                ColumnValue('sink_stamp')
            ]
        )

        expect_records = Records(
            [
                Record({
                    'source_addr': 1, 'source_stamp': 0, 'sink_stamp': 2}),
                Record({
                    'source_addr': 1, 'source_stamp': 0, 'sink_stamp': 3,
                }),
                Record({
                    'source_addr': 1, 'source_stamp': 10, 'sink_stamp': 12,
                }),
                Record({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 23,
                }),
                Record({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 24,
                }),
                Record({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 27,
                }),
            ],
            [
                ColumnValue('source_addr'),
                ColumnValue('source_stamp'),
                ColumnValue('sink_stamp')
            ]
        )

        from copy import deepcopy

        for source_records, copy_records, sink_records, expect_records in zip(
            [source_records, to_cpp_records(source_records)],
            [copy_records, to_cpp_records(copy_records)],
            [sink_records, to_cpp_records(sink_records)],
            [expect_records, to_cpp_records(expect_records)],
        ):
            if source_records is None and not CppImplEnabled:
                continue

            valid_args = {
                'source_stamp_key': 'source_stamp',
                'source_key': 'source_addr',
                'copy_stamp_key': 'copy_stamp',
                'copy_from_key': 'addr_from',
                'copy_to_key': 'addr_to',
                'sink_stamp_key': 'sink_stamp',
                'sink_from_key': 'sink_addr'
            }

            merged = source_records.merge_sequencial_for_addr_track(
                copy_records=copy_records,
                sink_records=sink_records,
                **valid_args
            )

            merged.sort(key='sink_stamp')
            expect_records.sort(key='sink_stamp')

            assert source_records.column_names == ['source_addr', 'source_stamp']
            assert copy_records.column_names == ['addr_from', 'addr_to', 'copy_stamp']
            assert sink_records.column_names == ['sink_addr', 'sink_stamp']

            assert merged.equals(expect_records)

            for key in valid_args:
                invalid_args = deepcopy(valid_args)
                invalid_args[key] = 'not_exist'

                with pytest.raises(InvalidArgumentError):
                    source_records.merge_sequencial_for_addr_track(
                                    copy_records=copy_records,
                                    sink_records=sink_records,
                                    **invalid_args
                                )
