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

from __future__ import annotations

from copy import deepcopy
from typing import Any

from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.record.column import ColumnValue
from caret_analyze.record.record_operations import merge

import pandas as pd
import pytest

RecordCppImpl: Any
RecordsCppImpl: Any

try:
    import caret_analyze.record.record_cpp_impl as cpp_impl

    RecordCppImpl = cpp_impl.RecordCppImpl
    RecordsCppImpl = cpp_impl.RecordsCppImpl
    CppImplEnabled = True
except ModuleNotFoundError as e:
    raise ModuleNotFoundError(
        'Failed to load RecordsCppImpl, ',
        'possibly due to missing information in the PYTHONPATH environment variable.') from e


class TestRecord:

    @pytest.mark.parametrize(
        'record, record_, expect',
        [
            (RecordCppImpl({'stamp': 0}), RecordCppImpl({'stamp': 0}), True),
            (RecordCppImpl({'stamp': 0}), RecordCppImpl({'stamp': 1}), False),
            (RecordCppImpl({'stamp': 0, 'value': 1}), RecordCppImpl(
                {'stamp': 0, 'value': 1}), True),
            (RecordCppImpl({'stamp': 0, 'value': 1}), RecordCppImpl(
                {'value': 1, 'stamp': 0}), True),
            (RecordCppImpl({'stamp': 0, 'value': 1}), RecordCppImpl(
                {'stamp': 1, 'value': 0}), False),
        ],
    )
    def test_equals(self, record, record_, expect):
        assert record.equals(record_) is expect

    def test_data(self):
        dic = {'stamp': 0, 'value': 18446744073709551615}
        record = RecordCppImpl(dic)
        assert record.data == dic

    def test_columns(self):
        dic = {'stamp': 0, 'value': 1}
        record = RecordCppImpl(dic)
        assert record.columns == set(dic.keys())

    def test_get(self):
        dic = {'stamp': 0, 'value': 1}
        record = RecordCppImpl(dic)
        assert record.get('stamp') == 0
        assert record.get('value') == 1

    def test_get_with_default(self):
        dic = {'stamp': 0}
        record = RecordCppImpl(dic)
        assert record.get_with_default('stamp', 0) == 0
        assert record.get_with_default('value', 0) == 0
        assert record.get_with_default('value', 1) == 1

    def test_add(self):
        expect = {'stamp': 0, 'value': 1}

        record = RecordCppImpl()
        record.add('stamp', 0)
        record.add('value', 1)
        assert record.data == expect
        assert record.columns == expect.keys()

    def test_change_dict_key(self):
        record = RecordCppImpl({'key_before': 0, 'value': 1})
        expect = RecordCppImpl({'key_after': 0, 'value': 1})

        record.change_dict_key('key_before', 'key_after')
        assert record.equals(expect)
        assert record.columns == expect.columns

    def test_drop_columns(self):
        dic = {'stamp': 0, 'value': 1}
        record = RecordCppImpl(dic)
        assert record.columns == set(dic.keys())

        record.drop_columns(['stamp'])
        assert record.columns == {'value'}

    def test_merge(self):
        left_dict = {'a': 1, 'b': 2}
        right_dict = {'c': 3, 'd': 4}
        merged_dict = deepcopy(left_dict)
        merged_dict.update(right_dict)

        left = RecordCppImpl(left_dict)
        right = RecordCppImpl(right_dict)

        left.merge(right)
        assert left.data == merged_dict
        assert right.data == right_dict


class TestRecords:

    def test_init(self):
        RecordsCppImpl()
        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl([RecordCppImpl({'stamp': 1})], None)

        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl(None, [ColumnValue('stamp'), ColumnValue('stamp')])

    def test_columns(self):
        columns = ['stamp', 'value']
        column_values = [ColumnValue(c) for c in columns]
        records = RecordsCppImpl([], column_values)
        assert records.columns == columns

    def test_data(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0, 'value': 1}),
                RecordCppImpl({'stamp': 2, 'value': 4}),
            ],
            [ColumnValue('stamp'), ColumnValue('value')]
        )
        records = RecordsCppImpl(expects.data, expects._columns.to_value())
        assert records.equals(expects)

    def test_len(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0, 'value': 1}),
                RecordCppImpl({'stamp': 2, 'value': 4})
            ],
            [ColumnValue('stamp'), ColumnValue('value')]
        )
        assert len(expects) == 2

    def test_append(self):
        stamp = 'stamp'
        value = 'value'
        expects = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1}),
                RecordCppImpl({stamp: 2, value: 4}),
                RecordCppImpl({stamp: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value)]
        )
        records = RecordsCppImpl(None, [ColumnValue(stamp), ColumnValue(value)])
        records.append(expects.data[0])
        records.append(expects.data[1])
        records.append(expects.data[2])
        assert records.equals(expects)
        assert records._columns.to_value() == expects._columns.to_value()

        # tests for multimethod
        records = RecordsCppImpl(None, [ColumnValue(stamp), ColumnValue(value)])
        records.append(expects.data[0].data)
        records.append(expects.data[1].data)
        records.append(expects.data[2].data)
        assert records.equals(expects)
        assert records.columns == expects.columns

    def test_drop_columns(self):
        key = 'stamp'
        value = 'value'
        records = RecordsCppImpl(
            [
                RecordCppImpl({key: 0, value: 3}),
                RecordCppImpl({key: 1, value: 4}),
                RecordCppImpl({key: 2, value: 5}),
            ], [ColumnValue(key), ColumnValue(value)]
        )

        columns_expect = [value]

        drop_keys = [key]
        records.drop_columns(drop_keys)
        for record in records.data:
            assert key not in record.columns
        assert records.columns == columns_expect
        for datum in records.data:
            assert datum.columns == set(columns_expect)
        assert records.data[0].get(value) == 3
        assert records.data[1].get(value) == 4
        assert records.data[2].get(value) == 5

        records_empty = RecordsCppImpl()
        assert records_empty.columns == []
        records_empty.drop_columns(drop_keys)
        assert records_empty.columns == []
        assert len(records_empty.data) == 0

        with pytest.raises(InvalidArgumentError):
            records.drop_columns('')

    def test_rename_columns(self):
        stamp = 'stamp'
        stamp_ = 'stamp_'
        value = 'value'
        value_ = 'value_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1}),
                RecordCppImpl(),
            ], [ColumnValue(stamp), ColumnValue(value)]
        )

        assert stamp in records.data[0].columns
        assert stamp_ not in records.data[0].columns
        assert stamp not in records.data[1].columns
        assert stamp_ not in records.data[1].columns

        assert value in records.data[0].columns
        assert value_ not in records.data[0].columns
        assert value not in records.data[1].columns
        assert value_ not in records.data[1].columns

        assert records.columns == [stamp, value]

        rename_keys = {
            value: value_,
            stamp: stamp_,
        }

        records.rename_columns(rename_keys)
        assert stamp not in records.data[0].columns
        assert stamp_ in records.data[0].columns
        assert stamp not in records.data[1].columns
        assert stamp_ not in records.data[1].columns

        assert value not in records.data[0].columns
        assert value_ in records.data[0].columns
        assert value not in records.data[1].columns
        assert value_ not in records.data[1].columns

        assert records.columns == [stamp_, value_]

    def test_rename_columns_validate_argument(self):
        stamp = 'stamp'
        stamp_ = 'stamp_'
        value = 'value'
        value_ = 'value_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 0}),
            ], [ColumnValue(stamp), ColumnValue(value)]
        )

        rename_keys = {stamp: stamp_}
        records.rename_columns(rename_keys)

        with pytest.raises(InvalidArgumentError):
            rename_keys = {stamp: stamp}
            records.rename_columns(rename_keys)

        # overwrite columns
        with pytest.raises(InvalidArgumentError):
            rename_keys = {stamp: stamp_, value: stamp}
            records.rename_columns(rename_keys)

        with pytest.raises(InvalidArgumentError):
            rename_keys = {stamp: value, value: value_}
            records.rename_columns(rename_keys)

        # Duplicate columns after change
        rename_keys = {stamp: stamp_, value: stamp_}
        with pytest.raises(InvalidArgumentError):
            records.rename_columns(rename_keys)

    def test_clone(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
            ], [ColumnValue('stamp')]
        )

        records_ = records.clone()
        records_.append_column(ColumnValue('value'), [9])
        assert records_.columns == ['stamp', 'value']
        assert records.columns == ['stamp']

    def test_filter_if(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
                RecordCppImpl({'stamp': 1}),
                RecordCppImpl({'stamp': 2}),
            ], [ColumnValue('stamp')]
        )

        assert len(records.data) == 3
        init_columns = records.columns

        records.filter_if(lambda record: record.get('stamp') == 1)
        assert init_columns == records.columns
        assert len(records.data) == 1
        assert records.data[0].get('stamp') == 1

        records.filter_if(lambda _: False)
        assert init_columns == records.columns
        assert len(records.data) == 0

    def test_concat(self):
        records_left = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
                RecordCppImpl({'stamp': 1}),
            ], [ColumnValue('stamp')]
        )
        records_right = RecordsCppImpl(
            [
                RecordCppImpl({'value': 3}),
                RecordCppImpl({'value': 4}),
                RecordCppImpl({'value': 5}),
            ], [ColumnValue('value')]
        )

        records_expect = RecordsCppImpl(
            records_left.data + records_right.data,
            [ColumnValue('stamp'), ColumnValue('value')]
        )

        records_concat = RecordsCppImpl(None, [ColumnValue('stamp'), ColumnValue('value')])
        records_concat.concat(records_left)
        records_concat.concat(records_right)
        assert records_concat.equals(records_expect)

        records_concat = RecordsCppImpl(None, [ColumnValue('stamp')])
        with pytest.raises(InvalidArgumentError):
            records_concat.concat(records_right)

    def test_bind_drop_as_delay(self):
        sort_key = 'sort_key'
        stamp = 'stamp'
        stamp_ = 'stamp_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({sort_key: 1, stamp: 0, stamp_: 1}),
                RecordCppImpl({sort_key: 2, stamp: 2}),
                RecordCppImpl({sort_key: 3, stamp_: 5}),
                RecordCppImpl({sort_key: 4, stamp: 6}),
                RecordCppImpl({sort_key: 5}),
            ], [ColumnValue(sort_key), ColumnValue(stamp), ColumnValue(stamp_)]
        )
        records.bind_drop_as_delay()
        records_expect = RecordsCppImpl(
            [
                RecordCppImpl({sort_key: 1, stamp: 0, stamp_: 1}),
                RecordCppImpl({sort_key: 2, stamp: 2, stamp_: 5}),
                RecordCppImpl({sort_key: 3, stamp: 6, stamp_: 5}),
                RecordCppImpl({sort_key: 4, stamp: 6}),
                RecordCppImpl({sort_key: 5}),
            ], [ColumnValue(sort_key), ColumnValue(stamp), ColumnValue(stamp_)]
        )

        assert records.equals(records_expect)

    def test_to_dataframe(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
                RecordCppImpl({'value': 1, 'stamp': 3}),
                RecordCppImpl({'addr': 2, 'stamp': 5}),
            ],
            [ColumnValue('value'), ColumnValue('stamp'), ColumnValue('addr')]
        )
        expect_dict = [record.data for record in records.data]
        expect_df = pd.DataFrame.from_dict(
            expect_dict, dtype='Int64'
        ).reindex(columns=['value', 'stamp', 'addr'])

        df = records.to_dataframe()
        assert df.equals(expect_df)

    def test_iter(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
                RecordCppImpl({'stamp': 3}),
                RecordCppImpl({'stamp': 5}),
            ],
            [ColumnValue('stamp')]
        )
        for i, record in enumerate(records):
            assert record.get('stamp') == records.data[i].get('stamp')

    def test_get_column_series(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0, 'value': 1}),
                RecordCppImpl({'value': 3}),
                RecordCppImpl({'stamp': 5}),
            ],
            [ColumnValue('stamp'), ColumnValue('value')]
        )
        assert records.get_column_series('stamp') == [0, None, 5]
        assert records.get_column_series('value') == [1, 3, None]

        with pytest.raises(InvalidArgumentError):
            records.get_column_series('x')

    def test_get_row_series(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0, 'value': 1}),
                RecordCppImpl({'value': 3}),
                RecordCppImpl({'stamp': 5}),
            ],
            [ColumnValue('stamp'), ColumnValue('value')]
        )
        assert records.get_row_series(0).equals(records.data[0])
        assert records.get_row_series(1).equals(records.data[1])
        assert records.get_row_series(2).equals(records.data[2])

        with pytest.raises(InvalidArgumentError):
            records.get_row_series(100)

    def test_groupby_1key(self):
        stamp = 'stamp'
        value = 'value'
        records = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1}),
                RecordCppImpl({stamp: 0, value: 2}),
                RecordCppImpl({stamp: 5, value: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value)]
        )
        expect = {}
        expect[(0,)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1}),
                RecordCppImpl({stamp: 0, value: 2}),
            ],
            [ColumnValue(stamp), ColumnValue(value)]
        )

        expect[(5,)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 5, value: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value)]
        )

        group = records.groupby([stamp])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_groupby_1key_key_missing(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'a': 0, 'b': 1}),
                RecordCppImpl({'b': 2}),
                RecordCppImpl({'a': 5, 'b': 3}),
            ],
            [ColumnValue('a'), ColumnValue('b')]
        )
        expect = {}
        expect[(0,)] = RecordsCppImpl(
            [
                RecordCppImpl({'a': 0, 'b': 1}),
            ],
            [ColumnValue('a'), ColumnValue('b')]
        )

        expect[(5,)] = RecordsCppImpl(
            [
                RecordCppImpl({'a': 5, 'b': 3}),
            ],
            [ColumnValue('a'), ColumnValue('b')]
        )
        expect[(2**64-1,)] = RecordsCppImpl(
            [
                RecordCppImpl({'b': 2}),
            ],
            [ColumnValue('a'), ColumnValue('b')]
        )

        group = records.groupby(['a'])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_groupby_2key(self):
        stamp = 'stamp'
        value = 'value'
        addr = 'addr'
        records = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1, addr: 2}),
                RecordCppImpl({stamp: 0, value: 2, addr: 3}),
                RecordCppImpl({stamp: 5, value: 3, addr: 3}),
                RecordCppImpl({stamp: 5, value: 4, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )

        expect = {}
        expect[(0, 2)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1, addr: 2}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )
        expect[(0, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 2, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )
        expect[(5, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 5, value: 3, addr: 3}),
                RecordCppImpl({stamp: 5, value: 4, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )

        group = records.groupby([stamp, addr])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_groupby_3key(self):
        stamp = 'stamp'
        value = 'value'
        addr = 'addr'
        records = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1, addr: 2}),
                RecordCppImpl({stamp: 0, value: 2, addr: 3}),
                RecordCppImpl({stamp: 5, value: 3, addr: 3}),
                RecordCppImpl({stamp: 5, value: 4, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )

        expect = {}
        expect[(0, 1, 2)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 1, addr: 2}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )
        expect[(0, 2, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0, value: 2, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )
        expect[(5, 3, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 5, value: 3, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )
        expect[(5, 4, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 5, value: 4, addr: 3}),
            ],
            [ColumnValue(stamp), ColumnValue(value), ColumnValue(addr)]
        )

        group = records.groupby([stamp, value, addr])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_append_column(self):
        stamp = 'stamp'
        expects = RecordsCppImpl(
            [
                RecordCppImpl({stamp: 0}),
            ],
            [ColumnValue(stamp)]
        )

        records = RecordsCppImpl([RecordCppImpl()], [])
        assert records.columns == []
        records.append_column(ColumnValue(stamp), [0])
        assert records.equals(expects)

    def test_sort(self):
        key = 'stamp'
        records = RecordsCppImpl(
            [
                RecordCppImpl({key: 2}),
                RecordCppImpl({key: 0}),
                RecordCppImpl({key: 1}),
            ],
            [ColumnValue(key)]
        )
        records_asc = RecordsCppImpl(
            [
                RecordCppImpl({key: 0}),
                RecordCppImpl({key: 1}),
                RecordCppImpl({key: 2}),
            ],
            [ColumnValue(key)]
        )
        records_desc = RecordsCppImpl(
            [
                RecordCppImpl({key: 2}),
                RecordCppImpl({key: 1}),
                RecordCppImpl({key: 0}),
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
        records = RecordsCppImpl(
            [
                RecordCppImpl({key: 2, key_: 2, key__: 6}),
                RecordCppImpl({key: 2, key_: 2}),
                RecordCppImpl({key: 1, key__: 5}),
                RecordCppImpl({key: 2, key_: 1, key__: 5}),
                RecordCppImpl({key: 0, key_: 5, key__: 5}),
                RecordCppImpl({key: 1}),
            ],
            [ColumnValue(key), ColumnValue(key_), ColumnValue(key__)]
        )
        records_asc = RecordsCppImpl(
            [
                RecordCppImpl({key: 0, key_: 5, key__: 5}),
                RecordCppImpl({key: 1, key__: 5}),
                RecordCppImpl({key: 1}),
                RecordCppImpl({key: 2, key_: 1, key__: 5}),
                RecordCppImpl({key: 2, key_: 2, key__: 6}),
                RecordCppImpl({key: 2, key_: 2}),
            ],
            [ColumnValue(key), ColumnValue(key_), ColumnValue(key__)]
        )
        records_desc = RecordsCppImpl(
            [
                RecordCppImpl({key: 2, key_: 2}),
                RecordCppImpl({key: 2, key_: 2, key__: 6}),
                RecordCppImpl({key: 2, key_: 1, key__: 5}),
                RecordCppImpl({key: 1}),
                RecordCppImpl({key: 1, key__: 5}),
                RecordCppImpl({key: 0, key_: 5, key__: 5}),
            ],
            [ColumnValue(key), ColumnValue(key_), ColumnValue(key__)]
        )
        records_ = records.clone()
        records_.sort_column_order(ascending=True, put_none_at_top=True)
        assert records_.equals(records_asc)

        records_ = records.clone()
        records_.sort_column_order(ascending=False, put_none_at_top=False)
        assert records_.equals(records_desc)

    def test_sort_with_sub_key(self):
        key = 'stamp'
        sub_key = 'stamp_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({key: 2, sub_key: 2}),
                RecordCppImpl({key: 2, sub_key: 1}),
                RecordCppImpl({key: 0, sub_key: 5}),
                RecordCppImpl({key: 1, sub_key: 3}),
            ],
            [ColumnValue(key), ColumnValue(sub_key)]
        )
        records_asc = RecordsCppImpl(
            [
                RecordCppImpl({key: 0, sub_key: 5}),
                RecordCppImpl({key: 1, sub_key: 3}),
                RecordCppImpl({key: 2, sub_key: 1}),
                RecordCppImpl({key: 2, sub_key: 2}),
            ],
            [ColumnValue(key), ColumnValue(sub_key)]
        )
        records_desc = RecordsCppImpl(
            [
                RecordCppImpl({key: 2, sub_key: 2}),
                RecordCppImpl({key: 2, sub_key: 1}),
                RecordCppImpl({key: 1, sub_key: 3}),
                RecordCppImpl({key: 0, sub_key: 5}),
            ],
            [ColumnValue(key), ColumnValue(sub_key)]
        )
        records_ = records.clone()
        records_.sort(key, sub_key=sub_key, ascending=True)
        assert records_.equals(records_asc)

        records_ = records.clone()
        records_.sort(key, sub_key=sub_key, ascending=False)
        assert records_.equals(records_desc)

    @pytest.mark.parametrize(
        'records, records_, expect',
        [
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0}),
                        RecordCppImpl({'stamp': 1}),
                    ],
                    [ColumnValue('stamp')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0}),
                        RecordCppImpl({'stamp': 1}),
                    ],
                    [ColumnValue('stamp')]
                ),
                True,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'value': 1}),
                        RecordCppImpl({'stamp': 5, 'value': 6}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value': 1, 'stamp': 0}),
                        RecordCppImpl({'value': 6, 'stamp': 5}),
                    ], [ColumnValue('value'), ColumnValue('stamp')]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'value': 1}),
                        RecordCppImpl({'stamp': 5, 'value': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value': 1, 'stamp': 0}),
                        RecordCppImpl({'value': 6, 'stamp': 5}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'value': 1}),
                        RecordCppImpl({'stamp': 5, 'value': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('value'), ColumnValue('stamp__')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value': 1, 'stamp': 0}),
                        RecordCppImpl({'value': 6, 'stamp': 5}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'value': 1}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'value': 1}),
                        RecordCppImpl({'stamp': 0, 'value': 7}),
                    ], [ColumnValue('stamp'), ColumnValue('value')]
                ),
                False,
            ),
        ],
    )
    def test_equals(self, records, records_, expect):
        assert records.equals(records_) is expect

    def test_reindex(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({'a': 1, 'b': 3}),
                RecordCppImpl({'b': 4, 'c': 5}),
            ],
            [
                ColumnValue('b'),
                ColumnValue('c'),
                ColumnValue('d'),
                ColumnValue('a'),
            ]
        )
        assert records.columns == ['b', 'c', 'd', 'a']
        records.reindex(['a', 'b', 'c', 'd'])
        assert records.columns == ['a', 'b', 'c', 'd']

        with pytest.raises(InvalidArgumentError):
            records.reindex(['a', 'b', 'c', 'd', 'e'])
        with pytest.raises(InvalidArgumentError):
            records.reindex(['a', 'b', 'c'])

    @pytest.mark.parametrize(
        'how, records_expect',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value_left': 10, 'value_right': 10,
                                       'stamp': 1, 'stamp_': 2}),
                        RecordCppImpl({'value_left': 20, 'value_right': 20,
                                       'stamp': 3, 'stamp_': 4}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 9, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 10, 'stamp_': 11}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value_left': 10, 'value_right': 10,
                                       'stamp': 1, 'stamp_': 2}),
                        RecordCppImpl({'value_left': 20, 'value_right': 20,
                                       'stamp': 3, 'stamp_': 4}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 9, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 10, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 40, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value_left': 10, 'value_right': 10,
                                       'stamp': 1, 'stamp_': 2}),
                        RecordCppImpl({'value_left': 20, 'value_right': 20,
                                       'stamp': 3, 'stamp_': 4}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 9, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 10, 'stamp_': 11}),
                        RecordCppImpl({'value_right': 50, 'stamp_': 10}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'value_left': 10, 'value_right': 10,
                                       'stamp': 1, 'stamp_': 2}),
                        RecordCppImpl({'value_left': 20, 'value_right': 20,
                                       'stamp': 3, 'stamp_': 4}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 30, 'value_right': 30,
                                       'stamp': 5, 'stamp_': 6}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 9, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 70, 'value_right': 70,
                                       'stamp': 10, 'stamp_': 11}),
                        RecordCppImpl({'value_left': 40, 'stamp': 7}),
                        RecordCppImpl({'value_right': 50, 'stamp_': 10}),
                    ],
                    [
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
        ],
    )
    def test_merge(self, how: str, records_expect):
        records_left = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 1, 'value_left': 10}),
                RecordCppImpl({'stamp': 3, 'value_left': 20}),
                RecordCppImpl({'stamp': 5, 'value_left': 30}),
                RecordCppImpl({'stamp': 7, 'value_left': 40}),
                RecordCppImpl({'stamp': 9, 'value_left': 70}),
                RecordCppImpl({'stamp': 10, 'value_left': 70}),
            ],
            [ColumnValue('stamp'), ColumnValue('value_left')]
        )

        records_right = RecordsCppImpl(
            [
                RecordCppImpl({'stamp_': 2, 'value_right': 10}),
                RecordCppImpl({'stamp_': 4, 'value_right': 20}),
                RecordCppImpl({'stamp_': 6, 'value_right': 30}),
                RecordCppImpl({'stamp_': 6, 'value_right': 30}),
                RecordCppImpl({'stamp_': 10, 'value_right': 50}),
                RecordCppImpl({'stamp_': 11, 'value_right': 70}),
            ],
            [ColumnValue('stamp_'), ColumnValue('value_right')]
        )

        columns = ['stamp', 'value_left', 'stamp_', 'value_right']
        merged = records_left.merge(
            records_right, 'value_left', 'value_right', columns, how=how)

        assert merged.equals(records_expect) is True  # type: ignore
        assert records_left.columns == [
            'stamp', 'value_left']  # type: ignore
        assert records_right.columns == [
            'stamp_', 'value_right']  # type: ignore

    def test_merge_validate(self):
        records_left = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 1, 'value_left': 10}),
            ],
            [ColumnValue('stamp'), ColumnValue('value_left')]
        )
        records_right = RecordsCppImpl(
            [
                RecordCppImpl({'stamp_': 2, 'value_right': 10}),
            ],
            [ColumnValue('stamp_'), ColumnValue('value_right')]
        )

        with pytest.raises(InvalidArgumentError):
            records_left.merge(
                records_right, 'value_left', 'value_right', ['unknown'], how='inner')

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
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
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_left'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        RecordCppImpl({'other_stamp': 8}),
                        RecordCppImpl({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_left'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        RecordCppImpl({'other_stamp_': 10}),
                        RecordCppImpl({'other_stamp_': 14}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_left'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp': 12,
                                'other_stamp_': 2,
                                'stamp': 9,
                                'stamp_': 3,
                                'value_left': 2,
                                'value_right': 2,
                            }
                        ),
                        RecordCppImpl({'other_stamp': 8}),
                        RecordCppImpl({'other_stamp': 16}),
                        RecordCppImpl({'other_stamp_': 10}),
                        RecordCppImpl({'other_stamp_': 14}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_left'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
        ],
    )
    def test_merge_with_loss(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'other_stamp': 4, 'stamp': 1, 'value_left': 1}),
                RecordCppImpl({'other_stamp': 8}),
                RecordCppImpl({'other_stamp': 12, 'stamp': 9, 'value_left': 2}),
                RecordCppImpl({'other_stamp': 16}),
            ],
            [ColumnValue('other_stamp'), ColumnValue('stamp'), ColumnValue('value_left')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'other_stamp_': 2, 'stamp_': 3, 'value_right': 2}),
                RecordCppImpl({'other_stamp_': 6, 'stamp_': 7, 'value_right': 1}),
                RecordCppImpl({'other_stamp_': 10}),
                RecordCppImpl({'other_stamp_': 14}),
            ],
            [ColumnValue('other_stamp_'), ColumnValue('stamp_'), ColumnValue('value_right')]
        )

        merged = merge(
            left_records=left_records,
            right_records=right_records,
            join_left_key='value_left',
            join_right_key='value_right',
            columns=['other_stamp', 'other_stamp_', 'stamp',
                     'stamp_', 'value_left', 'value_right'],
            how=how
        )

        assert merged.equals(expect_records) is True

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key_left': 1, 'stamp': 6}),
                        RecordCppImpl({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left_use_latest',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 4}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 5}),
                        RecordCppImpl({'key_left': 1, 'stamp': 6}),
                        RecordCppImpl({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key_right': 1, 'sub_stamp': 4}),
                        RecordCppImpl({'key_right': 2, 'sub_stamp': 5}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 2, 'key_right': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key_right': 1, 'sub_stamp': 4}),
                        RecordCppImpl({'key_right': 2, 'sub_stamp': 5}),
                        RecordCppImpl({'key_left': 1, 'stamp': 6}),
                        RecordCppImpl({'key_left': 2, 'stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequential_with_key(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'key_left': 1, 'stamp': 0}),
                RecordCppImpl({'key_left': 2, 'stamp': 1}),
                RecordCppImpl({'key_left': 1, 'stamp': 6}),
                RecordCppImpl({'key_left': 2, 'stamp': 7}),
            ],
            [ColumnValue('key_left'), ColumnValue('stamp')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'key_right': 2, 'sub_stamp': 2}),
                RecordCppImpl({'key_right': 1, 'sub_stamp': 3}),
                RecordCppImpl({'key_right': 1, 'sub_stamp': 4}),
                RecordCppImpl({'key_right': 2, 'sub_stamp': 5}),
            ],
            [ColumnValue('key_right'), ColumnValue('sub_stamp')]
        )

        merged_records = left_records.merge_sequential(
            right_records=right_records,
            left_stamp_key='stamp',
            right_stamp_key='sub_stamp',
            join_left_key='key_left',
            join_right_key='key_right',
            columns=['key_left', 'stamp', 'key_right', 'sub_stamp'],
            how=how,
        )

        assert merged_records.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                    ],
                    [ColumnValue('key'), ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key': 1, 'stamp': 6}),
                        RecordCppImpl({'key': 2, 'stamp': 7}),
                    ],
                    [ColumnValue('key'), ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'left_use_latest',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 4}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 5}),
                        RecordCppImpl({'key': 1, 'stamp': 6}),
                        RecordCppImpl({'key': 2, 'stamp': 7}),
                    ],
                    [ColumnValue('key'), ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key': 1, 'sub_stamp': 4}),
                        RecordCppImpl({'key': 2, 'sub_stamp': 5}),
                    ],
                    [ColumnValue('key'), ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key': 1, 'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key': 2, 'stamp': 1, 'sub_stamp': 2}),
                        RecordCppImpl({'key': 1, 'sub_stamp': 4}),
                        RecordCppImpl({'key': 2, 'sub_stamp': 5}),
                        RecordCppImpl({'key': 1, 'stamp': 6}),
                        RecordCppImpl({'key': 2, 'stamp': 7}),
                    ],
                    [ColumnValue('key'), ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
        ],
    )
    def test_merge_sequential_with_same_key(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'key': 1, 'stamp': 0}),
                RecordCppImpl({'key': 2, 'stamp': 1}),
                RecordCppImpl({'key': 1, 'stamp': 6}),
                RecordCppImpl({'key': 2, 'stamp': 7}),
            ],
            [ColumnValue('key'), ColumnValue('stamp')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'key': 2, 'sub_stamp': 2}),
                RecordCppImpl({'key': 1, 'sub_stamp': 3}),
                RecordCppImpl({'key': 1, 'sub_stamp': 4}),
                RecordCppImpl({'key': 2, 'sub_stamp': 5}),
            ],
            [ColumnValue('key'), ColumnValue('sub_stamp')]
        )

        merged_records = left_records.merge_sequential(
            right_records=right_records,
            left_stamp_key='stamp',
            right_stamp_key='sub_stamp',
            join_left_key='key',
            join_right_key='key',
            columns=['key', 'stamp', 'sub_stamp'],
            how=how,
        )

        assert merged_records.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'sub_stamp': 1}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 6}),
                    ],
                    [ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'sub_stamp': 1}),
                        RecordCppImpl({'stamp': 3}),
                        RecordCppImpl({'stamp': 4}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 6}),
                        RecordCppImpl({'stamp': 8}),
                    ],
                    [ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'left_use_latest',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'sub_stamp': 1}),
                        RecordCppImpl({'stamp': 3}),
                        RecordCppImpl({'stamp': 4}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 6}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 7}),
                        RecordCppImpl({'stamp': 8}),
                    ],
                    [ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'sub_stamp': 1}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 6}),
                        RecordCppImpl({'sub_stamp': 7}),
                    ],
                    [ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'stamp': 0, 'sub_stamp': 1}),
                        RecordCppImpl({'stamp': 3}),
                        RecordCppImpl({'stamp': 4}),
                        RecordCppImpl({'stamp': 5, 'sub_stamp': 6}),
                        RecordCppImpl({'sub_stamp': 7}),
                        RecordCppImpl({'stamp': 8}),
                    ],
                    [ColumnValue('stamp'), ColumnValue('sub_stamp')]
                ),
            ),
        ],
    )
    def test_merge_sequential_without_key(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
                RecordCppImpl({'stamp': 3}),
                RecordCppImpl({'stamp': 4}),
                RecordCppImpl({'stamp': 5}),
                RecordCppImpl({'stamp': 8}),
            ],
            [ColumnValue('stamp')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'sub_stamp': 1}),
                RecordCppImpl({'sub_stamp': 6}),
                RecordCppImpl({'sub_stamp': 7}),
            ],
            [ColumnValue('sub_stamp')]
        )

        merged = left_records.merge_sequential(
            right_records=right_records,
            left_stamp_key='stamp',
            right_stamp_key='sub_stamp',
            join_left_key=None,
            join_right_key=None,
            columns=['stamp', 'sub_stamp'],
            how=how,
        )

        assert merged.equals(expect_records)

    def test_merge_sequential_validate(self):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'stamp': 0}),
            ],
            [ColumnValue('stamp')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'sub_stamp': 1}),
            ],
            [ColumnValue('sub_stamp')]
        )

        with pytest.raises(InvalidArgumentError):
            left_records.merge_sequential(
                right_records=right_records,
                left_stamp_key='stamp',
                right_stamp_key='sub_stamp',
                join_left_key=None,
                join_right_key=None,
                columns=['not_exist'],
                how='left',
            )

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        RecordCppImpl({'key_left': 1, 'stamp': 4}),
                        RecordCppImpl({'key_left': 1, 'stamp': 5}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'left_use_latest',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1,
                                       'stamp': 0, 'sub_stamp': 2}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1,
                                       'stamp': 0, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 1, 'stamp': 4}),
                        RecordCppImpl({'key_left': 1, 'stamp': 5}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1,
                                       'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        RecordCppImpl({'key_right': 3, 'sub_stamp': 1}),
                        RecordCppImpl({'key_right': 1, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 0, 'sub_stamp': 2}),
                        RecordCppImpl({'key_right': 3, 'sub_stamp': 1}),
                        RecordCppImpl({'key_right': 1, 'sub_stamp': 3}),
                        RecordCppImpl({'key_left': 1, 'stamp': 4}),
                        RecordCppImpl({'key_left': 1, 'stamp': 5}),
                        RecordCppImpl({'key_left': 1, 'key_right': 1, 'stamp': 6, 'sub_stamp': 7}),
                    ],
                    [
                        ColumnValue('key_left'),
                        ColumnValue('stamp'),
                        ColumnValue('key_right'),
                        ColumnValue('sub_stamp'),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequential_with_drop(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'key_left': 1, 'stamp': 0}),
                RecordCppImpl({'key_left': 1, 'stamp': 4}),
                RecordCppImpl({'key_left': 1, 'stamp': 5}),
                RecordCppImpl({'key_left': 1, 'stamp': 6}),
            ],
            [ColumnValue('key_left'), ColumnValue('stamp')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'key_right': 3, 'sub_stamp': 1}),
                RecordCppImpl({'key_right': 1, 'sub_stamp': 2}),
                RecordCppImpl({'key_right': 1, 'sub_stamp': 3}),
                RecordCppImpl({'key_right': 1, 'sub_stamp': 7}),
            ],
            [ColumnValue('key_right'), ColumnValue('sub_stamp')]
        )

        merged = left_records.merge_sequential(
            right_records=right_records,
            left_stamp_key='stamp',
            right_stamp_key='sub_stamp',
            join_left_key='key_left',
            join_right_key='key_right',
            columns=['key_left', 'stamp', 'key_right', 'sub_stamp'],
            how=how,
        )

        assert merged.equals(expect_records)

    @pytest.mark.parametrize(
        'how, expect_records',
        [
            (
                'inner',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
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
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'left',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        RecordCppImpl({'other_stamp': 8}),
                        RecordCppImpl({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'left_use_latest',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 6,
                                'stamp': 1,
                                'stamp_': 7,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        RecordCppImpl({'other_stamp': 8}),
                        RecordCppImpl({'other_stamp': 16}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'right',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp_': 6,
                                'stamp_': 7,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl({'other_stamp_': 10, 'stamp_': 10}),
                        RecordCppImpl({'other_stamp_': 14, 'value_right': 2}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
            (
                'outer',
                RecordsCppImpl(
                    [
                        RecordCppImpl(
                            {
                                'other_stamp': 4,
                                'other_stamp_': 2,
                                'stamp': 1,
                                'stamp_': 3,
                                'value_left': 1,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl(
                            {
                                'other_stamp_': 6,
                                'stamp_': 7,
                                'value_right': 1,
                            }
                        ),
                        RecordCppImpl({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                        RecordCppImpl({'other_stamp_': 10, 'stamp_': 10}),
                        RecordCppImpl({'other_stamp': 8}),
                        RecordCppImpl({'other_stamp': 16}),
                        RecordCppImpl({'other_stamp_': 14, 'value_right': 2}),
                    ],
                    [
                        ColumnValue('other_stamp'),
                        ColumnValue('stamp'),
                        ColumnValue('value_left'),
                        ColumnValue('other_stamp_'),
                        ColumnValue('stamp_'),
                        ColumnValue('value_right'),
                    ]
                ),
            ),
        ],
    )
    def test_merge_sequential_with_loss(self, how, expect_records):
        left_records = RecordsCppImpl(
            [
                RecordCppImpl({'other_stamp': 4, 'stamp': 1, 'value_left': 1}),
                RecordCppImpl({'other_stamp': 8}),
                RecordCppImpl({'other_stamp': 12, 'stamp': 9, 'value_left': 1}),
                RecordCppImpl({'other_stamp': 16}),
            ],
            [ColumnValue('other_stamp'), ColumnValue('stamp'), ColumnValue('value_left')]
        )

        right_records = RecordsCppImpl(
            [
                RecordCppImpl({'other_stamp_': 2, 'stamp_': 3, 'value_right': 1}),
                RecordCppImpl({'other_stamp_': 6, 'stamp_': 7, 'value_right': 1}),
                RecordCppImpl({'other_stamp_': 10, 'stamp_': 10}),
                RecordCppImpl({'other_stamp_': 14, 'value_right': 2}),
            ],
            [ColumnValue('other_stamp_'), ColumnValue('stamp_'), ColumnValue('value_right')]
        )

        merged = left_records.merge_sequential(
            right_records=right_records,
            left_stamp_key='stamp',
            right_stamp_key='stamp_',
            join_left_key='value_left',
            join_right_key='value_right',
            columns=['other_stamp', 'stamp', 'value_left',
                     'other_stamp_', 'stamp_', 'value_right'],
            how=how,
        )

        assert merged.equals(expect_records) is True

    def test_merge_sequential_for_addr_track(self):
        source_records = RecordsCppImpl(
            [
                RecordCppImpl({'source_addr': 1, 'source_stamp': 0}),
                RecordCppImpl({'source_addr': 1, 'source_stamp': 10}),
                RecordCppImpl({'source_addr': 3, 'source_stamp': 20}),
            ],
            [ColumnValue('source_addr'), ColumnValue('source_stamp')]
        )

        copy_records = RecordsCppImpl(
            [
                RecordCppImpl({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 1}),
                RecordCppImpl({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 11}),
                RecordCppImpl({'addr_from': 3, 'addr_to': 13, 'copy_stamp': 21}),
                RecordCppImpl({'addr_from': 13, 'addr_to': 23, 'copy_stamp': 22}),
            ],
            [ColumnValue('addr_from'), ColumnValue('addr_to'), ColumnValue('copy_stamp')]
        )

        sink_records = RecordsCppImpl(
            [
                RecordCppImpl({'sink_addr': 13, 'sink_stamp': 2}),
                RecordCppImpl({'sink_addr': 1, 'sink_stamp': 3}),
                RecordCppImpl({'sink_addr': 13, 'sink_stamp': 12}),
                RecordCppImpl({'sink_addr': 13, 'sink_stamp': 23}),
                RecordCppImpl({'sink_addr': 3, 'sink_stamp': 24}),
                RecordCppImpl({'sink_addr': 13, 'sink_stamp': 25}),
                RecordCppImpl({'sink_addr': 3, 'sink_stamp': 26}),
                RecordCppImpl({'sink_addr': 23, 'sink_stamp': 27}),
            ],
            [ColumnValue('sink_addr'), ColumnValue('sink_stamp')]
        )

        expect_records = RecordsCppImpl(
            [
                RecordCppImpl({
                    'source_addr': 1, 'source_stamp': 0, 'sink_stamp': 2}),
                RecordCppImpl({
                    'source_addr': 1, 'source_stamp': 0, 'sink_stamp': 3,
                }),
                RecordCppImpl({
                    'source_addr': 1, 'source_stamp': 10, 'sink_stamp': 12,
                }),
                RecordCppImpl({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 23,
                }),
                RecordCppImpl({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 24,
                }),
                RecordCppImpl({
                    'source_addr': 3, 'source_stamp': 20, 'sink_stamp': 27,
                }),
            ],
            [ColumnValue('source_addr'), ColumnValue('source_stamp'), ColumnValue('sink_stamp')]
        )

        merged = source_records.merge_sequential_for_addr_track(
            source_stamp_key='source_stamp',
            source_key='source_addr',
            copy_records=copy_records,
            copy_stamp_key='copy_stamp',
            copy_from_key='addr_from',
            copy_to_key='addr_to',
            sink_records=sink_records,
            sink_stamp_key='sink_stamp',
            sink_from_key='sink_addr',
            columns=['source_addr', 'source_stamp', 'sink_stamp'],
        )

        merged.sort(key='sink_stamp')
        expect_records.sort(key='sink_stamp')

        assert source_records.columns == ['source_addr', 'source_stamp']
        assert copy_records.columns == [
            'addr_from', 'addr_to', 'copy_stamp']
        assert sink_records.columns == ['sink_addr', 'sink_stamp']

        assert merged.equals(expect_records)

    def test_merge_sequential_for_addr_track_validate(self):
        source_records = RecordsCppImpl(
            [
                RecordCppImpl({'source_addr': 1, 'source_stamp': 0}),
            ],
            [ColumnValue('source_addr'), ColumnValue('source_stamp')]
        )

        copy_records = RecordsCppImpl(
            [
                RecordCppImpl({'addr_from': 1, 'addr_to': 13, 'copy_stamp': 1}),
            ],
            [ColumnValue('addr_from'), ColumnValue('addr_to'), ColumnValue('copy_stamp')]
        )

        sink_records = RecordsCppImpl(
            [
                RecordCppImpl({'sink_addr': 13, 'sink_stamp': 2}),
            ],
            [ColumnValue('sink_addr'), ColumnValue('sink_stamp')]
        )

        with pytest.raises(InvalidArgumentError):
            source_records.merge_sequential_for_addr_track(
                source_stamp_key='source_stamp',
                source_key='source_addr',
                copy_records=copy_records,
                copy_stamp_key='copy_stamp',
                copy_from_key='addr_from',
                copy_to_key='addr_to',
                sink_records=sink_records,
                sink_stamp_key='sink_stamp',
                sink_from_key='sink_addr',
                columns=['unknown'],
            )
