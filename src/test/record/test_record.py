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

import pandas as pd

from copy import deepcopy
from typing import Any

from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.record.column import ColumnValue

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

column0 = 'stamp'
column1 = 'value'
column2 = 'addr'

class TestRecord:

    @pytest.mark.parametrize(
        'record, record_, expect',
        [
            (RecordCppImpl({column0: 0}), RecordCppImpl({column0: 0}), True),
            (RecordCppImpl({column0: 0}), RecordCppImpl({column0: 1}), False),
            (RecordCppImpl({column0: 0, column1: 1}), RecordCppImpl(
                {column0: 0, column1: 1}), True),
            (RecordCppImpl({column0: 0, column1: 1}), RecordCppImpl(
                {column1: 1, column0: 0}), True),
            (RecordCppImpl({column0: 0, column1: 1}), RecordCppImpl(
                {column0: 1, column1: 0}), False),
        ],
    )
    def test_equals(self, record, record_, expect):
        assert record.equals(record_) is expect

    def test_data(self):
        dic = {column0: 0, column1: 18446744073709551615}
        record = RecordCppImpl(dic)
        assert record.data == dic


    def test_columns(self):
        dic = {column0: 0, column1: 1}
        record = RecordCppImpl(dic)
        assert record.columns == set(dic.keys())

    def test_get(self):
        dic = {column0: 0, column1: 1}
        record = RecordCppImpl(dic)
        assert record.get(column0) == 0
        assert record.get(column1) == 1

    def test_get_with_default(self):
        dic = {column0: 0}
        record = RecordCppImpl(dic)
        assert record.get_with_default(column0, 0) == 0
        assert record.get_with_default(column1, 0) == 0
        assert record.get_with_default(column1, 1) == 1

    def test_add(self):
        expect = {column0: 0, column1: 1}

        record = RecordCppImpl()
        record.add(column0, 0)
        record.add(column1, 1)
        assert record.data == expect
        assert record.columns == expect.keys()

    def test_change_dict_key(self):
        record = RecordCppImpl({'key_before': 0, column1: 1})
        expect = RecordCppImpl({'key_after': 0, column1: 1})

        record.change_dict_key('key_before', 'key_after')
        assert record.equals(expect)
        assert record.columns == expect.columns

    def test_drop_columns(self):
        dic = {column0: 0, column1: 1}
        record = RecordCppImpl(dic)
        assert record.columns == set(dic.keys())

        record.drop_columns([column0])
        assert record.columns == {column1}

    def test_merge(self):
        left_dict = {'a': 1, 'b': 2}
        right_dict = {column2: 3, 'd': 4}
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
            RecordsCppImpl([RecordCppImpl({column0: 1})], None)

        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl(None, [ColumnValue(column0), ColumnValue(column0)])

    def test_columns(self):
        columns = [column1, column0]
        column_values = [ColumnValue(c) for c in columns]
        records = RecordsCppImpl([], column_values)
        assert records.columns == columns

    def test_data(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column0: 2, column1: 4}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        records = RecordsCppImpl(expects.data, expects._columns.to_value())
        assert records.equals(expects)

    def test_len(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column0: 2, column1: 4})
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        assert len(expects) == 2

    def test_append(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column0: 2, column1: 4}),
                RecordCppImpl({column0: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        records = RecordsCppImpl(None, [ColumnValue(column0), ColumnValue(column1)])
        records.append(expects.data[0])
        records.append(expects.data[1])
        records.append(expects.data[2])
        assert records.equals(expects)
        assert records._columns.to_value() == expects._columns.to_value()

        # tests for multimethod
        records = RecordsCppImpl(None, [ColumnValue(column0), ColumnValue(column1)])
        records.append(expects.data[0].data)
        records.append(expects.data[1].data)
        records.append(expects.data[2].data)
        assert records.equals(expects)
        assert records.columns == expects.columns

    def test_drop_columns(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 3}),
                RecordCppImpl({column0: 1, column1: 4}),
                RecordCppImpl({column0: 2, column1: 5}),
            ], [ColumnValue(column0), ColumnValue(column1)]
        )

        columns_expect = [column1]

        drop_keys = [column0]
        records.drop_columns(drop_keys)
        for record in records.data:
            assert column0 not in record.columns
        assert records.columns == columns_expect
        for datum in records.data:
            assert datum.columns == set(columns_expect)
        assert records.data[0].get(column1) == 3
        assert records.data[1].get(column1) == 4
        assert records.data[2].get(column1) == 5

        records_empty = RecordsCppImpl()
        assert records_empty.columns == []
        records_empty.drop_columns(drop_keys)
        assert records_empty.columns == []
        assert len(records_empty.data) == 0

        with pytest.raises(InvalidArgumentError):
            records.drop_columns('')

    def test_rename_columns(self):
        column0_ = 'stamp_'
        column1_ = 'value_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl(),
            ], [ColumnValue(column0), ColumnValue(column1)]
        )

        assert column0 in records.data[0].columns
        assert column0_ not in records.data[0].columns
        assert column0 not in records.data[1].columns
        assert column0_ not in records.data[1].columns

        assert column1 in records.data[0].columns
        assert column1_ not in records.data[0].columns
        assert column1 not in records.data[1].columns
        assert column1_ not in records.data[1].columns

        assert records.columns == [column0, column1]

        rename_keys = {
            column1: column1_,
            column0: column0_,
        }

        records.rename_columns(rename_keys)
        assert column0 not in records.data[0].columns
        assert column0_ in records.data[0].columns
        assert column0 not in records.data[1].columns
        assert column0_ not in records.data[1].columns

        assert column1 not in records.data[0].columns
        assert column1_ in records.data[0].columns
        assert column1 not in records.data[1].columns
        assert column1_ not in records.data[1].columns

        assert records.columns == [column0_, column1_]

    def test_rename_columns_validate_argument(self):
        column0_ = 'stamp_'
        column1_ = 'value_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 0}),
            ], [ColumnValue(column0), ColumnValue(column1)]
        )

        rename_keys = {column0: column0_}
        records.rename_columns(rename_keys)

        with pytest.raises(InvalidArgumentError):
            rename_keys = {column0: column0}
            records.rename_columns(rename_keys)

        # overwrite columns
        with pytest.raises(InvalidArgumentError):
            rename_keys = {column0: column0_, column1: column0}
            records.rename_columns(rename_keys)

        with pytest.raises(InvalidArgumentError):
            rename_keys = {column0: column1, column1: column1_}
            records.rename_columns(rename_keys)

        # Duplicate columns after change
        rename_keys = {column0: column0_, column1: column0_}
        with pytest.raises(InvalidArgumentError):
            records.rename_columns(rename_keys)

    def test_clone(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
            ], [ColumnValue(column0)]
        )

        records_ = records.clone()
        records_.append_column(ColumnValue(column1), [9])
        assert records_.columns == [column0, column1]
        assert records.columns == [column0]

    def test_filter_if(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column0: 1}),
                RecordCppImpl({column0: 2}),
            ], [ColumnValue(column0)]
        )

        assert len(records.data) == 3
        init_columns = records.columns

        records.filter_if(lambda record: record.get(column0) == 1)
        assert init_columns == records.columns
        assert len(records.data) == 1
        assert records.data[0].get(column0) == 1

        records.filter_if(lambda _: False)
        assert init_columns == records.columns
        assert len(records.data) == 0

    def test_concat(self):
        records_left = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column0: 1}),
            ], [ColumnValue(column0)]
        )
        records_right = RecordsCppImpl(
            [
                RecordCppImpl({column1: 3}),
                RecordCppImpl({column1: 4}),
                RecordCppImpl({column1: 5}),
            ], [ColumnValue(column1)]
        )

        records_expect = RecordsCppImpl(
            records_left.data + records_right.data,
            [ColumnValue(column0), ColumnValue(column1)]
        )

        records_concat = RecordsCppImpl(None, [ColumnValue(column0), ColumnValue(column1)])
        records_concat.concat(records_left)
        records_concat.concat(records_right)
        assert records_concat.equals(records_expect)

        records_concat = RecordsCppImpl(None, [ColumnValue(column0)])
        with pytest.raises(InvalidArgumentError):
            records_concat.concat(records_right)

    def test_bind_drop_as_delay(self):
        sort_key = 'sort_key'
        column0_ = 'stamp_'
        records = RecordsCppImpl(
            [
                RecordCppImpl({sort_key: 1, column0: 0, column0_: 1}),
                RecordCppImpl({sort_key: 2, column0: 2}),
                RecordCppImpl({sort_key: 3, column0_: 5}),
                RecordCppImpl({sort_key: 4, column0: 6}),
                RecordCppImpl({sort_key: 5}),
            ], [ColumnValue(sort_key), ColumnValue(column0), ColumnValue(column0_)]
        )
        records.bind_drop_as_delay()
        records_expect = RecordsCppImpl(
            [
                RecordCppImpl({sort_key: 1, column0: 0, column0_: 1}),
                RecordCppImpl({sort_key: 2, column0: 2, column0_: 5}),
                RecordCppImpl({sort_key: 3, column0: 6, column0_: 5}),
                RecordCppImpl({sort_key: 4, column0: 6}),
                RecordCppImpl({sort_key: 5}),
            ], [ColumnValue(sort_key), ColumnValue(column0), ColumnValue(column0_)]
        )

        assert records.equals(records_expect)

    def test_to_dataframe(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column1: 1, column0: 3}),
                RecordCppImpl({column2: 2, column0: 5}),
            ],
            [ColumnValue(column1), ColumnValue(column0), ColumnValue(column2)]
        )
        expect_dict = [record.data for record in records.data]
        expect_df = pd.DataFrame.from_dict(
            expect_dict, dtype='Int64'
        ).reindex(columns=[column1, column0, column2])

        df = records.to_dataframe()
        assert df.equals(expect_df)

    def test_iter(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column0: 3}),
                RecordCppImpl({column0: 5}),
            ],
            [ColumnValue(column0)]
        )
        for i, record in enumerate(records):
            assert record.get(column0) == records.data[i].get(column0)

    def test_get_column_series(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column1: 3}),
                RecordCppImpl({column0: 5}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        assert records.get_column_series(column0) == [0, None, 5]
        assert records.get_column_series(column1) == [1, 3, None]

        with pytest.raises(InvalidArgumentError):
            records.get_column_series('x')

    def test_get_row_series(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column1: 3}),
                RecordCppImpl({column0: 5}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        assert records.get_row_series(0).equals(records.data[0])
        assert records.get_row_series(1).equals(records.data[1])
        assert records.get_row_series(2).equals(records.data[2])

        with pytest.raises(InvalidArgumentError):
            records.get_row_series(100)

    def test_groupby_1key(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column0: 0, column1: 2}),
                RecordCppImpl({column0: 5, column1: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )
        expect = {}
        expect[(0,)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1}),
                RecordCppImpl({column0: 0, column1: 2}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )

        expect[(5,)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 5, column1: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1)]
        )

        group = records.groupby([column0])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_groupby_2key(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1, column2: 2}),
                RecordCppImpl({column0: 0, column1: 2, column2: 3}),
                RecordCppImpl({column0: 5, column1: 3, column2: 3}),
                RecordCppImpl({column0: 5, column1: 4, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )

        expect = {}
        expect[(0, 2)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1, column2: 2}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )
        expect[(0, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 2, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )
        expect[(5, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 5, column1: 3, column2: 3}),
                RecordCppImpl({column0: 5, column1: 4, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )

        group = records.groupby([column0, column2])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_groupby_3key(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1, column2: 2}),
                RecordCppImpl({column0: 0, column1: 2, column2: 3}),
                RecordCppImpl({column0: 5, column1: 3, column2: 3}),
                RecordCppImpl({column0: 5, column1: 4, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )

        expect = {}
        expect[(0, 1, 2)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 1, column2: 2}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )
        expect[(0, 2, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0, column1: 2, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )
        expect[(5, 3, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 5, column1: 3, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )
        expect[(5, 4, 3)] = RecordsCppImpl(
            [
                RecordCppImpl({column0: 5, column1: 4, column2: 3}),
            ],
            [ColumnValue(column0), ColumnValue(column1), ColumnValue(column2)]
        )

        group = records.groupby([column0, column1, column2])

        assert group.keys() == expect.keys()

        for k, v in group.items():
            assert v.equals(expect[k])

    def test_append_column(self):
        expects = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
            ],
            [ColumnValue(column0)]
        )

        records = RecordsCppImpl([RecordCppImpl()], [])
        assert records.columns == []
        records.append_column(ColumnValue(column0), [0])
        assert records.equals(expects)

    def test_sort(self):
        records = RecordsCppImpl(
            [
                RecordCppImpl({column0: 2}),
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column0: 1}),
            ],
            [ColumnValue(column0)]
        )
        records_asc = RecordsCppImpl(
            [
                RecordCppImpl({column0: 0}),
                RecordCppImpl({column0: 1}),
                RecordCppImpl({column0: 2}),
            ],
            [ColumnValue(column0)]
        )
        records_desc = RecordsCppImpl(
            [
                RecordCppImpl({column0: 2}),
                RecordCppImpl({column0: 1}),
                RecordCppImpl({column0: 0}),
            ],
            [ColumnValue(column0)]
        )
        records_ = records.clone()
        records_.sort(column0, ascending=True)
        assert records_.equals(records_asc)

        records_ = records.clone()
        records_.sort(column0, ascending=False)
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
                        RecordCppImpl({column0: 0}),
                        RecordCppImpl({column0: 1}),
                    ],
                    [ColumnValue(column0)]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0}),
                        RecordCppImpl({column0: 1}),
                    ],
                    [ColumnValue(column0)]
                ),
                True,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0, column1: 1}),
                        RecordCppImpl({column0: 5, column1: 6}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({column1: 1, column0: 0}),
                        RecordCppImpl({column1: 6, column0: 5}),
                    ], [ColumnValue(column1), ColumnValue(column0)]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0, column1: 1}),
                        RecordCppImpl({column0: 5, column1: 7}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({column1: 1, column0: 0}),
                        RecordCppImpl({column1: 6, column0: 5}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0, column1: 1}),
                        RecordCppImpl({column0: 5, column1: 7}),
                    ], [ColumnValue(column0), ColumnValue(column1), ColumnValue('stamp__')]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({column1: 1, column0: 0}),
                        RecordCppImpl({column1: 6, column0: 5}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
                ),
                False,
            ),
            (
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0, column1: 1}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
                ),
                RecordsCppImpl(
                    [
                        RecordCppImpl({column0: 0, column1: 1}),
                        RecordCppImpl({column0: 0, column1: 7}),
                    ], [ColumnValue(column0), ColumnValue(column1)]
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
    def test_merge(self, how: str, records_expect: RecordsCppImpl):
        records_left  = RecordsCppImpl(
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
