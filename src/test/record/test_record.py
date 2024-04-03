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
            RecordsCppImpl([RecordCppImpl({'a': 1})], None)

        with pytest.raises(InvalidArgumentError):
            RecordsCppImpl(None, [ColumnValue('a'), ColumnValue('a')])

    def test_columns(self):
        columns = ['value', 'stamp']
        column_values = [ColumnValue(c) for c in columns]
        records = RecordsCppImpl([], column_values)
        assert records.columns == columns

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
