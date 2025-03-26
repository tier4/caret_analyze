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

from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.record import Column, Columns, ColumnValue

import pytest


class TestColumn:

    def test_eq(self):
        column = Column(ColumnValue('name'))
        column_ = Column(ColumnValue('name'))

        assert column != column_
        assert column.value == column_.value

        column_ = Column(ColumnValue('name_'))
        assert column != column_
        assert column.value != column_.value

    def test_column_name(self, mocker):
        column = Column(ColumnValue('name'))

        assert column.column_name == 'name'

    def test_create_renamed(self):
        column = Column(ColumnValue('old'))

        assert column.column_name == 'old'
        column.rename('new')
        assert column.column_name == 'new'


class TestColumns:

    def test_rename_validation_already_exist(self, caplog):
        old_column_name = 'old'
        new_column_name = 'already_exist'
        rename_rule = {old_column_name: new_column_name}
        columns = Columns([
                ColumnValue(new_column_name),
            ])
        with pytest.raises(InvalidArgumentError):
            columns.rename(rename_rule)
            assert 'Column already exists. columns:' in caplog.messages[0]

    def test_column_names(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])
        assert columns.column_names == column_names

    def test_to_value(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        column_0 = Column(ColumnValue(column_names[0]))
        column_1 = Column(ColumnValue(column_names[1]))

        columns = Columns([ColumnValue(c) for c in column_names])
        assert columns.to_value()[0] == column_0.value
        assert columns.to_value()[1] == column_1.value

    def test_from_str(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])
        columns_from_str = Columns.from_str(column_names)
        assert columns_from_str.to_value() == columns.to_value()

    def test_drop(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])
        assert len(columns) == 2

        columns.drop([column_names[0]])
        assert len(columns) == 1
        columns.drop([column_names[1]])
        assert len(columns) == 0

    def test_drop_twice(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])
        assert len(columns) == 2

        columns.drop([column_names[0]])
        assert len(columns) == 1
        columns.drop([column_names[0]])  # no data is dropped
        assert len(columns) == 1

    def test_drop_not_exist_column_name(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])
        assert len(columns) == 2

        columns.drop(['not_exist'])  # no data is dropped
        assert len(columns) == 2

    def test_reindex(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])

        columns.reindex(column_names[::-1])  # set column names in reverse order
        assert columns.column_names[0] == column_names[1]
        assert columns.column_names[1] == column_names[0]

    def test_reindex_missing_columns(self):
        column_names = [
            'first_column',
            'second_column',
            ]
        columns = Columns([ColumnValue(c) for c in column_names])

        with pytest.raises(InvalidArgumentError):
            columns.reindex(column_names + ['third_column'])  # contains unregistered column names
