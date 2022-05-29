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


from caret_analyze.exceptions import InvalidColumnMapperError
from caret_analyze.record import (
    Column,
)

from caret_analyze.record.column import ColumnAttribute, ColumnEventObserver, ColumnMapper

import pytest


class TestColumn:

    def test_eq(self, mocker):
        observer_mock = mocker.Mock(spec=ColumnEventObserver)
        column = Column(observer_mock, 'name')
        column_ = Column(observer_mock, 'name')

        assert column != column_
        assert column.to_value() == column_.to_value()

        column_ = Column(observer_mock, 'name', [ColumnAttribute.MSG_PIPELINE])
        assert column != column_
        assert column.to_value() != column_.to_value()

    def test_column_name(self, mocker):
        observer_mock = mocker.Mock(spec=ColumnEventObserver)
        column = Column(observer_mock, 'name')

        assert column.column_name == 'name'

    def test_create_renamed(self, mocker):
        observer_mock = mocker.Mock(spec=ColumnEventObserver)
        column = Column(observer_mock, 'old')

        assert column.column_name == 'old'
        column.rename('new')
        assert column.column_name == 'new'


class TestColumnMapper:

    def test_merge(self, mocker):
        mapper = ColumnMapper()
        mapper.add(1, 1)

        mapper_ = ColumnMapper()
        mapper_.add(2, 2)

        mapper.merge(mapper_)

        assert mapper.get(1) == 1
        assert mapper.get(2) == 2

    def test_merge_missmatch(self):
        mapper = ColumnMapper()
        mapper.add(1, 1)

        mapper_ = ColumnMapper()
        mapper_.add(1, 2)

        with pytest.raises(InvalidColumnMapperError):
            mapper.merge(mapper_)
