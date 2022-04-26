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


from caret_analyze.record import (
    Column,
)

from caret_analyze.record.column import ColumnAttribute


class TestColumn:

    def test_eq(self):
        column = Column('name')
        column_ = Column('name')

        assert column == column_

        column_ = Column('name', [ColumnAttribute.MSG_PIPELINE])
        assert column != column_

    def test_column_name(self):
        column = Column('name')

        assert column.column_name == 'name'

    def test_create_renamed(self):
        old = Column('old')
        new = old.create_renamed('new')

        assert new.column_name == 'new'
