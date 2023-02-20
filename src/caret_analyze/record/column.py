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

from __future__ import annotations

from collections import UserList
from itertools import groupby
from typing import (
    Collection,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from ..common import UniqueList
from ..exceptions import (
    InvalidArgumentError,
    ItemNotFoundError,
)
from ..value_objects import ValueObject


def validate_rename_rule(
    rename_rule: Dict[str, str],
    old_columns: List[str]
) -> None:
    already_exist = set(old_columns) & set(rename_rule.values())
    if len(already_exist) > 0:
        raise InvalidArgumentError(
            f'Column already exists. columns: {already_exist}'
        )

    not_exist = set(rename_rule.keys()) - set(old_columns)
    if len(not_exist) > 0:
        raise InvalidArgumentError(
            f'Target column does not exist. columns: {not_exist}'
        )

    # overwrite columns
    if len(set(rename_rule.keys()) & set(rename_rule.values())) > 0:
        msg = 'Overwrite columns. '
        msg += str(rename_rule)
        raise InvalidArgumentError(msg)

    # duplicate columns after change
    for _, group in groupby(rename_rule.values()):
        if len(list(group)) > 1:
            msg = 'duplicate columns'
            msg += str(rename_rule)
            raise InvalidArgumentError(msg)

    return None


class ColumnValue(ValueObject):

    def __init__(
        self,
        column_name: str,
    ) -> None:
        self._column_name = column_name

    def __str__(self) -> str:
        return self._column_name

    @property
    def column_name(self) -> str:
        return str(self)


class Column():

    def __init__(
        self,
        value: ColumnValue,
    ) -> None:
        self._value = value

    def __str__(self) -> str:
        return self._value.column_name

    @property
    def column_name(self) -> str:
        return str(self)

    def rename(self, new: str) -> None:
        self._value = ColumnValue(new)

    @property
    def value(self) -> ColumnValue:
        return self._value

    @staticmethod
    def from_str(column_name: str) -> Column:
        return Column(ColumnValue(column_name))


class Columns(UserList):

    def __init__(
        self,
        init: Optional[Sequence[ColumnValue]] = None,
    ) -> None:
        uniq_init = UniqueList(init or [])
        super().__init__([Column(value) for value in uniq_init])

    def append(self, item: Column) -> None:
        super().append(item)

    def as_list(self) -> List[Column]:
        return list(self)

    def drop(self, columns: Collection[str]) -> None:
        if not isinstance(columns, Collection) or isinstance(columns, str):
            raise InvalidArgumentError(
                f'columns must be a collection, not {type(columns)}')

        self.data = [
            column
            for column
            in self.data
            if str(column) not in columns
        ]

    def reindex(self, columns: Sequence[str]) -> None:

        err_columns = set(self.column_names) ^ set(columns)
        if len(err_columns) > 0:
            msg = 'Column names do not match. '
            for err_column in err_columns:
                msg += f'{err_column}, '
            raise InvalidArgumentError(msg)

        necessary_columns = {
            column.column_name
            for column
            in self.data
        }
        tmp = []
        for column_name in columns:
            for i, column in enumerate(self.data):
                if column.column_name == column_name:
                    tmp.append(self.data.pop(i))
                    break
        missing_columns = set(necessary_columns) - {c.column_name for c in tmp}
        if len(missing_columns) > 0:
            raise ValueError(
                f'Not all necessary columns are present in the new order: {missing_columns}')

        self.data = tmp

    def get(self, name: str, take: Optional[str] = None) -> Column:
        if take is not None:
            assert take in ['head', 'tail']

        columns = [
            c
            for c
            in self.data
            if c.column_name == name]

        if len(columns) == 0:
            raise ItemNotFoundError('Item not found')

        if take is not None and take == 'tail':
            return columns[-1]

        if take is not None and take == 'head':
            return columns[0]

        assert len(columns) == 1
        return columns[0]

    def gets(self, names: Collection[str]) -> List[Column]:
        return [self.get(_) for _ in names]

    @property
    def column_names(self) -> List[str]:
        return [str(_) for _ in self.data]

    def rename(self, rename_rule: Dict[str, str]):
        validate_rename_rule(rename_rule, self.column_names)
        for column in self.data:
            if column.column_name in rename_rule:
                old = column.column_name
                new = rename_rule[old]
                column.rename(new)

    def to_value(self) -> Tuple[ColumnValue, ...]:
        return tuple(c.value for c in self.data)

    @staticmethod
    def from_str(column_names: Sequence[str]) -> Columns:
        return Columns(
            [ColumnValue(c) for c in column_names]
        )
