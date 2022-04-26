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
from copy import deepcopy

from typing import Dict, List, Optional, Set, Tuple

from ..value_objects import ValueObject


class ColumnMapper():

    def __init__(self) -> None:
        self._map: Dict[int, object] = {}
        self._map_rev: Dict[object, int] = {}
        self._keys: Set[int] = set()

    def add(self, key: int, value: object):
        self._map[key] = value
        self._map_rev[value] = key
        self._keys.add(key)

    def get(self, key: int) -> object:
        return self._map[key]

    def get_key(self, value: object) -> int:
        return self._map_rev[value]

    @property
    def keys(self) -> Set[int]:
        return self._keys

    @property
    def enabled(self) -> bool:
        return len(self._map) > 0


# class ColumnMapperContainer():

#     def __init__(self) -> None:
#         self._mappers = {}

#     def __eq__(self, __o: object) -> bool:
#         return True

#     def __hash__(self) -> int:
#         return 0

#     def get(self, column_name: str) -> ColumnMapper:
#         if column_name not in self._mappers:
#             self._mappers[column_name] = ColumnMapper()

#         return self._mappers[column_name]

#     def rename(self, old: str, new: str,) -> None:
#         self._mappers[new] = self._mappers.pop(old)

#     def shallow_copy(self, old: str, new: str) -> None:
#         self._mappers[new] = self._mappers[old]


class ColumnAttribute(ValueObject):
    SYSTEM_TIME: ColumnAttribute
    MSG_PIPELINE: ColumnAttribute
    SEND_MSG: ColumnAttribute
    TAKE_MSG: ColumnAttribute

    def __init__(self, attr_name: str) -> None:
        self._attr_name = attr_name

    @property
    def attr_name(self) -> str:
        return self._attr_name


ColumnAttribute.SYSTEM_TIME = ColumnAttribute('system_time')
ColumnAttribute.MSG_PIPELINE = ColumnAttribute('msg_pipeline')
ColumnAttribute.SEND_MSG = ColumnAttribute('send_msg')
ColumnAttribute.TAKE_MSG = ColumnAttribute('take_msg')


def get_column_name(columns: List[Column], column_name: str) -> str:
    for c in columns:
        if c.column_name.endswith(column_name):
            return c.column_name
    raise ValueError(f'Column name "{column_name}" not found.')


def get_column(columns: List[Column], column_name: str) -> Column:
    for c in columns:
        if c.column_name.endswith(column_name):
            return c
    raise ValueError(f'Column name "{column_name}" not found.')


class Column(ValueObject):

    def __init__(
        self,
        column_name: str,
        attrs: Optional[List[ColumnAttribute]] = None,
        *,
        mapper: Optional[ColumnMapper] = None
    ) -> None:
        assert isinstance(column_name, str)
        self._column_name = column_name
        self._mapper = mapper
        self._attrs = () if attrs is None else tuple(attrs)

    def __str__(self) -> str:
        return self.column_name

    @property
    def column_name(self) -> str:
        return self._column_name

    # def add_attr(self, attr: ColumnAttribute):
    #     self._attrs.append(attr)

    @property
    def attrs(self) -> Tuple[ColumnAttribute, ...]:
        return self._attrs

    def create_renamed(
        self,
        column_name: str,
    ) -> Column:
        return Column(
            column_name,
            list(self._attrs),
            mapper=deepcopy(self._mapper)
        )

    def get_mapped(self, value: int) -> object:
        assert self._mapper is not None
        return self._mapper.get(value)

    def has_mapper(self) -> bool:
        return self._mapper is not None


class UniqueList(UserList):

    def __init__(self, init=None):
        super().__init__(None)
        init = init or []
        for i in init:
            self.append(i)

    def append(self, i):
        if i in self.data:
            return
        self.data.append(i)

    def __add__(self, other):
        return self.data + other

    def __iadd__(self, other):
        for i in other:
            self.append(i)
        return self


class Columns(UniqueList):

    def __init__(self, init: Optional[List[Column]] = None):
        super().__init__(init=init)

    def as_list(self) -> List[Column]:
        return list(self)
