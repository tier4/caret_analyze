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

from abc import ABCMeta, abstractmethod

from collections import UserList

from typing import Dict, List, Optional, Set, Tuple, Collection, Sequence

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
    OPTIONAL: ColumnAttribute
    NODE_IO: ColumnAttribute

    def __init__(self, attr_name: str) -> None:
        self._attr_name = attr_name

    @property
    def attr_name(self) -> str:
        return self._attr_name


ColumnAttribute.SYSTEM_TIME = ColumnAttribute('system_time')
ColumnAttribute.MSG_PIPELINE = ColumnAttribute('msg_pipeline')
ColumnAttribute.SEND_MSG = ColumnAttribute('send_msg')
ColumnAttribute.TAKE_MSG = ColumnAttribute('take_msg')
ColumnAttribute.OPTIONAL = ColumnAttribute('optional')
ColumnAttribute.NODE_IO = ColumnAttribute('node_io')


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


class ColumnValue(ValueObject):

    def __init__(
        self,
        base_column_name: str,
        attrs: Optional[Collection[ColumnAttribute]] = None,
        prefix: Optional[Collection[str]] = None,
        *,
        mapper: Optional[ColumnMapper] = None,
    ) -> None:
        self._base_column_name = base_column_name
        self._attrs = set() if attrs is None else set(attrs)
        self._prefix = () if prefix is None else tuple(prefix)
        self._mapper = mapper

    def __str__(self) -> str:
        return '/'.join(list(self.prefix) + [self._base_column_name])

    @property
    def base_column_name(self) -> str:
        return self._base_column_name

    @property
    def attrs(self) -> Set[ColumnAttribute]:
        return self._attrs

    @property
    def prefix(self) -> Tuple[str, ...]:
        return self._prefix

    @property
    def mapper(self) -> Optional[ColumnMapper]:
        return self._mapper


class Column():

    def __init__(
        self,
        observer: ColumnEventObserver,
        base_column_name: str,
        attrs: Optional[Collection[ColumnAttribute]] = None,
        prefix: Optional[List[str]] = None,
        *,
        mapper: Optional[ColumnMapper] = None,
    ) -> None:
        assert isinstance(base_column_name, str)
        self._base_column_name = base_column_name
        self._mapper = mapper
        self._attrs = set() if attrs is None else set(attrs)
        self._prefix: List[str] = prefix or []
        self._observer = observer

    def __str__(self) -> str:
        return '/'.join(self._prefix + [self._base_column_name])

    @property
    def column_name(self) -> str:
        return str(self)

    @property
    def base_column_name(self) -> str:
        return self._base_column_name

    def rename(self, new: str) -> None:
        old = str(self)
        self._base_column_name = new
        new = str(self)
        self._observer.on_column_renamed(old, new)

    def to_value(self) -> ColumnValue:
        return ColumnValue(self.base_column_name, self.attrs, tuple(self._prefix))

    # def add_attr(self, attr: ColumnAttribute):
    #     self._attrs.append(attr)

    def add_prefix(self, prefix: str) -> None:
        old = str(self)
        self._prefix.append(prefix)
        # assert len(self._prefix) <= 1
        new = str(self)
        self._observer.on_column_renamed(old, new)

    @property
    def attrs(self) -> Set[ColumnAttribute]:
        return self._attrs

    # def clone(self) -> Column:
    #     return Column(
    #         self._base_column_name,
    #         self.attrs,
    #         # mapper=deepcopy(self._mapper),
    #         prefix=self._prefix)

    # def create_renamed(
    #     self,
    #     column_name: str,
    # ) -> Column:
    #     return Column(
    #         column_name,
    #         list(self._attrs),
    #         mapper=deepcopy(self._mapper)
    #     )

    def get_mapped(self, value: int) -> object:
        assert self._mapper is not None
        return self._mapper.get(value)

    def has_mapper(self) -> bool:
        return self._mapper is not None

    def attach(self, observer: ColumnEventObserver):
        self._observer = observer

    @staticmethod
    def from_value(observer: ColumnEventObserver, column: ColumnValue) -> Column:
        return Column(
            observer, column.base_column_name, column.attrs, list(column.prefix),
            mapper=column.mapper)

    @property
    def prefix(self) -> List[str]:
        return self._prefix


class UniqueList(UserList):

    def __init__(
        self,
        init=None,
    ) -> None:
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

    def as_list(self) -> List[ColumnValue]:
        return self.data


class Columns(UserList):

    def __init__(
        self,
        observer: ColumnEventObserver,
        init: Optional[List[ColumnValue]] = None,
    ) -> None:
        columns = [Column.from_value(observer, value) for value in init or []]
        super().__init__(columns)
        self._observer: ColumnEventObserver = observer

    def as_list(self) -> List[Column]:
        return list(self)

    def drop(self, columns: Collection[str], *, base_name_match=False) -> None:
        if base_name_match:
            ordered_columns = self.gets(columns, base_name_match=base_name_match)
            columns = [str(_) for _ in ordered_columns]

        self.data = [
            column
            for column
            in self.data
            if str(column) not in columns
        ]
        if self._observer is not None:
            for column in columns:
                self._observer.on_column_dropped(column)

    # def clone(self) -> Columns:
    #     return Columns([c.clone() for c in self.data])

    def reindex(self, columns: Sequence[str], *, base_name_match=False) -> None:
        if base_name_match:
            ordered_columns = self.gets(columns, base_name_match=base_name_match)
            columns = [str(_) for _ in ordered_columns]

        tmp = []
        for column_name in columns:
            for i, column in enumerate(self.data):
                if column.column_name == column_name:
                    tmp.append(self.data.pop(i))
                    break
        assert len(tmp) == len(columns)

        self.data = tmp
        self._observer.on_column_reindexed(columns)

    def get(self, name: str, take: Optional[str] = None, *, base_name_match=False) -> Column:
        if take is not None:
            assert take in ['head', 'tail']

        if base_name_match:
            columns = [
                c
                for c
                in self.data
                if c.base_column_name == name]
        else:
            columns = [
                c
                for c
                in self.data
                if c.column_name == name]

        assert len(columns) > 0
        if take is not None and take == 'tail':
            return columns[-1]

        if take is not None and take == 'head':
            return columns[0]

        assert len(columns) == 1
        return columns[0]

    def get_by_attrs(
        self,
        attrs: Collection[ColumnAttribute],
        take: Optional[str] = None,
    ) -> Column:
        if take is not None:
            assert take in ['head', 'tail']

        set_attrs = set(attrs)
        columns = [
            c
            for c
            in self.data
            if set(c.attrs) == set_attrs]

        assert len(columns) > 0
        if take is not None and take == 'tail':
            return columns[-1]

        if take is not None and take == 'head':
            return columns[0]

        assert len(columns) == 1
        return columns[0]

    def gets(self, names: Collection[str], base_name_match=False) -> List[Column]:
        return [self.get(_, base_name_match=base_name_match) for _ in names]

    def attach(self, observer: ColumnEventObserver):
        self._observer = observer
        for column in self.data:
            column.attach(observer)

    @property
    def column_names(self) -> List[str]:
        return [str(_) for _ in self.data]

    def rename(self, rename_rule: Dict[str, str]):
        for column in self.data:
            if column.column_name in rename_rule:
                old = column.column_name
                new = rename_rule[old]
                column.rename(new)

    # @singledispatchmethod
    # def unique_concat(self, args):
    #     raise NotImplementedError('')

    # @staticmethod
    # @unique_concat.register
    # def _unique_concat_list(
    #     left: Tuple[ColumnValue, ...],
    #     right: Tuple[ColumnValue, ...]
    # ) -> Columns:
    #     uniqued_column_values = UniqueList(left + right).as_list()
    #     columns = [
    #         Column(column.base_column_name, column.attrs)
    #         for column
    #         in uniqued_column_values]
    #     return Columns(columns)

    def to_value(self) -> Tuple[ColumnValue, ...]:
        return tuple(c.to_value() for c in self.data)


class ColumnEventObserver(metaclass=ABCMeta):

    @abstractmethod
    def on_column_renamed(self, old: str, new: str):
        pass

    @abstractmethod
    def on_column_dropped(self, column: str):
        pass

    @abstractmethod
    def on_column_reindexed(self, columns: Sequence[str]):
        pass
