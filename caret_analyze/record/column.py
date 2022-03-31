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

from functools import cached_property
from typing import Optional, Tuple

from ..exceptions import InvalidColumnAttributeError
from ..value_objects import ValueObject


class ColumnMapper():

    def __init__(self):
        self._map = {}

    def add(self, key: int, value: object):
        self._map[key] = value

    def get(self, key: int) -> object:
        return self._map[key]

    @property
    def enabled(self) -> bool:
        return len(self._map) > 0


class ColumnMapperContainer():

    def __init__(self) -> None:
        self._mappers = {}

    def __eq__(self, __o: object) -> bool:
        return True

    def __hash__(self) -> int:
        return 0

    def get(self, column_name: str) -> ColumnMapper:
        if column_name not in self._mappers:
            self._mappers[column_name] = ColumnMapper()

        return self._mappers[column_name]


class ColumnAttribute(ValueObject):
    SYSTEM_TIME: ColumnAttribute
    MSG_PIPELINE: ColumnAttribute
    SEND_MSG: ColumnAttribute
    TAKE_MSG: ColumnAttribute
    KEY_HASH: ColumnAttribute

    def __init__(self, attr_name: str) -> None:
        self._attr_name = attr_name

    @property
    def attr_name(self) -> str:
        return self._attr_name


ColumnAttribute.SYSTEM_TIME = ColumnAttribute('system_time')
ColumnAttribute.MSG_PIPELINE = ColumnAttribute('msg_pipeline')
ColumnAttribute.SEND_MSG = ColumnAttribute('send_msg')
ColumnAttribute.TAKE_MSG = ColumnAttribute('take_msg')
ColumnAttribute.KEY_HASH = ColumnAttribute('key_hash')


class Column(ValueObject):

    def __init__(
        self,
        column_name: str,
        attrs: Optional[Tuple[ColumnAttribute, ...]] = None,
        *,
        mapper_container: Optional[ColumnMapperContainer] = None
    ) -> None:
        assert isinstance(column_name, str)
        self._column_name = column_name
        self._mapper_container = mapper_container
        self._attrs = attrs or ()

    def __str__(self) -> str:
        return self.column_name

    @property
    def column_name(self) -> str:
        return self._column_name

    def add_attr(self, attr: ColumnAttribute):
        self._attrs.append(attr)

    @property
    def attrs(self) -> Tuple[ColumnAttribute, ...]:
        return self._attrs

    def create_renamed(
        self,
        column_name: str
    ) -> Column:
        return Column(
            column_name,
            self.attrs,
            mapper_container=self._mapper_container
        )

    @property
    def mapper(self) -> Optional[ColumnMapper]:
        if self._mapper_container is None:
            return None
        if ColumnAttribute.KEY_HASH not in self._attrs:
            raise InvalidColumnAttributeError(
                'attribute does not have key_hash'
            )
        return self._mapper_container.get(self.column_name)

    @cached_property
    def key_hash(self) -> bool:
        return ColumnAttribute.KEY_HASH in self._attrs
