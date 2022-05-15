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

from typing import (
    Dict,
    Iterable,
    Iterator,
    Optional,
    Tuple,
)

from .callback import (
    CallbacksStruct,
)

from ...value_objects import (
    CallbackGroupStructValue,
    CallbackGroupType,
    NodeValue,
)

from ...exceptions import ItemNotFoundError, Error

from logging import getLogger

logger = getLogger(__name__)

class CallbackGroupStruct():

    def __init__(
        self,
        callback_group_id: str,
        callback_group_name: str,
        callback_group_type: CallbackGroupType,
        node_name: Optional[str] = None,
        callbacks: Optional[CallbacksStruct] = None,
    ) -> None:
        self._callback_group_type = callback_group_type
        self._node_name = node_name
        self._callbacks = callbacks
        self._callback_group_name = callback_group_name
        self._callback_group_id = callback_group_id

    def to_value(self) -> CallbackGroupStructValue:
        return CallbackGroupStructValue(
            callback_group_type=self.callback_group_type,
            callback_group_name=self.callback_group_name,
            node_name=self.node_name,
            callback_values=self.callbacks.to_value(),
            callback_group_id=self.callback_group_id,
        )

    @property
    def callback_group_name(self) -> str:
        return self._callback_group_name

    @property
    def node_name(self) -> str:
        assert self._node_name is not None
        return self._node_name

    @property
    def callback_group_type(self) -> CallbackGroupType:
        return self._callback_group_type

    @property
    def callback_group_id(self) -> str:
        return self._callback_group_id

    @property
    def callbacks(self) -> CallbacksStruct:
        assert self._callbacks is not None
        return self._callbacks


class CallbackGroupsStruct(Iterable):

    def __init__(
        self,
        node: Optional[NodeValue] = None
    ) -> None:
        self._data: Dict[str, CallbackGroupStruct] = {}
        self._node = node

    @property
    def node(self) -> NodeValue:
        assert self._node is not None
        return self._node

    @node.setter
    def node(self, node: NodeValue):
        self._node = node

    def insert(self, cbg: CallbackGroupStruct) -> None:
        self._data[cbg.callback_group_id] = cbg

    def add(self, cbgs: CallbackGroupsStruct) -> None:
        for cbg in cbgs:
            self.insert(cbg)

    def callbacks(self) -> CallbacksStruct:
        cbs = CallbacksStruct()
        for cbg in self:
            cbs.add(cbg.callbacks)
        return cbs

    def get_cbg(self, callback_group_id: str) -> CallbackGroupStruct:
        if callback_group_id in self._data:
            return self._data[callback_group_id]
        raise ItemNotFoundError(f'CallbackGroup {callback_group_id}')

    def get_cbgs(self, *callback_group_ids: str) -> CallbackGroupsStruct:
        cbgs = CallbackGroupsStruct()

        for cbg_id in callback_group_ids:
            try:
                cbg = self.get_cbg(cbg_id)
                cbgs.insert(cbg)
            except Error as e:
                logger.warning('Failed to get callback group %s: %s', cbg_id, e)

        return cbgs

    def to_value(self) -> Tuple[CallbackGroupStructValue, ...]:
        return tuple(_.to_value() for _ in self._data.values())

    def __iter__(self) -> Iterator[CallbackGroupStruct]:
        return iter(self._data.values())
