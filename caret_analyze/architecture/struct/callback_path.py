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

from abc import abstractmethod
from logging import getLogger
from typing import (
    Iterable,
    Iterator,
    List,
    Tuple,
    Union,
)

from .callback import CallbackStruct
from .struct_interface import (
    CallbackStructInterface,
    VariablePassingStructInterface,
)
from ...value_objects import CallbackPathStructValue


logger = getLogger(__name__)


class CallbackPathStruct():

    def __init__(
        self,
        node_name: str,
        child: List[Union[CallbackStructInterface, VariablePassingStructInterface]],
    ) -> None:
        self._node_name = node_name
        self._child = child

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def child(self) -> List[Union[CallbackStructInterface, VariablePassingStructInterface]]:
        return self._child

    def to_value(self) -> CallbackPathStructValue:
        child = tuple(_.to_value() for _ in self.child)
        return CallbackPathStructValue(
            self.node_name,
            child
        )


class CallbackPathsStruct(Iterable):

    @abstractmethod
    def __iter__(self) -> Iterator[CallbackPathStruct]:
        pass

    def to_value(self) -> Tuple[CallbackPathStructValue, ...]:
        return tuple(callback.to_value() for callback in self)

    def as_list(self) -> List[CallbackStruct]:
        return list(self)
