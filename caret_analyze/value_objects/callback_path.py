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

from logging import getLogger
from typing import (
    Iterable,
    Iterator,
    Tuple,
    Union,
)

from .callback import CallbackStructValue
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue
from ..common import Summarizable, Summary, Util

logger = getLogger(__name__)


class CallbackPathValue(ValueObject):
    def __init__(
        self,
    ) -> None:
        pass


class CallbackPathStructValue(ValueObject, Summarizable, Iterable):

    def __init__(
        self,
        node_name: str,
        child: Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]
    ) -> None:
        self._node_name = node_name
        self._child = child

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def child(self) -> Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]:
        return self._child

    @property
    def summary(self) -> Summary:
        raise NotImplementedError('')

    @property
    def callbacks(self) -> Tuple[CallbackStructValue, ...]:
        return tuple(Util.filter_items(
            lambda x: isinstance(x, CallbackStructValue),
            self.child))

    @property
    def var_passes(self) -> Tuple[VariablePassingStructValue, ...]:
        return tuple(Util.filter_items(
            lambda x: isinstance(x, VariablePassingStructValue),
            self.child))

    def __iter__(self) -> Iterator[Union[CallbackStructValue, VariablePassingStructValue]]:
        return iter(self.child)
