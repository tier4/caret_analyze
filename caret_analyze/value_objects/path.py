
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

from typing import Optional, Tuple, Union

from .node_path import NodePathValue, NodePathStructValue
from .value_object import ValueObject
from ..common import Util, CustomDict
from .communication import CommunicationStructValue


class PathValue(ValueObject):
    """Path name alias."""

    def __init__(
        self,
        alias: str,
        node_path_values: Tuple[NodePathValue, ...]
    ) -> None:
        self._alias = alias
        self._node_paths_info = node_path_values

    @property
    def path_name(self) -> str:
        return self._alias

    @property
    def node_path_values(self) -> Tuple[NodePathValue, ...]:
        return self._node_paths_info


class PathStructValue(ValueObject):
    def __init__(
        self,
        path_name: Optional[str],
        child: Tuple[Union[NodePathStructValue, CommunicationStructValue], ...],
    ) -> None:
        self._path_name = path_name
        self._child = child

    @property
    def path_name(self) -> Optional[str]:
        return self._path_name

    @property
    def node_names(self) -> Tuple[str, ...]:
        return tuple(_.node_name for _ in self.node_paths)

    @property
    def child_names(self) -> Tuple[str, ...]:
        names = []
        for child in self.child:
            if isinstance(child, NodePathStructValue):
                names.append(child.node_name)
            elif isinstance(child, CommunicationStructValue):
                names.append(child.topic_name)
        return tuple(names)

    @property
    def node_paths(self) -> Tuple[NodePathStructValue, ...]:
        node_paths = Util.filter(
            lambda x: isinstance(x, NodePathStructValue),
            self._child)
        return tuple(node_paths)

    @property
    def communications(self) -> Tuple[CommunicationStructValue, ...]:
        comm_paths = Util.filter(
            lambda x: isinstance(x, CommunicationStructValue),
            self._child)
        return tuple(comm_paths)

    @property
    def summary(self) -> CustomDict:
        d: CustomDict = CustomDict()
        d['path'] = []
        for child in self.child:
            if isinstance(child, NodePathStructValue):
                d['path'].append({'node': child.node_name})
            if isinstance(child, CommunicationStructValue):
                d['path'].append({'topic': child.topic_name})

        return d

    @property
    def child(self) -> Tuple[Union[NodePathStructValue, CommunicationStructValue], ...]:
        return self._child
