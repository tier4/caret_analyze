
# Copyright 2021 TIER IV, Inc.
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

from .communication import CommunicationStruct
from .node_path import NodePathStruct
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...value_objects import PathStructValue

logger = getLogger(__name__)


class PathStruct():
    def __init__(
        self,
        path_name: str | None,
        child: list[NodePathStruct | CommunicationStruct],
    ) -> None:
        self._path_name = path_name
        self._child = child
        self._validate(child)

    @property
    def path_name(self) -> str | None:
        return self._path_name

    @path_name.setter
    def path_name(self, n: str):
        self._path_name = n

    @property
    def node_names(self) -> list[str]:
        return [_.node_name for _ in self.node_paths]

    @property
    def topic_names(self) -> list[str]:
        return [_.topic_name for _ in self.communications]

    @property
    def child_names(self) -> list[str]:
        names: list[str] = []
        for child in self.child:
            if isinstance(child, NodePathStruct):
                names.append(child.node_name)
            elif isinstance(child, CommunicationStruct):
                names.append(child.topic_name)
        return names

    @property
    def node_paths(self) -> list[NodePathStruct]:
        node_paths: list[NodePathStruct] = Util.filter_items(
            lambda x: isinstance(x, NodePathStruct),
            self._child)
        return node_paths

    @property
    def communications(self) -> list[CommunicationStruct]:
        comm_paths: list[CommunicationStruct] = Util.filter_items(
            lambda x: isinstance(x, CommunicationStruct),
            self._child)
        return comm_paths

    @property
    def child(self) -> list[NodePathStruct | CommunicationStruct]:
        return self._child

    @staticmethod
    def _validate(path_elements: list[NodePathStruct | CommunicationStruct]):
        if len(path_elements) == 0:
            return

        t = NodePathStruct \
            if isinstance(path_elements[0], NodePathStruct) \
            else CommunicationStruct

        for e in path_elements[1:]:
            if t == CommunicationStruct:
                t = NodePathStruct
            else:
                t = CommunicationStruct
            if isinstance(e, t):
                continue
            msg = 'NodePath and Communication should be alternated.'
            raise InvalidArgumentError(msg)

    def to_value(self) -> PathStructValue:
        """
        Get path struct value.

        Returns
        -------
        PathStructValue
            Path struct value instance.

        """
        return PathStructValue(None if self.path_name is None else self.path_name,
                               tuple(v.to_value() for v in self.child))

    def rename_node(self, src: str, dst: str) -> None:
        """
        Rename node.

        Parameters
        ----------
        src : str
            Current node name.
        dst : str
            Updated node name.

        """
        for n in self.node_paths:
            n.rename_node(src, dst)

        for c in self.communications:
            c.rename_node(src, dst)

    def rename_topic(self, src: str, dst: str) -> None:
        """
        Rename topic.

        Parameters
        ----------
        src : str
            Current topic name.
        dst : str
            Updated topic name.

        """
        for n in self.node_paths:
            n.rename_topic(src, dst)

        for c in self.communications:
            c.rename_topic(src, dst)
