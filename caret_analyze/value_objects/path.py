
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
from typing import Optional, Tuple, Union

from .communication import CommunicationStructValue
from .node_path import NodePathStructValue, NodePathValue
from .value_object import ValueObject
from .transform import TransformCommunicationStructValue
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError

logger = getLogger(__name__)


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


CommunicationTypes = Union[CommunicationStructValue, TransformCommunicationStructValue]


class PathStructValue(ValueObject, Summarizable):
    def __init__(
        self,
        path_name: Optional[str],
        child: Tuple[Union[NodePathStructValue, CommunicationTypes], ...],
    ) -> None:
        self._path_name = path_name
        self._child = child
        self._validate(child)

    @property
    def path_name(self) -> Optional[str]:
        return self._path_name

    @property
    def node_names(self) -> Tuple[str, ...]:
        return tuple(_.node_name for _ in self.node_paths)

    @property
    def topic_names(self) -> Tuple[str, ...]:
        return tuple(_.topic_name for _ in self.communications)

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
        node_paths = Util.filter_items(
            lambda x: isinstance(x, NodePathStructValue),
            self._child)
        return tuple(node_paths)

    @property
    def communications(self) -> Tuple[CommunicationStructValue, ...]:
        comm_paths = Util.filter_items(
            lambda x: isinstance(x, CommunicationStructValue),
            self._child)
        return tuple(comm_paths)

    @property
    def summary(self) -> Summary:
        d: Summary = Summary()
        d['path'] = []
        for child in self.child:
            if isinstance(child, NodePathStructValue):
                d['path'].append({
                    'node': child.node_name,
                })
            if isinstance(child, CommunicationStructValue):
                d['path'].append({'topic': child.topic_name})

        return d

    @property
    def child(self) -> Tuple[Union[NodePathStructValue, CommunicationTypes], ...]:
        return self._child

    @staticmethod
    def _validate(path_elements: Tuple[Union[NodePathStructValue, CommunicationTypes], ...]):
        if len(path_elements) == 0:
            return

        is_node_path = [isinstance(e, NodePathStructValue) for e in path_elements]
        is_node_path_indexes = [i for i, _ in enumerate(is_node_path) if _ is True]
        is_even_number = [i % 2 == 0 for i in is_node_path_indexes]

        if len(set(is_even_number)) >= 2:
            msg = 'NodePath and Communication should be alternated.'
            raise InvalidArgumentError(msg)

    def verify(self) -> bool:
        is_valid = True

        for child in self.node_paths[1:-1]:
            if child.message_context is None:
                is_valid = False
                msg = 'Detected "message_context is None". Correct these node_path definitions. \n'
                msg += 'To see node definition and procedure,'
                msg += f'execute :\n' \
                    f">> check_procedure('yaml', '/path/to/yaml', arch, '{child.node_name}') \n"
                msg += str(child.summary)
                logger.warning(msg)
                continue

            if child.message_context.verify() is False:
                is_valid = False

        return is_valid
