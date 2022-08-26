
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

from .communication import CommunicationStruct
from .node_path import NodePathStruct
from ...common import Summarizable, Summary, Util
from ...exceptions import InvalidArgumentError
from ...value_objects import PathStructValue

logger = getLogger(__name__)


class PathStruct(Summarizable):
    def __init__(
        self,
        path_name: Optional[str],
        child: Tuple[Union[NodePathStruct, CommunicationStruct], ...],
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
            if isinstance(child, NodePathStruct):
                names.append(child.node_name)
            elif isinstance(child, CommunicationStruct):
                names.append(child.topic_name)
        return tuple(names)

    @property
    def node_paths(self) -> Tuple[NodePathStruct, ...]:
        node_paths = Util.filter_items(
            lambda x: isinstance(x, NodePathStruct),
            self._child)
        return tuple(node_paths)

    @property
    def communications(self) -> Tuple[CommunicationStruct, ...]:
        comm_paths = Util.filter_items(
            lambda x: isinstance(x, CommunicationStruct),
            self._child)
        return tuple(comm_paths)

    @property
    def summary(self) -> Summary:
        d: Summary = Summary()
        d['path'] = []
        for child in self.child:
            if isinstance(child, NodePathStruct):
                context = None
                if child.message_context is not None:
                    context = child.message_context.summary

                d['path'].append({
                    'node': child.node_name,
                    'message_context': context
                })
            if isinstance(child, CommunicationStruct):
                d['path'].append({'topic': child.topic_name})

        return d

    @property
    def child(self) -> Tuple[Union[NodePathStruct, CommunicationStruct], ...]:
        return self._child

    @staticmethod
    def _validate(path_elements: Tuple[Union[NodePathStruct, CommunicationStruct], ...]):
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

    def to_value(self) -> PathStructValue:
        return PathStructValue(self.path_name, Tuple(v.to_value() for v in self.child))
