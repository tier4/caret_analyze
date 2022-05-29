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
from typing import Iterable, Iterator, List, Tuple, Union

from .communication import CommunicationsStruct
from .node import NodesStruct
from ..reader_interface import ArchitectureReader
from ...exceptions import Error
from ...value_objects import (
    CommunicationStructValue,
    NodePathStructValue,
    PathStructValue,
    PathValue,
    TransformCommunicationStructValue,
)

logger = getLogger(__name__)


class PathsStruct(Iterable):

    def __init__(
        self,
        reader: ArchitectureReader,
        nodes_loaded: NodesStruct,
        communications_loaded: CommunicationsStruct,
    ) -> None:
        paths: List[PathStruct] = []
        for path_value in reader.get_paths():
            try:
                path = PathStruct(path_value, nodes_loaded,
                                  communications_loaded)
                paths.append(path)
            except Error as e:
                logger.warning(
                    f'Failed to load path. path_name={path.path_name}. {e}')

        self._data = paths

    def __iter__(self) -> Iterator[PathStruct]:
        return iter(self._data)

    def to_value(self) -> Tuple[PathStructValue, ...]:
        return tuple(_.to_value() for _ in self._data)

    # serviceはactioに対応していないので、おかしな結果になってしまう。
    # def _insert_publishers_to_callbacks(
    #     self,
    #     publishers: List[PublisherInfo],
    #     callbacks: List[CallbackStructInfo]
    # ) -> List[CallbackStructInfo]:
    #     for publisher in publishers:
    #         if publisher.callback_name in [None]:
    #             continue

    #         callback = Util.find_one(
    #             callbacks,
    #             lambda x: x.callback_name == publisher.callback_name)
    #         callback.publishers_info.append(publisher)

    #     # automatically assign if there is only one callback.
    #     if len(callbacks) == 1:
    #         callback = callbacks[0]
    #         publisher = PublisherInfo(
    #             publisher.node_name,
    #             publisher.topic_name,
    #             callback.callback_name,
    #         )
    #         callback.publishers_info.append(publisher)

    # def _find_callback(
    #     self,
    #     node_name: str,
    #     callback_name: str
    # ) -> CallbackStructInfo:
    #     for node in self.nodes:
    #         for callback in node.callbacks:
    #             if callback.node_name == node_name and callback.callback_name == callback_name:
    #                 return callback
    #     raise ItemNotFoundError(
    #         f'Failed to find callback. node_name: {node_name}, callback_name: {callback_name}')


ChildType = Union[
    NodePathStructValue,
    CommunicationStructValue,
    TransformCommunicationStructValue
]


class PathStruct():

    def __init__(
        self,
        path_info: PathValue,
        nodes: NodesStruct,
        comms: CommunicationsStruct,
    ) -> None:
        self._val = path_info
        self._child: List[ChildType] = []

        node_paths = path_info.node_path_values

        self._child.append(nodes.get_node_path(node_paths[0]))
        for pub_node_path, sub_node_path in zip(node_paths[:-1], node_paths[1:]):
            self._child.append(comms.get(pub_node_path, sub_node_path))
            self._child.append(nodes.get_node_path(sub_node_path))

    def to_value(self) -> PathStructValue:
        child = [_.to_value() for _ in self._child]
        return PathStructValue(self.path_name, tuple(child))

    @property
    def child(self) -> List[ChildType]:
        return self._child

    @property
    def path_name(self) -> str:
        return self._val.path_name
