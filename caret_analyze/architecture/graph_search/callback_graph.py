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

from logging import getLogger
from typing import List, Optional, Union

from .graph_search import Graph, GraphNode, GraphPath
from ..struct.callback_path import CallbackPathStruct
from ..struct.struct_interface import (
    CallbackStructInterface,
    NodeInputType,
    NodeOutputType,
    NodePathsStructInterface,
    NodeStructInterface,
    VariablePassingStructInterface,
)
from ...common import Util
from ...exceptions import InvalidArgumentError, ItemNotFoundError

logger = getLogger(__name__)


class CallbackPathSearcher:

    def __init__(
        self,
        node: NodeStructInterface,
        node_paths: NodePathsStructInterface,
    ) -> None:
        self._node = node
        self._paths = node_paths

        callbacks = node.callbacks
        var_passes = node.variable_passings

        if callbacks is None or var_passes is None:
            return

        self._graph = Graph()

        for callback in callbacks:
            if callback.callback_name is None:
                continue

            write_name = self._to_node_point_name(callback.callback_name, 'write')
            read_name = self._to_node_point_name(callback.callback_name, 'read')

            self._graph.add_edge(GraphNode(read_name), GraphNode(write_name))

        for var_pass in var_passes:
            if var_pass.callback_name_read is None:
                continue
            if var_pass.callback_name_write is None:
                continue

            write_name = self._to_node_point_name(var_pass.callback_name_write, 'write')
            read_name = self._to_node_point_name(var_pass.callback_name_read, 'read')

            self._graph.add_edge(GraphNode(write_name), GraphNode(read_name))

    def search(
        self,
        start_callback: CallbackStructInterface,
        end_callback: CallbackStructInterface,
    ) -> List[CallbackPathStruct]:
        start_name = self._to_node_point_name(start_callback.callback_name, 'read')
        end_name = self._to_node_point_name(end_callback.callback_name, 'write')

        graph_paths = self._graph.search_paths(GraphNode(start_name), GraphNode(end_name))

        paths: List[CallbackPathStruct] = []
        for graph_path in graph_paths:
            in_values = start_callback.node_inputs
            out_values = end_callback.node_outputs or []

            if (out_values is None or len(out_values) == 0) and in_values is not None:
                for in_value in in_values:
                    paths.append(self._to_path(graph_path, in_value, None))

            for out_value in out_values:
                if in_values is None:
                    paths.append(self._to_path(graph_path, None, out_value))
                else:
                    for in_value in in_values:
                        paths.append(self._to_path(graph_path, in_value, out_value))
        return paths

    def _to_path(
        self,
        callbacks_graph_path: GraphPath,
        node_in_value: Optional[NodeInputType],
        node_out_value: Optional[NodeOutputType],
    ) -> CallbackPathStruct:
        child: List[Union[CallbackStructInterface, VariablePassingStructInterface]] = []

        graph_nodes = callbacks_graph_path.nodes
        graph_node_names = [_.node_name for _ in graph_nodes]

        for graph_node_from, graph_node_to in zip(graph_node_names[:-1], graph_node_names[1:]):
            cb_or_varpass = self._find_cb_or_varpass(graph_node_from, graph_node_to)
            child.append(cb_or_varpass)

        callback_path = CallbackPathStruct(self._node.node_name, child)

        return callback_path

    def _find_cb_or_varpass(
        self,
        graph_node_from: str,
        graph_node_to: str,
    ) -> Union[CallbackStructInterface, VariablePassingStructInterface]:
        read_write_name_ = self._point_name_to_read_write_name(graph_node_from)

        read_read_name = self._point_name_to_read_write_name(graph_node_to)

        if read_write_name_ == 'write' or read_read_name == 'read':
            return self._find_varpass(graph_node_from, graph_node_to)

        if read_write_name_ == 'read' or read_read_name == 'write':
            return self._find_cb(graph_node_from, graph_node_to)

        raise InvalidArgumentError('')

    def _find_varpass(
        self,
        graph_node_from: str,
        graph_node_to: str,
    ) -> VariablePassingStructInterface:

        def is_target(var_pass:  VariablePassingStructInterface):
            if graph_node_to is not None:
                read_cb_name = self._point_name_to_callback_name(graph_node_to)
                read_cb_match = var_pass.callback_name_read == read_cb_name

            if graph_node_from is not None:
                write_cb_name = self._point_name_to_callback_name(
                    graph_node_from)
                write_cb_match = var_pass.callback_name_write == write_cb_name

            if read_cb_match is None and write_cb_match is None:
                return False

            return read_cb_match and write_cb_match

        try:
            return Util.find_one(is_target, self._node.variable_passings)
        except ItemNotFoundError:
            pass
        raise ItemNotFoundError('')

    def _find_cb(
        self,
        graph_node_from: str,
        graph_node_to: str,
    ) -> CallbackStructInterface:
        def is_target(callback: CallbackStructInterface):
            callback_name = self._point_name_to_callback_name(graph_node_from)
            return callback.callback_name == callback_name

        callbacks = self._node.callbacks
        return Util.find_one(is_target, callbacks)

    @staticmethod
    def _to_node_point_name(callback_name: str, read_or_write: str) -> str:
        return f'{callback_name}@{read_or_write}'

    @staticmethod
    def _point_name_to_callback_name(point_name: str) -> str:
        return point_name.split('@')[0]

    @staticmethod
    def _point_name_to_read_write_name(point_name: str) -> str:
        return point_name.split('@')[1]
