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

from typing import List

from collections import UserList
import copy

from trace_analysis.callback import CallbackBase
from trace_analysis.node import Node
from trace_analysis.communication import Communication, VariablePassing
from trace_analysis.util import Util


class GraphNode:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, node: object) -> bool:
        if not isinstance(node, GraphNode):
            return NotImplemented
        return self.name == node.name


class GraphBranch:
    def __init__(self, src_node: GraphNode, dst_node: GraphNode):
        self.arrived = False
        self.src_node = src_node
        self.dst_node = dst_node

    def __eq__(self, branch: object) -> bool:
        if not isinstance(branch, GraphBranch):
            return NotImplemented
        return self.src_node == branch.src_node and self.dst_node == branch.dst_node


class GraphPath(UserList):
    def __init__(self, init: List[GraphBranch] = None):
        init = init or []
        super().__init__(init)

    def to_graph_nodes(self) -> List[GraphNode]:
        if len(self) == 0:
            return []

        nodes: List[GraphNode] = []
        nodes.append(self[0].src_node)
        for branch in self:
            nodes.append(branch.dst_node)

        return nodes


class GraphSearcher:
    def __init__(self, branches: List[GraphBranch]):
        self._branches = branches

    def search(self, src_node: GraphNode, dst_node: GraphNode) -> List[GraphPath]:
        def search_local(
            node: GraphNode, path: GraphPath, branches: List[GraphBranch], paths: List[GraphPath]
        ):
            if node == dst_node and len(path) > 0:
                paths.append(path)

            for branch in filter(lambda x: x.src_node == node, branches):
                if branch.arrived:
                    continue

                branches_ = copy.deepcopy(branches)
                branch_ = next(filter(lambda branch_: branch_ == branch, branches_))
                branch_.arrived = True

                path_ = copy.deepcopy(path)
                path_.append(branch)

                search_local(branch_.dst_node, path_, branches_, paths)

        paths: List[GraphPath] = []
        path = GraphPath()
        search_local(src_node, path, self._branches, paths)
        return paths


class CallbackPathSercher:
    def __init__(
        self,
        nodes: List[Node],
        communications: List[Communication],
        variable_pasisngs: List[VariablePassing],
    ) -> None:
        self._callbacks: List[CallbackBase] = Util.flatten([node.callbacks for node in nodes])
        self._communications = communications
        self._variable_passings = variable_pasisngs

    def search(
        self,
        start_callback_unique_name: str,
        end_callback_unique_name: str,
    ) -> List[List[CallbackBase]]:

        branches: List[GraphBranch] = []

        for communication in self._communications:
            src_node = GraphNode(communication.callback_publish.unique_name)
            dst_node = GraphNode(communication.callback_subscription.unique_name)
            branches.append(GraphBranch(src_node, dst_node))

        for variable_passing in self._variable_passings:
            src_node = GraphNode(variable_passing.callback_write.unique_name)
            dst_node = GraphNode(variable_passing.callback_read.unique_name)
            branches.append(GraphBranch(src_node, dst_node))

        searcher = GraphSearcher(branches)

        src_node = GraphNode(start_callback_unique_name)
        dst_node = GraphNode(end_callback_unique_name)
        graph_paths: List[GraphPath] = searcher.search(src_node, dst_node)

        paths: List[List[CallbackBase]] = [self._to_path(path) for path in graph_paths]
        return paths

    def _to_path(self, graph_path: GraphPath) -> List[CallbackBase]:
        to_callback = {callback.unique_name: callback for callback in self._callbacks}
        callbacks_path: List[CallbackBase] = [
            to_callback[node.name] for node in graph_path.to_graph_nodes()
        ]
        return callbacks_path
