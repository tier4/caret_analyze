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

from collections import defaultdict, UserList
from copy import deepcopy
from itertools import product
from logging import getLogger
from typing import (
    DefaultDict,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from multimethod import multimethod as singledispatchmethod

from ...exceptions import InvalidArgumentError
from ...value_objects.value_object import ValueObject

logger = getLogger(__name__)


class GraphEdgeCore(ValueObject):

    def __init__(self, i_from: int, i_to: int, label: Optional[str] = None):
        self.i_from = i_from
        self.i_to = i_to
        self.label = label or ''


class GraphPathCore(UserList):

    def __init__(self, init: List[GraphEdgeCore] = None):
        init = init or []
        super().__init__(init)

    @property
    def path(self) -> Tuple[GraphEdgeCore, ...]:
        return tuple(self.data)

    def to_graph_node_indices(self) -> List[int]:
        if len(self) == 0:
            return []

        nodes: List[int] = []
        nodes.append(self[0].i_from)

        if self[0].i_from == self[0].i_to:
            return nodes

        for edge in self:
            nodes.append(edge.i_to)

        return nodes


class GraphCore:

    def __init__(self):
        self._v = 0
        # default dictionary to store graph
        self._graph: DefaultDict[int, List[GraphEdgeCore]]
        self._graph = defaultdict(list)

    def add_edge(self, u: int, v: int, label: Optional[str] = None):
        self._v = max(self._v, u + 1, v + 1)
        self._graph[u].append(GraphEdgeCore(u, v, label))

    def _search_paths_recursion(
        self,
        u: int,
        d: int,
        edge: Optional[GraphEdgeCore],
        visited: Dict[Tuple[int, int], bool],
        path: GraphPathCore,
        paths: List[GraphPathCore],
    ) -> None:

        if edge is not None:
            path.append(edge)

        if u == d:
            paths.append(deepcopy(path))
        else:
            for edge in self._graph[u]:
                i = edge.i_to

                if visited[u, i] is False:
                    visited[u, i] = True
                    self._search_paths_recursion(
                        i, d, edge, visited, path, paths)
                    visited[u, i] = False

        if len(path) > 0:
            path.pop()

    def _search_paths(
        self,
        u: int,
        d: int,
        edge: Optional[GraphEdgeCore],
        visited: Dict[Tuple[int, int], bool],
        path: GraphPathCore,
        paths: List[GraphPathCore],
        max_depth: int = 0
    ) -> None:

        edges_cache = []
        forward = True

        def get_next_edge(u, edges_cache) -> Optional[GraphEdgeCore]:
            last_cache = edges_cache[-1]
            if 0 < max_depth and max_depth < len(path):
                return None
            while len(last_cache) > 0:
                edge = last_cache.pop()
                i = edge.i_to
                if visited[u, i] is False:
                    return edge
            return None

        u_start = u

        edges_cache.append(deepcopy(self._graph[u]))
        while True:
            if u == d and forward and len(path) > 0:
                paths.append(deepcopy(path))

            if u != d or u == u_start:
                edge = get_next_edge(u, edges_cache)
            else:
                edge = None

            if edge is not None:
                i = edge.i_to
                visited[u, i] = True
                u = i
                path.append(edge)
                edges_cache.append(deepcopy(self._graph[u]))
                forward = True
            else:
                forward = False
                edges_cache.pop()
                if len(path) > 0:
                    last_edge = path.pop()
                    u = last_edge.i_from
                    i = last_edge.i_to
                    visited[u, i] = False

                if edges_cache == []:
                    return

    def search_paths(
        self,
        start: int,
        goal: int,
        max_depth: int = 0
    ) -> List[GraphPathCore]:

        visited: Dict[Tuple[int, int], bool] = {}
        for i, j in product(range(self._v), range(self._v)):
            visited[i, j] = False

        path: GraphPathCore = GraphPathCore()
        paths: List[GraphPathCore] = []

        # self._search_paths_recursion(start, goal, None, visited, path, paths)
        self._search_paths(start, goal, None, visited, path, paths, max_depth)

        return paths


class GraphNode(ValueObject):

    def __init__(self, node_name: str) -> None:
        self.node_name = node_name


class GraphEdge(ValueObject):

    def __init__(
        self,
        node_from: GraphNode,
        node_to: GraphNode,
        label: Optional[str] = None
    ) -> None:
        self._node_from = node_from
        self._node_to = node_to
        self._label = label or ''

    @property
    def label(self) -> str:
        return self._label

    @property
    def node_from(self) -> GraphNode:
        return self._node_from

    @property
    def node_name_from(self) -> str:
        return self.node_from.node_name

    @property
    def node_to(self) -> GraphNode:
        return self._node_to

    @property
    def node_name_to(self) -> str:
        return self.node_to.node_name


class GraphPath(UserList):

    def __init__(self, init: List[GraphEdge] = None):
        init = init or []
        super().__init__(init)

    @property
    def edges(self) -> List[GraphEdge]:
        return self.data

    @property
    def nodes(self) -> List[GraphNode]:
        if len(self) == 0:
            return []

        nodes: List[GraphNode] = []
        nodes.append(self[0].node_from)

        if self[0].node_from == self[0].node_to:
            return nodes

        for edge in self:
            nodes.append(edge.node_to)

        return nodes


class Graph:

    def __init__(self) -> None:
        self._idx_to_node: Dict[int, GraphNode] = {}
        self._node_to_idx: Dict[GraphNode, int] = {}
        self._nodes: Set[GraphNode] = set()

        self._graph = GraphCore()

    def _add_node(self, node: GraphNode) -> None:
        index = len(self._nodes)
        self._idx_to_node[index] = node
        self._node_to_idx[node] = index
        self._nodes.add(node)

    def add_edge(self, node_from: GraphNode, node_to: GraphNode, label: Optional[str] = None):
        if node_from not in self._nodes:
            self._add_node(node_from)

        if node_to not in self._nodes:
            self._add_node(node_to)

        self._graph.add_edge(
            self._node_to_idx[node_from],
            self._node_to_idx[node_to],
            label
        )

    @singledispatchmethod
    def search_paths(self, args) -> List[GraphPath]:
        raise NotImplementedError('')

    @search_paths.register
    def _search_paths(
        self,
        start: GraphNode,
        goal: GraphNode,
        *,
        max_depth: Optional[int] = None
    ) -> List[GraphPath]:
        self._validate(start, goal)

        paths_core: List[GraphPathCore] = self._graph.search_paths(
            self._node_to_idx[start],
            self._node_to_idx[goal],
            max_depth or 0
        )

        paths: List[GraphPath] = []

        for path_core in paths_core:
            path = GraphPath()
            for edge_core in path_core:
                node_from = self._idx_to_node[edge_core.i_from]
                node_to = self._idx_to_node[edge_core.i_to]
                path.append(GraphEdge(node_from, node_to, edge_core.label))

            paths.append(path)
        return paths

    def _validate(self, *nodes: GraphNode) -> None:
        for node in nodes:
            if node not in self._nodes:
                raise InvalidArgumentError(
                    f'Received an unregistered graph node. Return empty paths. {node}')

    @search_paths.register
    def _search_paths_seq(
        self,
        nodes: Sequence[GraphNode],
        *,
        max_depth: Optional[int] = None
    ) -> List[GraphPath]:
        if len(nodes) < 2:
            raise InvalidArgumentError('At least two nodes are required.')

        self._validate(*nodes)

        path_cores: List[List[GraphPathCore]] = []
        for start, goal in zip(nodes[:-1], nodes[1:]):
            path_cores.append(
                self._graph.search_paths(
                    self._node_to_idx[start],
                    self._node_to_idx[goal],
                    max_depth or 0
                )
            )

        paths: List[GraphPath] = []
        for path_cores_ in product(*path_cores):
            path = GraphPath()

            for path_core in path_cores_:
                for edge_core in path_core:
                    node_from = self._idx_to_node[edge_core.i_from]
                    node_to = self._idx_to_node[edge_core.i_to]
                    path.append(GraphEdge(node_from, node_to, edge_core.label))

            paths.append(path)

        return paths
