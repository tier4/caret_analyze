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
from typing import Callable, DefaultDict, Dict, List, Optional, Set, Tuple, Union

from ..common import Util
from ..exceptions import (InvalidArgumentError, ItemNotFoundError,
                          MultipleItemFoundError)
from ..value_objects import (CallbackStructValue, CommunicationStructValue,
                             NodePathStructValue, NodeStructValue,
                             PathStructValue, PublisherStructValue,
                             SubscriptionStructValue,
                             VariablePassingStructValue)
from ..value_objects.value_object import ValueObject

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

    def add_node(self, node: GraphNode) -> None:
        index = len(self._nodes)
        self._idx_to_node[index] = node
        self._node_to_idx[node] = index
        self._nodes.add(node)

    def add_edge(self, node_from: GraphNode, node_to: GraphNode, label: Optional[str] = None):
        if node_from not in self._nodes:
            self.add_node(node_from)

        if node_to not in self._nodes:
            self.add_node(node_to)

        self._graph.add_edge(
            self._node_to_idx[node_from],
            self._node_to_idx[node_to],
            label
        )

    def _validate(self, *nodes: GraphNode) -> None:
        for node in nodes:
            if node not in self._nodes:
                raise ItemNotFoundError(
                    f'Received an unregistered graph node. Return empty paths. {node}')

    def search_paths(
        self,
        *nodes: GraphNode,
        max_depth: Optional[int] = None
    ) -> List[GraphPath]:
        if len(nodes) < 2:
            raise InvalidArgumentError('nodes must be at least 2')

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


class CallbackPathSearcher:

    def __init__(
        self,
        node: NodeStructValue,
    ) -> None:
        self._node = node

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
        start_callback: CallbackStructValue,
        end_callback: CallbackStructValue,
    ) -> Tuple[NodePathStructValue, ...]:
        # src_node = GraphNode(self._to_node_point_name(start_callback.callback_name, 'write'))
        # dst_node = GraphNode(self._to_node_point_name(end_callback.callback_name, 'read'))

        start_name = self._to_node_point_name(
            start_callback.callback_name, 'read')
        end_name = self._to_node_point_name(
            end_callback.callback_name, 'write')

        graph_paths = self._graph.search_paths(GraphNode(start_name), GraphNode(end_name))

        paths: List[NodePathStructValue] = []
        for graph_path in graph_paths:
            paths += self._to_paths(graph_path, start_callback, end_callback)

        return tuple(paths)

    def _to_paths(
        self,
        callback_graph_path: GraphPath,
        start_callback: CallbackStructValue,
        end_callback: CallbackStructValue,
    ) -> List[NodePathStructValue]:
        subscribe_topic_name = start_callback.subscribe_topic_name

        if end_callback.publish_topic_names is None or \
           end_callback.publish_topic_names == ():
            return [self._to_path(callback_graph_path, subscribe_topic_name, None)]

        return [
            self._to_path(callback_graph_path,
                          subscribe_topic_name, publish_topic_name)
            for publish_topic_name
            in end_callback.publish_topic_names
        ]

    def _to_path(
        self,
        callbacks_graph_path: GraphPath,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
    ) -> NodePathStructValue:
        child: List[Union[CallbackStructValue, VariablePassingStructValue]] = []

        graph_nodes = callbacks_graph_path.nodes
        graph_node_names = [_.node_name for _ in graph_nodes]

        for graph_node_from, graph_node_to in zip(graph_node_names[:-1], graph_node_names[1:]):
            cb_or_varpass = self._find_cb_or_varpass(
                graph_node_from, graph_node_to)
            child.append(cb_or_varpass)

        sub: Optional[SubscriptionStructValue] = None
        pub: Optional[PublisherStructValue] = None

        if subscribe_topic_name is not None:
            try:
                sub = self._node.get_subscription(subscribe_topic_name)
            except ItemNotFoundError:
                msg = 'Failed to find subscription. '
                msg += f'node_name: {self._node.node_name}, '
                msg += f'topic_name: {subscribe_topic_name}'
                logger.warning(msg)
            except MultipleItemFoundError:
                msg = 'Failed to identify subscription. Several candidates were found. '
                msg += f'node_name: {self._node.node_name}, '
                msg += f'topic_name: {subscribe_topic_name}'
                logger.warning(msg)

        if publish_topic_name is not None:
            try:
                pub = self._node.get_publisher(publish_topic_name)
            except ItemNotFoundError:
                msg = 'Failed to find publisher. '
                msg += f'node_name: {self._node.node_name}'
                msg += f'topic_name: {publish_topic_name}'
                logger.warning(msg)
            except MultipleItemFoundError:
                msg = 'Failed to identify publisher. Several candidates were found. '
                msg += f'node_name: {self._node.node_name}, '
                msg += f'topic_name: {publish_topic_name}'
                logger.warning(msg)

        return NodePathStructValue(
            self._node.node_name,
            sub,
            pub,
            tuple(child),
            None)

    def _find_cb_or_varpass(
        self,
        graph_node_from: str,
        graph_node_to: str,
    ) -> Union[CallbackStructValue, VariablePassingStructValue]:
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
    ) -> VariablePassingStructValue:

        def is_target(var_pass:  VariablePassingStructValue):
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
    ) -> CallbackStructValue:
        def is_target(callback: CallbackStructValue):
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


NodePathKey = Tuple[Optional[str], Optional[str], Optional[str]]


class NodePathSearcher:

    def __init__(
        self,
        nodes: Tuple[NodeStructValue, ...],
        communications: Tuple[CommunicationStructValue, ...],
        node_filter: Optional[Callable[[str], bool]] = None,
        communication_filter: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self._nodes = nodes
        self._comms = communications

        self._graph = Graph()

        self._node_path_dict: Dict[NodePathKey, NodePathStructValue] = {}
        self._comm_dict: Dict[Tuple[str, str, str], CommunicationStructValue] = {}

        node_paths: List[NodePathStructValue] = Util.flatten([n.paths for n in self._nodes])
        for node_path in node_paths:
            key = self._node_path_key(
                node_path.subscribe_topic_name, node_path.publish_topic_name, node_path.node_name
            )
            if key not in self._node_path_dict:
                self._node_path_dict[key] = node_path
            else:
                logger.warning(
                    'duplicated node_path found. skip adding. '
                    f'node_name: {node_path.node_name}, '
                    f'subscribe_topic_name: {node_path.subscribe_topic_name}, '
                    f'publish_topic_name: {node_path.publish_topic_name}')

        for node in nodes:
            if node_filter is not None and \
                    not node_filter(node.node_name):
                continue

            self._graph.add_node(GraphNode(node.node_name))

        for comm in communications:
            if communication_filter is not None and \
                    not communication_filter(comm.topic_name):
                continue
            if node_filter is not None and not node_filter(comm.publish_node_name):
                continue
            if node_filter is not None and not node_filter(comm.subscribe_node_name):
                continue

            key = self._comm_key(comm.publish_node_name, comm.subscribe_node_name, comm.topic_name)
            if key not in self._comm_dict:
                self._comm_dict[key] = comm
            else:
                logger.warning(
                    'duplicated communication found. skip adding.'
                    f'topic_name: {comm.topic_name}, '
                    f'publish_node_name: {comm.publish_node_name}, '
                    f'subscribe_node_name: {comm.subscribe_node_name}, ')
                continue

            self._graph.add_edge(
                GraphNode(comm.publish_node_name),
                GraphNode(comm.subscribe_node_name),
                comm.topic_name
            )

    @staticmethod
    def _comm_key(
        publish_node_name: str,
        subscribe_node_name: str,
        topic_name: str
    ) -> Tuple[str, str, str]:
        return (publish_node_name, subscribe_node_name, topic_name)

    @staticmethod
    def _node_path_key(
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        node_name: Optional[str]
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        return (subscribe_topic_name, publish_topic_name, node_name)

    def search(
        self,
        *node_names: str,
        max_node_depth: Optional[int] = None
    ) -> List[PathStructValue]:
        paths: List[PathStructValue] = []

        max_search_depth = max_node_depth or 0

        graph_nodes: List[GraphNode] = [GraphNode(node) for node in node_names]
        graph_paths = self._graph.search_paths(
            *graph_nodes,
            max_depth=max_search_depth)

        for graph_path in graph_paths:
            paths.append(self._to_path(graph_path))

        return paths

    def _find_node(self, node_name: str) -> NodeStructValue:
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self._nodes)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}. '
            raise ItemNotFoundError(msg)

    @staticmethod
    def _get_publisher(
        nodes: Tuple[NodeStructValue, ...],
        node_name: str,
        topic_name: str
    ) -> PublisherStructValue:
        node: NodeStructValue
        node = Util.find_one(lambda x: x.node_name == node_name, nodes)
        return node.get_publisher(topic_name)

    @staticmethod
    def _get_subscription(
        nodes: Tuple[NodeStructValue, ...],
        node_name: str,
        topic_name: str
    ) -> SubscriptionStructValue:
        node: NodeStructValue
        node = Util.find_one(lambda x: x.node_name == node_name, nodes)
        return node.get_subscription(topic_name)

    @staticmethod
    def _create_head_dummy_node_path(
        nodes: Tuple[NodeStructValue, ...],
        head_edge: GraphEdge
    ) -> NodePathStructValue:
        node_name = head_edge.node_name_from
        topic_name = head_edge.label
        publisher = NodePathSearcher._get_publisher(nodes, node_name, topic_name)
        return NodePathStructValue(node_name, None, publisher, None, None)

    @staticmethod
    def _create_tail_dummy_node_path(
        nodes: Tuple[NodeStructValue, ...],
        tail_edge: GraphEdge,
    ) -> NodePathStructValue:
        node_name = tail_edge.node_name_to
        topic_name: str = tail_edge.label
        sub = NodePathSearcher._get_subscription(nodes, node_name, topic_name)
        return NodePathStructValue(node_name, sub, None, None, None)

    def _to_path(
        self,
        node_graph_path: GraphPath,
    ) -> PathStructValue:
        child: List[Union[NodePathStructValue, CommunicationStructValue]] = []

        # add head node path
        if len(node_graph_path.edges) == 0:
            raise InvalidArgumentError("path doesn't have any edges")
        head_node_path = self._create_head_dummy_node_path(self._nodes, node_graph_path.edges[0])
        child.append(head_node_path)

        for edge_, edge in zip(node_graph_path.edges[:-1], node_graph_path.edges[1:]):
            comm = self._find_comm(
                edge_.node_from.node_name,
                edge_.node_to.node_name,
                edge_.label)
            child.append(comm)

            node = self._find_node_path(
                edge_.label,
                edge.label,
                edge_.node_to.node_name,
            )

            child.append(node)

        # add tail comm
        tail_edge = node_graph_path.edges[-1]
        comm = self._find_comm(
            tail_edge.node_name_from,
            tail_edge.node_name_to,
            tail_edge.label)
        child.append(comm)

        # add tail node path
        tail_node_path = self._create_tail_dummy_node_path(self._nodes, tail_edge)
        child.append(tail_node_path)

        path_info = PathStructValue(
            None,
            tuple(child)
        )
        return path_info

    def _find_comm(
        self,
        node_from: str,
        node_to: str,
        topic_name: str
    ) -> CommunicationStructValue:
        key = self._comm_key(node_from, node_to, topic_name)
        if key not in self._comm_dict:
            msg = 'Failed to find communication path. '
            msg += f'publish node name: {node_from}, '
            msg += f'subscription node name: {node_to}, '
            msg += f'topic name: {topic_name}, '
            raise ItemNotFoundError(msg)

        return self._comm_dict[key]

    def _find_node_path(
        self,
        sub_topic_name: str,
        pub_topic_name: str,
        node_name: str,
    ) -> NodePathStructValue:
        key = self._node_path_key(sub_topic_name, pub_topic_name, node_name)
        if key not in self._node_path_dict:
            msg = 'Failed to find node path. '
            msg += f'publish topic name: {pub_topic_name}, '
            msg += f'subscription topic name: {sub_topic_name}, '
            msg += f'node name: {node_name}, '
            raise ItemNotFoundError(msg)
        return self._node_path_dict[key]
