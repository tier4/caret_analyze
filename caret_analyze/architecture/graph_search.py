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

from collections import UserList
from copy import deepcopy
from itertools import product
from typing import List, Optional, Tuple, Union, Dict, Set, DefaultDict
from logging import getLogger
from collections import defaultdict

from ..exceptions import InvalidArgumentError, ItemNotFoundError, MultipleItemFoundError
from ..common import Util
from ..value_objects import (CallbackStructValue,
                             CommunicationStructValue,
                             NodePathStructValue,
                             NodeStructValue,
                             PathStructValue,
                             PublisherStructValue,
                             SubscriptionStructValue,
                             VariablePassingStructValue,
                             CallbackChain)

from ..value_objects.value_object import ValueObject


logger = getLogger(__name__)


class GraphEdgeCore(ValueObject):

    def __init__(self, i_from: int, i_to: int, label: Optional[str] = None):
        self.i_from = i_from
        self.i_to = i_to
        self.label = label


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

        edges_cache.append(deepcopy(self._graph[u]))
        while True:
            if u == d and forward:
                paths.append(deepcopy(path))

            edge = get_next_edge(u, edges_cache)

            if edge is not None:
                i = edge.i_to
                visited[u, i] = True
                u = i
                path.append(edge)
                edges_cache.append(deepcopy(self._graph[u]))
                forward = True
                # self._search_paths_recursion(i, d, edge, visited, path, paths)
                # visited[u, i] = False
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
        self.node_from = node_from
        self.node_to = node_to
        self.label = label


class GraphPath(UserList):

    def __init__(self, init: List[GraphEdge] = None):
        init = init or []
        super().__init__(init)

    @property
    def path(self) -> Tuple[GraphEdge, ...]:
        return tuple(self.data)

    def to_graph_nodes(self) -> List[GraphNode]:
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

    def search_paths(
        self,
        start: GraphNode,
        goal: GraphNode,
        max_depth: Optional[int] = None
    ) -> List[GraphPath]:
        if start not in self._nodes:
            logger.info(f'Received an unregistered graph node. Return empty paths. {start}')
            return []

        if goal not in self._nodes:
            logger.info(f'Received an unregistered graph node. Return empty paths. {goal}')
            return []

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

        graph_nodes = callbacks_graph_path.to_graph_nodes()
        graph_node_names = [_.node_name for _ in graph_nodes]

        for graph_node_from, graph_node_to in zip(graph_node_names[:-1], graph_node_names[1:]):
            cb_or_varpass = self._find_cb_or_varpass(
                graph_node_from, graph_node_to)
            child.append(cb_or_varpass)

        sub_info: Optional[SubscriptionStructValue] = None
        pub_info: Optional[PublisherStructValue] = None

        if subscribe_topic_name is not None:
            try:
                sub_info = self._node.get_subscription(subscribe_topic_name)
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
                pub_info = self._node.get_publisher(publish_topic_name)
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

        callbacks = Util.filter(lambda x: isinstance(x, CallbackStructValue), child)
        callback_names = tuple(_.callback_name for _ in callbacks)
        message_context = CallbackChain(
            subscribe_topic_name, publish_topic_name, callback_names)
        return NodePathStructValue(
            self._node.node_name,
            sub_info,
            pub_info,
            tuple(child),
            message_context)

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


class NodePathSearcher:

    def __init__(
        self,
        nodes: Tuple[NodeStructValue, ...],
        communications: Tuple[CommunicationStructValue, ...],
    ) -> None:
        self._nodes = nodes
        self._comms = communications

        self._graph = Graph()

        for node in self._nodes:
            node_name = node.node_name
            for node_path in node._node_path_values:
                pub_topic_name = node_path.publish_topic_name or ''
                sub_topic_name = node_path.subscribe_topic_name or ''
                pub_name = NodePathSearcher._to_node_point_name(
                    node_name, pub_topic_name, 'pub')
                sub_name = NodePathSearcher._to_node_point_name(
                    node_name, sub_topic_name, 'sub')

                self._graph.add_edge(GraphNode(sub_name), GraphNode(pub_name))

        for comm in communications:
            pub_name = NodePathSearcher._to_node_point_name(
                comm.publish_node_name, comm.topic_name, 'pub')
            sub_name = NodePathSearcher._to_node_point_name(
                comm.subscribe_node_name, comm.topic_name, 'sub')

            self._graph.add_edge(
                GraphNode(pub_name),
                GraphNode(sub_name),
                comm.topic_name
            )

    def search(
        self,
        start_node_name: str,
        end_node_name: str,
        max_node_depth: Optional[int]
    ) -> List[PathStructValue]:
        paths: List[PathStructValue] = []

        # Create a graph node that is the search startpoint.
        # There are a number of start_node publishers.
        src_graph_nodes = self._to_src_graph_nodes(start_node_name)

        # Create a graph node that is the search endpoint.
        # There are a number of end_node subscriptions.
        dst_graph_nodes = self._to_dst_graph_nodes(end_node_name)

        max_search_depth = (max_node_depth or 0) * 2

        for src_node, dst_node in product(src_graph_nodes, dst_graph_nodes):
            graph_paths = self._graph.search_paths(
                GraphNode(src_node),
                GraphNode(dst_node),
                max_search_depth)

            paths += [
                self._to_path(graph_path)
                for graph_path
                in graph_paths
            ]

        return paths

    def _to_src_graph_nodes(self, node_name: str) -> List[str]:
        src_ros_node = self._find_node(node_name)
        return [
            self._to_node_point_name(node_name, topic_name, 'pub')
            for topic_name
            in src_ros_node.publish_topic_names]

    def _to_dst_graph_nodes(self, node_name: str) -> List[str]:
        node = self._find_node(node_name)
        return [
            self._to_node_point_name(node_name, topic_name, 'sub')
            for topic_name
            in node.subscribe_topic_names]

    def _find_node(self, node_name: str) -> NodeStructValue:
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self._nodes)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}. '
            raise ItemNotFoundError(msg)

    def _get_publisher(
        self,
        node_name: str,
        topic_name: str
    ) -> PublisherStructValue:
        node: NodeStructValue
        node = Util.find_one(lambda x: x.node_name == node_name, self._nodes)
        return node.get_publisher(topic_name)

    def _get_subscription(
        self,
        node_name: str,
        topic_name: str
    ) -> SubscriptionStructValue:
        node: NodeStructValue
        node = Util.find_one(lambda x: x.node_name == node_name, self._nodes)
        return node.get_subscription(topic_name)

    def _to_path(
        self,
        node_graph_path: GraphPath,
    ) -> PathStructValue:
        child: List[Union[NodePathStructValue, CommunicationStructValue]] = []

        graph_nodes = node_graph_path.to_graph_nodes()
        graph_node_names = [_.node_name for _ in graph_nodes]

        # add head node path
        head_node_name = self._point_name_to_node_name(graph_node_names[0])
        head_topic_name = self._point_name_to_topic_name(graph_node_names[0])
        child.append(
            NodePathStructValue(
                head_node_name,
                None,
                self._get_publisher(head_node_name, head_topic_name),
                None, None
            )
        )

        for graph_node_from, graph_node_to in zip(graph_node_names[:-1], graph_node_names[1:]):
            node_or_comm = self._find_comm_or_node(graph_node_from, graph_node_to)
            child.append(node_or_comm)

        # add tail node path
        tail_node_name = self._point_name_to_node_name(graph_node_names[-1])
        tail_topic_name = self._point_name_to_topic_name(graph_node_names[-1])
        child.append(
            NodePathStructValue(
                tail_node_name,
                self._get_subscription(tail_node_name, tail_topic_name),
                None, None, None
            )
        )

        path_info = PathStructValue(
            None,
            tuple(child)
        )
        return path_info

    def _find_comm_or_node(
        self,
        graph_node_from: str,
        graph_node_to: str,
    ) -> Union[NodePathStructValue, CommunicationStructValue]:
        pub_sub_name_, pub_sub_name = None, None

        if graph_node_from is not None:
            pub_sub_name_ = self._point_name_to_pub_sub_name(graph_node_from)
        if graph_node_to is not None:
            pub_sub_name = self._point_name_to_pub_sub_name(graph_node_to)

        if pub_sub_name_ == 'pub' or pub_sub_name == 'sub':
            # communication case
            pub_node_name, sub_node_name, topic_name = None, None, None

            if graph_node_from is not None:
                pub_node_name = self._point_name_to_node_name(graph_node_from)
                topic_name = self._point_name_to_topic_name(graph_node_from)

            if graph_node_to is not None:
                sub_node_name = self._point_name_to_node_name(graph_node_to)
                topic_name = self._point_name_to_topic_name(graph_node_to)

            return self._find_comm(sub_node_name, pub_node_name, topic_name)

        elif pub_sub_name_ == 'sub' or pub_sub_name == 'pub':
            # node path case
            sub_topic_name, pub_topic_name, node_name = None, None, None

            if graph_node_from is not None:
                node_name = self._point_name_to_node_name(graph_node_from)
                sub_topic_name = self._point_name_to_topic_name(graph_node_from)

            if graph_node_to is not None:
                node_name = self._point_name_to_node_name(graph_node_to)
                pub_topic_name = self._point_name_to_topic_name(graph_node_to)

            return self._find_node_path(sub_topic_name, pub_topic_name, node_name)

        else:
            msg = 'Unexpected arguments were given. '
            node_from = str(None) if graph_node_from is None else graph_node_from
            msg += f'node_from: {node_from}. '
            node_to = str(None) if graph_node_to is None else graph_node_to
            msg += f'node_to: {node_to}. '
            raise InvalidArgumentError(msg)

    def _find_comm(
        self,
        sub_node_name: str,
        pub_node_name: str,
        topic_name: str,
    ) -> CommunicationStructValue:
        def is_target(comm: CommunicationStructValue):
            pub_match = comm.publish_node_name == pub_node_name
            sub_match = comm.subscribe_node_name == sub_node_name
            topic_match = comm.topic_name == topic_name
            return pub_match and sub_match and topic_match

        try:
            return Util.find_one(is_target, self._comms)
        except ItemNotFoundError:
            msg = 'Failed to find communication path. '
            msg += f'publish node name: {pub_node_name}, '
            msg += f'subscription node name: {sub_node_name}, '
            msg += f'topic name: {topic_name}, '
            raise ItemNotFoundError(msg)

    def _find_node_path(
        self,
        sub_topic_name: str,
        pub_topic_name: str,
        node_name: str,
    ) -> NodePathStructValue:
        def is_target(node_path: NodePathStructValue):
            sub_match = node_path.subscribe_topic_name == sub_topic_name
            if sub_topic_name is None:
                sub_match = True
            pub_match = node_path.publish_topic_name == pub_topic_name
            if pub_topic_name is None:
                pub_match = True
            node_match = node_path.node_name == node_name
            if node_name is None:
                node_match = True
            return sub_match and pub_match and node_match

        try:
            node_paths = Util.flatten([n.paths for n in self._nodes])
            return Util.find_one(is_target, node_paths)
        except ItemNotFoundError:
            msg = 'Failed to find node path. '
            msg += f'publish topic name: {pub_topic_name}, '
            msg += f'subscription topic name: {sub_topic_name}, '
            msg += f'node name: {node_name}, '
            raise ItemNotFoundError(msg)

    @staticmethod
    def _to_node_point_name(node_name: str, topic_name: str, pub_or_sub: str) -> str:
        return f'{node_name}@{topic_name}@{pub_or_sub}'

    @staticmethod
    def _point_name_to_node_name(point_name: str) -> str:
        return point_name.split('@')[0]

    @staticmethod
    def _point_name_to_topic_name(point_name: str) -> str:
        return point_name.split('@')[1]

    @staticmethod
    def _point_name_to_pub_sub_name(point_name: str) -> str:
        return point_name.split('@')[2]
