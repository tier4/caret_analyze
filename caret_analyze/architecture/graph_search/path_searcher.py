
from __future__ import annotations
from collections import defaultdict

from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, DefaultDict, Collection

from .graph_search import Graph, GraphEdge, GraphNode, GraphPath
from ...exceptions import InvalidArgumentError, ItemNotFoundError
from ...common import Util
from itertools import product

logger = getLogger(__name__)

EdgeKey = Tuple[str, str, str]
NodeKey = Tuple[str, Optional[str], Optional[str]]


class PathSearcher():

    def __init__(
        self,
        create_path: Callable[[List[Union[NodeBase, EdgeBase]]], PathBase]
    ) -> None:
        self._graph = Graph()
        self._edge_dict: Dict[EdgeKey, EdgeBase] = {}
        self._node_dict: DefaultDict[NodeKey, List[NodeBase]] = defaultdict(list)
        self._create_path = create_path

    def add_edge(self, edge: EdgeBase) -> None:
        key = self._get_edge_key(edge.src_node_name, edge.dst_node_name, edge.edge_name)

        if key in self._edge_dict:
            return None

        self._edge_dict[key] = edge
        self._graph.add_edge(
            GraphNode(edge.src_node_name),
            GraphNode(edge.dst_node_name),
            edge.edge_name
        )

    def add_node(self, node: NodeBase):
        key = self._get_node_key(node.node_name, node.src_edge_name, node.dst_edge_name)
        self._node_dict[key].append(node)

    # @singledispatchmethod
    # def search_paths(self, args) -> List[PathBase]:
    #     raise NotImplementedError('')

    # @search_paths.register
    # def _search_paths(
    #     self,
    #     start_node_name: str,
    #     end_node_name: str,
    #     max_search_depth: Optional[int]
    # ) -> List[PathBase]:
    #     return self._search_paths_seq(
    #         start_node_name, end_node_name, max_search_depth=max_search_depth)

    def search_paths(
        self,
        node_names: Collection[str],
        max_search_depth: Optional[int]
    ) -> List[PathBase]:

        nodes = [GraphNode(node_name) for node_name in node_names]
        graph_paths = self._graph.search_paths(
            nodes,
            max_depth=max_search_depth
        )

        return Util.flatten([self._to_paths(path) for path in graph_paths])

    def _to_paths(
        self,
        graph_path: GraphPath
    ) -> List[PathBase]:

        # add head node path
        if len(graph_path.edges) == 0:
            raise InvalidArgumentError("path doesn't have any edges")
        # head_node_path = self._create_head_dummy_node_path(self._nodes, graph_path.edges[0])
        # node_edge = node_edges[0]

        nodes: List[List[NodeBase]] = []

        nodes.append(self._find_nodes(None, graph_path.edges[0]))
        for edge_, edge in zip(graph_path.edges[:-1], graph_path.edges[1:]):
            nodes.append(self._find_nodes(edge_, edge))
        nodes.append(self._find_nodes(graph_path.edges[-1], None))

        paths: List[PathBase] = []
        for path_nodes in product(*nodes):
            child: List[Union[NodeBase, EdgeBase]] = []
            child.append(path_nodes[0])
            for edge, node in zip(graph_path.edges, path_nodes[1:]):
                child.append(self._find_edge(edge))
                child.append(node)
            paths.append(self._create_path(child))

        return paths

    def _find_nodes(
        self,
        edge_: Optional[GraphEdge],
        edge: Optional[GraphEdge]
    ) -> List[NodeBase]:
        node_name: str
        dst_edge_name: Optional[str] = None
        src_edge_name: Optional[str] = None

        assert edge_ is not None or edge is not None

        if edge_ is not None:
            node_name = edge_.node_name_to
            src_edge_name = edge_.label

        if edge is not None:
            node_name = edge.node_name_from
            dst_edge_name = edge.label

        key = self._get_node_key(
            dst_edge_name=dst_edge_name,
            src_edge_name=src_edge_name,
            node_name=node_name)
        assert key in self._node_dict
        return self._node_dict[key]

    def _find_edge(
        self,
        edge: GraphEdge
    ) -> EdgeBase:
        key = self._get_edge_key(edge.node_name_from, edge.node_name_to, edge.label)
        if key not in self._edge_dict:
            msg = 'Failed to find communication path. '
            # msg += f'publish node name: {node_from}, '
            # msg += f'subscription node name: {node_to}, '
            # msg += f'topic name: {topic_name}, '
            raise ItemNotFoundError(msg)

        return self._edge_dict[key]

    @staticmethod
    def _get_edge_key(
        src_node_name: str,
        dst_node_name: str,
        edge_name: str
    ) -> EdgeKey:
        return (src_node_name, dst_node_name, edge_name)

    @staticmethod
    def _get_node_key(
        node_name: str,
        src_edge_name: Optional[str],
        dst_edge_name: Optional[str]
    ) -> NodeKey:
        return (node_name, src_edge_name, dst_edge_name)


class EdgeBase(metaclass=ABCMeta):

    @property
    @abstractmethod
    def src_node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def dst_node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def edge_name(self) -> str:
        pass

    @property
    @abstractmethod
    def data(self) -> Any:
        pass


class NodeBase(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def src_edge_name(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def dst_edge_name(self) -> Optional[str]:
        pass

    @property
    @abstractmethod
    def data(self) -> Any:
        pass


class PathBase(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def create_instance(
        child: List[Union[NodeBase, EdgeBase]]
    ) -> PathBase:
        pass

    @property
    @abstractmethod
    def data(self) -> List[Any]:
        pass
