
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .graph_search import Graph, GraphEdge, GraphNode, GraphPath
from ...exceptions import InvalidArgumentError, ItemNotFoundError

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
        self._node_dict: Dict[NodeKey, NodeBase] = {}
        self._create_path = create_path

    def add_edge(self, edge: EdgeBase) -> None:
        key = self._get_edge_key(edge.src_node_name, edge.dst_node_name, edge.edge_name)

        if key in self._edge_dict:
            logger.warning(
                'duplicated edge found. skip adding.'
                f'edge_name: {edge.edge_name}, '
                f'src_node_name: {edge.src_node_name}, '
                f'dst_node_name: {edge.dst_node_name}, ')
            return None

        self._edge_dict[key] = edge
        self._graph.add_edge(
            GraphNode(edge.src_node_name),
            GraphNode(edge.dst_node_name),
            edge.edge_name
        )

    def add_node(self, node: NodeBase):
        key = self._get_node_key(node.node_name, node.src_edge_name, node.dst_edge_name)
        if key in self._node_dict:
            logger.warning(
                'duplicated node found. skip adding. '
                f'node_name: {node.node_name}, '
                f'src_edge_name: {node.src_edge_name}, '
                f'dst_edge_name: {node.dst_edge_name}')

        self._node_dict[key] = node

    def search_paths(
        self,
        start_node_name: str,
        end_node_name: str,
        max_search_depth: Optional[int]
    ) -> List[PathBase]:

        graph_paths = self._graph.search_paths(
            GraphNode(start_node_name),
            GraphNode(end_node_name),
            max_depth=max_search_depth
        )

        return [self._to_path(path) for path in graph_paths]

    def _to_path(
        self,
        graph_path: GraphPath
    ) -> PathBase:

        child: List[Union[NodeBase, EdgeBase]] = []
        # add head node path
        if len(graph_path.edges) == 0:
            raise InvalidArgumentError("path doesn't have any edges")
        # head_node_path = self._create_head_dummy_node_path(self._nodes, graph_path.edges[0])
        head_edge = graph_path.edges[0]

        head_node_path = self._find_node(None, head_edge)

        child.append(head_node_path)

        for edge_, edge in zip(graph_path.edges[:-1], graph_path.edges[1:]):
            comm = self._find_edge(edge_)
            child.append(comm)

            node = self._find_node(edge_, edge)
            child.append(node)

        # add tail comm
        tail_edge = graph_path.edges[-1]
        comm = self._find_edge(tail_edge)
        child.append(comm)

        # add tail node path
        tail_node = self._find_node(tail_edge, None)
        child.append(tail_node)

        path_info = self._create_path(child)

        return path_info

    def _find_node(
        self,
        edge_: Optional[GraphEdge],
        edge: Optional[GraphEdge]
    ) -> NodeBase:
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
