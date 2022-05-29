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
from typing import (
    Callable,
    Collection,
    List,
    Optional,
    Tuple,
    Union,
)

from .path_searcher import EdgeBase, NodeBase, PathBase, PathSearcher
from ...value_objects import (
    CommunicationStructValue,
    NodePathStructValue,
    NodeStructValue,
    PathStructValue,
    TransformCommunicationStructValue,
)

logger = getLogger(__name__)

NodePathKey = Tuple[Optional[str], Optional[str], Optional[str]]


class CommunicationWrapper(EdgeBase):

    def __init__(
        self,
        comm: Union[TransformCommunicationStructValue, CommunicationStructValue]
    ):
        super().__init__()
        self._data = comm

    @property
    def dst_node_name(self) -> str:
        if isinstance(self._data, TransformCommunicationStructValue):
            return self._data.buffer.lookup_node_name
        else:
            return self._data.subscribe_node_name

    @property
    def src_node_name(self) -> str:
        if isinstance(self._data, TransformCommunicationStructValue):
            return self._data.broadcaster.node_name
        else:
            return self._data.publish_node_name

    @property
    def edge_name(self) -> str:
        if isinstance(self._data, TransformCommunicationStructValue):
            br = self._data.broadcaster
            return '{}[frame_id={},child_frame_id={}]'.format(
                br.topic_name,
                br.frame_id,
                br.child_frame_id,
            )
        else:
            return self._data.topic_name

    @property
    def data(self) -> Union[TransformCommunicationStructValue, CommunicationStructValue]:
        return self._data


class NodePathWrapper(NodeBase):

    def __init__(self, node_path: NodePathStructValue):
        super().__init__()

        self._data = node_path

    @property
    def dst_edge_name(self) -> Optional[str]:
        if self._data.tf_frame_broadcaster is not None:
            br = self._data.tf_frame_broadcaster
            return '{}[frame_id={},child_frame_id={}]'.format(
                br.topic_name,
                br.frame_id,
                br.child_frame_id,
            )
        else:
            return self._data.publish_topic_name

    @property
    def src_edge_name(self) -> Optional[str]:
        if self._data.tf_frame_buffer is not None:
            buff = self._data.tf_frame_buffer
            return '{}[frame_id={},child_frame_id={}]'.format(
                buff.topic_name,
                buff.listen_frame_id,
                buff.listen_child_frame_id,
            )
        else:
            return self._data.subscribe_topic_name

    @property
    def node_name(self) -> str:
        return self._data.node_name

    @property
    def data(self) -> NodePathStructValue:
        return self._data


class PathWrapper(PathBase):

    def __init__(self, child: List[Union[NodeBase, EdgeBase]]) -> None:
        super().__init__()
        self._data = [_.data for _ in child]

    @staticmethod
    def create_instance(
        child: List[Union[NodeBase, EdgeBase]]
    ) -> PathWrapper:
        return PathWrapper(child)

    @property
    def data(self) -> List[Union[NodePathStructValue, CommunicationStructValue]]:
        return self._data


class NodePathSearcher:

    def __init__(
        self,
        nodes: Tuple[NodeStructValue, ...],
        communications: Tuple[CommunicationStructValue, ...],
        node_filter: Optional[Callable[[str], bool]] = None,
        communication_filter: Optional[Callable[[str], bool]] = None,
    ) -> None:

        self._searcher = PathSearcher(PathWrapper.create_instance)

        # for node_path in Util.flatten([n.paths for n in nodes]):
        for node in nodes:
            for node_path in node.paths:
                if node_filter is not None and not node_filter(node_path.node_name):
                    continue

                node_path_wrapped = NodePathWrapper(node_path)
                self._searcher.add_node(node_path_wrapped)
        for comm in communications:
            if communication_filter is not None and \
                    not communication_filter(comm.topic_name):
                continue
            if node_filter is not None and not node_filter(comm.publish_node_name):
                continue
            if node_filter is not None and not node_filter(comm.subscribe_node_name):
                continue

            comm_wrapped = CommunicationWrapper(comm)
            self._searcher.add_edge(comm_wrapped)

    def search(
        self,
        node_names: Collection[str],
        max_node_depth: int = 10
    ) -> List[PathStructValue]:

        paths = self._searcher.search_paths(
            node_names,
            max_search_depth=max_node_depth
        )

        struct_paths = []
        for path in paths:
            struct_path = PathStructValue(None, path.data)  # type: ignore
            struct_paths.append(struct_path)

        return struct_paths
