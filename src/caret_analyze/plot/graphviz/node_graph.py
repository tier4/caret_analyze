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

from typing import Optional, Union

from graphviz import Digraph, Source

from ...architecture import Architecture
from ...runtime import Application, Communication, Node
from ...value_objects import CommunicationStructValue, NodeStructValue

ArchClass = Union[Architecture, Application]
NodeClass = Union[NodeStructValue, Node]
CommunicationClass = Union[CommunicationStructValue, Communication]


def node_graph(
    arch: ArchClass,
    export_path: Optional[str] = None,
) -> Optional[Source]:
    # it would be preferable to be able to use rqt_graph.

    dot = NodeGraph(arch).to_dot()
    source = Source(dot)
    if export_path is not None:
        file_path_wo_ext = export_path.split('.')[0]
        ext = export_path.split('.')[-1]
        source.render(file_path_wo_ext, format=ext)
        return None
    return source


class NodeGraph:
    IGNORE_NODES = ['/rviz2']
    PATH_HIGHLIGHT_COLOR = 'darkgreen'
    PATH_HIGHLIGHT_FILL_COLOR = 'darkseagreen1'

    def __init__(
        self,
        arch: ArchClass,
    ):
        self._graph = Digraph(format='svg', engine='dot')
        self._graph.name = 'Hover the mouse over a node.'
        self._graph.attr(compound='true', rankdir='LR',
                         style='rounded', newrank='true')

        self._labelled_edges = []
        self._labelled_edges.append(LabelledEdge(self, '/tf'))
        self._labelled_edges.append(LabelledEdge(self, '/tf_static'))

        self._draw_graph(arch)

    def to_dot(self):
        return self._graph.source

    def _to_ns(self, node_name: str) -> str:
        splitted = node_name.split('/')
        return '/'.join(splitted[:-1])

    def _draw_graph(self, arch: ArchClass) -> None:
        for node in arch.nodes:
            if node.node_name in NodeGraph.IGNORE_NODES:
                continue
            self._graph.node(node.node_name)

        for comm in arch.communications:
            self._draw_comm(comm)

    def _draw_comm(self, comm: CommunicationClass) -> None:
        for labelled_edge in self._labelled_edges:
            if comm.topic_name == labelled_edge.topic_name:
                labelled_edge.draw(comm)
                return
        self._draw_edge(comm)

    def _draw_edge(self, comm: CommunicationClass):
        tail_name = comm.publish_node_name
        head_name = comm.subscribe_node_name
        if (
            tail_name in NodeGraph.IGNORE_NODES
            or head_name in NodeGraph.IGNORE_NODES
        ):
            return

        color = 'black'
        penwidth = '1.0'

        self._graph.edge(tail_name, head_name,
                         label=comm.topic_name,
                         color=color,
                         penwidth=penwidth)


class LabelledEdge:

    def __init__(self, callback_graph, topic_name, color='blue'):
        self._callback_graph = callback_graph
        self.topic_name = topic_name
        self._pub_nodes = set()
        self._sub_nodes = set()
        self._color = color

    def _to_tail_name(self, comm):
        return self.topic_name + comm.subscription.node_name

    def _to_head_name(self, comm):
        return self.topic_name + comm.publisher.node_name

    @property
    def label(self) -> str:
        return f'[{self.topic_name}]'

    def draw(self, comm: CommunicationClass):
        if comm.publisher.node_name not in self._pub_nodes:
            self._pub_nodes.add(comm.publisher.node_name)
            head_name = self._to_head_name(comm)

            self._callback_graph._graph.node(
                head_name, label=self.label, color=self._color)
            self._callback_graph._draw_edge(comm)

        if comm.subscription.node_name not in self._sub_nodes:
            self._sub_nodes.add(comm.subscription.node_name)
            head_name = comm.subscribe_node_name
            tail_name = self._to_tail_name(comm)
            self._callback_graph._graph.node(
                tail_name, color=self._color, shape='cds', label=self.label
            )
            self._callback_graph._graph.edge(
                tail_name, head_name, color=self._color, label=self.label
            )
