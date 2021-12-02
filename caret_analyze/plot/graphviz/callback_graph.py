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

from typing import List, Optional, Union

from graphviz import Digraph, Source

from ...architecture import Architecture
from ...runtime.callback import CallbackBase, SubscriptionCallback, TimerCallback
from ...runtime.node import Node
from ...runtime.path import Path
from ...common import Util


def callback_graph(
    arch: Architecture,
    callbacks: List[CallbackBase],
    export_path: Optional[str] = None,
    separate: bool = False,
    plot_target_path_only: bool = False
) -> Optional[Source]:
    if plot_target_path_only and len(callbacks) == 0:
        print('Failed to find specified path. Ignore plot_target_path_only argument.')
        plot_target_path_only = False

    dot = CallbackGraph(arch, callbacks, separate, plot_target_path_only).to_dot()
    source = Source(dot)
    if export_path is not None:
        file_path_wo_ext = export_path.split('.')[0]
        ext = export_path.split('.')[-1]
        source.render(file_path_wo_ext, format=ext)
        return None
    return source


class CallbackGraph:
    IGNORE_NODES = ['/rviz2']
    PATH_HIGHLIGHT_COLOR = 'darkgreen'
    PATH_HIGHLIGHT_FILL_COLOR = 'darkseagreen1'

    def __init__(
        self,
        arch: Architecture,
        callbacks: List[CallbackBase],
        separate: bool,
        plot_target_path_only: bool
    ):
        self._arch = arch
        path = Path(callbacks, arch.communications, arch.variable_passings)

        self._graph = Digraph(format='svg', engine='dot')
        self._graph.name = 'Hover the mouse over a callback.'
        self._graph.attr(compound='true', rankdir='LR',
                         style='rounded', newrank='true')

        self._labelled_edges = []
        self._labelled_edges.append(LabelledEdge(self, '/tf'))
        self._labelled_edges.append(LabelledEdge(self, '/tf_static'))

        if separate:
            for comm in arch.communications:
                if comm.topic_name in [_.topic_name for _ in self._labelled_edges]:
                    continue
                self._labelled_edges.append(LabelledEdge(self, comm.topic_name))

        self._draw_graph(path, plot_target_path_only)

    def to_dot(self):
        return self._graph.source

    def _to_ns(self, node_name: str) -> str:
        splitted = node_name.split('/')
        return '/'.join(splitted[:-1])

    def _draw_graph(self, path: Path, plot_target_path_only: bool) -> None:
        for node in self._arch.nodes:
            if node.node_name in CallbackGraph.IGNORE_NODES:
                continue
            if plot_target_path_only and not self._contain(node, path):
                continue
            self._draw_node(node, path)

        for comm in self._arch.communications:
            pub_node: Node = Util.find_one(
                self._arch.nodes, lambda x: x.node_name == comm.publisher.node_name
            )
            sub_node: Node = Util.find_one(
                self._arch.nodes, lambda x: x.node_name == comm.subscription.node_name
            )

            if plot_target_path_only and \
               (not self._contain(pub_node, path) or not self._contain(sub_node, path)):
                continue
            self._draw_comm(comm, path.contains(comm))  # highlight

        for var in self._arch.variable_passings:
            node_var: Node = Util.find_one(
                self._arch.nodes, lambda x: x.node_name == var.callback_to.node_name
            )
            if plot_target_path_only and not self._contain(node_var, path):
                continue

            head_name = var.callback_to.callback_name
            tail_name = var.callback_from.callback_name
            if path.contains(var):
                color = CallbackGraph.PATH_HIGHLIGHT_COLOR
                penwidth = '4.0'
            else:
                color = 'black'
                penwidth = '1.0'
            self._graph.edge(tail_name, head_name, color=color, penwidth=penwidth)

    def _contain(self, node: Node, path: Path) -> bool:
        for callback in node.callbacks:
            if path.contains(callback):
                return True
        return False

    def _draw_comm(self, comm, highlight: bool) -> None:
        for labelled_edge in self._labelled_edges:
            if comm.topic_name == labelled_edge.topic_name:
                labelled_edge.draw(comm)
                return
        head_name = comm.callback_to.name
        head_node_name = comm.callback_to.node_name
        self._draw_edge(comm, head_name, head_node_name, highlight)

    def _get_tooltip(self, callback: CallbackBase) -> str:
        if isinstance(callback, TimerCallback):
            period_ns_str = '{:,}'.format(callback.period_ns)
            return f'period ns: {period_ns_str}\n symbol: {callback.symbol}'

        elif isinstance(callback, SubscriptionCallback):
            return f'topic name: {callback.topic_name}\n symbol: {callback.symbol}'

        assert False, 'not implemented'

    def _draw_callbacks(self, node_cluster, node: Node, path: Path):
        if len(node.callbacks) == 0:
            node_cluster.node(
                node.node_name,
                ' ',
                _attributes={'shape': 'box', 'style': 'filled'},
                color='white',
                fillcolor='white',
            )
            return

        for callback in node.callbacks:
            tooltip: str = self._get_tooltip(callback)
            if path.contains(callback):
                color = CallbackGraph.PATH_HIGHLIGHT_COLOR
                fillcolor = CallbackGraph.PATH_HIGHLIGHT_FILL_COLOR
            else:
                color = 'black'
                fillcolor = 'white'

            node_cluster.node(
                callback.callback_name,
                callback.callback_name,
                _attributes={'shape': 'box',
                             'tooltip': tooltip, 'style': 'filled'},
                color=color,
                fillcolor=fillcolor,
            )

    def _draw_node(self, node: Node, path: Path) -> None:
        # Nesting clusters is not available due to an error in graphviz.
        # Graphviz-2.49.1 used for verification.

        # draw node cluster
        with self._graph.subgraph(
            name=self._to_cluster_name(node.node_name, prefix='node_'),
            graph_attr={'label': node.node_name,
                        'bgcolor': 'white', 'color': 'black'},
        ) as node_cluster:

            self._draw_callbacks(node_cluster, node, path)

    def _draw_callback_to_callback(self, comm, head_name, head_node_name, highlight: bool):
        tail_name = comm.callback_from.name
        tail_node_name = comm.callback_from.node_name
        if (
            tail_node_name in CallbackGraph.IGNORE_NODES
            or head_node_name in CallbackGraph.IGNORE_NODES
        ):
            return

        if highlight:
            color = CallbackGraph.PATH_HIGHLIGHT_COLOR
            penwidth = '4.0'
        else:
            color = 'black'
            penwidth = '1.0'

        self._graph.edge(tail_name, head_name,
                         label=comm.topic_name,
                         color=color,
                         penwidth=penwidth)

    def _draw_node_to_callback(self, comm, head_name, head_node_name):
        tail_node = Util.find_one(
            self._arch.nodes, lambda x: x.node_name == comm.publisher.node_name
        )
        if tail_node is None or len(tail_node.callbacks) == 0:
            return None

        # Choose only one temporary callback that you need to connect to.
        tail_callback = tail_node.callbacks[0]
        tail_name = tail_callback.name
        tail_node_name = tail_callback.node_name
        if (
            tail_node_name in CallbackGraph.IGNORE_NODES
            or head_node_name in CallbackGraph.IGNORE_NODES
        ):
            return None

        publish_node_cluster_name = self._to_cluster_name(
            comm.publisher.node_name, prefix='node_')
        tooltip = 'The callback to publish is not specified.'
        self._graph.edge(
            tail_name,
            head_name,
            label=comm.topic_name,
            ltail=publish_node_cluster_name,
            color='red',
            dir='both',
            _attributes={'arrowtail': 'dot'},
            tooltip=tooltip,
        )

    def _to_cluster_name(self, node_name: str, prefix=''):
        return 'cluster_' + prefix + node_name

    def _draw_edge(self, comm, head_name, head_node_name, highlight: bool) -> None:
        if comm.callback_from is not None:
            self._draw_callback_to_callback(
                comm, head_name, head_node_name, highlight)
        else:
            self._draw_node_to_callback(comm, head_name, head_node_name)


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

    def draw(self, comm):
        if comm.publisher.node_name not in self._pub_nodes:
            self._pub_nodes.add(comm.publisher.node_name)
            head_name = self._to_head_name(comm)

            self._callback_graph._graph.node(
                head_name, label=self.label, color=self._color)
            self._callback_graph._draw_edge(
                comm, head_name, comm.publisher.node_name, False)

        if comm.subscription.node_name not in self._sub_nodes:
            self._sub_nodes.add(comm.subscription.node_name)
            head_name = comm.callback_to.name
            tail_name = self._to_tail_name(comm)
            self._callback_graph._graph.node(
                tail_name, color=self._color, shape='cds', label=self.label
            )
            self._callback_graph._graph.edge(
                tail_name, head_name, color=self._color, label=self.label
            )
