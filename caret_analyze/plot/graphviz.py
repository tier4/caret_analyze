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

from typing import List, Optional

from graphviz import Digraph
from graphviz import Source
import numpy as np

from ..architecture import Architecture
from ..callback import CallbackBase
from ..callback import SubscriptionCallback
from ..callback import TimerCallback
from ..communication import Communication
from ..communication import VariablePassing
from ..node import Node
from ..path import Path
from ..util import Util


def callback_graph(
    arch: Architecture, callbacks: List[CallbackBase], export_path: Optional[str] = None
) -> Optional[Source]:
    dot = CallbackGraph(arch, callbacks).to_dot()
    source = Source(dot)
    if export_path is not None:
        file_path_wo_ext = export_path.split('.')[0]
        ext = export_path.split('.')[-1]
        source.render(file_path_wo_ext, format=ext)
        return None
    return source


def chain_latency(
    path: Path, export_path: Optional[str] = None,
    granularity: str = 'node', treat_drop_as_delay=True
) -> Optional[Source]:
    granularity = granularity or 'callback'
    assert granularity in ['callback', 'node', 'end-to-end']

    graph = Digraph()
    graph.engine = 'dot'

    graph.attr('node', shape='box')

    def to_node_paths(path) -> List[Path]:
        callbacks: List[CallbackBase] = []
        paths: List[Path] = []
        for cb, cb_ in zip(path.callbacks[:-1], path.callbacks[1:]):
            callbacks.append(cb)
            if cb.node_name != cb_.node_name:
                paths.append(
                    Path(callbacks, path.communications, path.variable_passings))
                callbacks.clear()
        paths.append(
            Path(callbacks + [path.callbacks[-1]],
                 path.communications, path.variable_passings)
        )
        return paths

    def to_label(latency):
        label = (
            'min: {:.2f} ms\n'.format(np.min(latency * 1.0e-6))
            + 'avg: {:.2f} ms\n'.format(np.average(latency * 1.0e-6))
            + 'max: {:.2f} ms'.format(np.max(latency * 1.0e-6))
        )
        return label

    if granularity == 'callback':
        for component in path:
            _, latency = component.to_timeseries(
                remove_dropped=True, treat_drop_as_delay=treat_drop_as_delay)
            if isinstance(component, CallbackBase):
                label = f'{component.node_name}\n{component.callback_name}\n'
                label += to_label(latency)
                graph.node(component.unique_name, label=label)
            elif isinstance(component, Communication):
                label = component.topic_name
                label += '\n' + to_label(latency)
                callback_from = component.callback_from
                if callback_from is None:
                    continue
                graph.edge(
                    callback_from.unique_name,
                    component.callback_to.unique_name,
                    label=label,
                )
            elif isinstance(component, VariablePassing):
                # label = to_label(latency)
                graph.edge(
                    component.callback_from.unique_name,
                    component.callback_to.unique_name,
                    label=label,
                )
    elif granularity == 'node':
        node_paths = to_node_paths(path)
        for node_path in node_paths:
            _, latency = node_path.to_timeseries(
                remove_dropped=True, treat_drop_as_delay=treat_drop_as_delay)
            node_name = node_path.callbacks[0].node_name
            label = node_name
            label += '\n' + to_label(latency)
            graph.node(node_name, label=label)
        node_names = [path.callbacks[0].node_name for path in node_paths]

        for comm_path in path.communications:
            callback_from = comm_path.callback_from
            if callback_from is None:
                continue
            if (
                callback_from.node_name not in node_names
                or comm_path.callback_to.node_name not in node_names
            ):
                continue
            if comm_path.is_intra_process:
                _, pubsub_latency = comm_path.to_timeseries(
                    remove_dropped=True, treat_drop_as_delay=treat_drop_as_delay)
                label = comm_path.topic_name
                label += '\n' + (
                    'min: {:.2f} ms\n'.format(np.min(pubsub_latency * 1.0e-6))
                    + 'avg: {:.2f} ms\n'.format(np.average(pubsub_latency * 1.0e-6))
                    + 'max: {:.2f} ms'.format(np.max(pubsub_latency * 1.0e-6))
                )
            else:
                _, pubsub_latency = comm_path.to_timeseries(
                    remove_dropped=True
                )
                if comm_path.rmw_implementation == 'rmw_fastrtps_cpp':
                    _, dds_latency = comm_path.to_dds_latency().to_timeseries(
                        remove_dropped=True, treat_drop_as_delay=treat_drop_as_delay)
                    label = comm_path.topic_name
                    label += '\n' + (
                        'min: {:.2f} ({:.2f}) ms\n'.format(
                            np.min(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                        )
                        + 'avg: {:.2f} ({:.2f}) ms\n'.format(
                            np.average(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                        )
                        + 'max: {:.2f} ({:.2f}) ms'.format(
                            np.max(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                        )
                    )
                else:
                    label = comm_path.topic_name
                    label += '\n' + (
                        'min: {:.2f}  ms\n'.format(np.min(pubsub_latency * 1.0e-6))
                        + 'avg: {:.2f} ms\n'.format(np.average(pubsub_latency * 1.0e-6))
                        + 'max: {:.2f} ms'.format(np.max(pubsub_latency * 1.0e-6))
                    )

            callback_from = comm_path.callback_from
            if callback_from is None:
                continue
            graph.edge(
                callback_from.node_name,
                comm_path.callback_to.node_name,
                label=label,
            )
    elif granularity == 'end-to-end':
        node_paths = to_node_paths(path)

        for node_path in [node_paths[0], node_paths[-1]]:
            _, latency = node_path.to_timeseries(
                remove_dropped=True, treat_drop_as_delay=treat_drop_as_delay)
            node_name = node_path.callbacks[0].node_name
            label = node_name + '\n' + to_label(latency)
            graph.node(node_name, label=label)

        inter_mediate_callbacks = []
        terminal_callbacks = node_paths[0].callbacks + node_paths[-1].callbacks
        for callback in path.callbacks:
            if callback not in terminal_callbacks:
                inter_mediate_callbacks.append(callback)

        path = Path(inter_mediate_callbacks,
                    path.communications, path.variable_passings)
        _, latency = path.to_timeseries(remove_dropped=True)

        start_node_name = node_paths[0].callbacks[0].node_name
        end_node_name = node_paths[-1].callbacks[0].node_name
        graph.edge(start_node_name, end_node_name, label=to_label(latency))

    source = Source(graph.source)
    if export_path is not None:
        file_path_wo_ext = export_path.split('.')[0]
        ext = export_path.split('.')[-1]
        source.render(file_path_wo_ext, format=ext)
        return None

    return graph


class CallbackGraph:
    IGNORE_NODES = ['/rviz2']
    PATH_HIGHLIGHT_COLOR = 'darkgreen'
    PATH_HIGHLIGHT_FILL_COLOR = 'darkseagreen1'

    def __init__(self, arch: Architecture, callbacks: List[CallbackBase]):
        self._arch = arch
        path = Path(callbacks, arch.communications, arch.variable_passings)

        self._graph = Digraph(format='svg', engine='dot')
        self._graph.name = 'Hover the mouse over a callback.'
        self._graph.attr(compound='true', rankdir='LR', style='rounded')

        self._labelled_edges = []
        self._labelled_edges.append(self.LabelledEdge(self, '/tf'))
        self._labelled_edges.append(self.LabelledEdge(self, '/tf_static'))

        self._draw_graph(path)

    def to_dot(self):
        return self._graph.source

    def _to_ns(self, node_name: str) -> str:
        splitted = node_name.split('/')
        return '/'.join(splitted[:-1])

    def _draw_graph(self, path: Path) -> None:
        for node in self._arch.nodes:
            if node.node_name in CallbackGraph.IGNORE_NODES:
                continue
            self._draw_node(node, path)

        for comm in self._arch.communications:
            self._draw_comm(comm, path.contains(comm))  # highlight

        for var in self._arch.variable_passings:
            head_name = var.callback_to.unique_name
            tail_name = var.callback_from.unique_name
            if path.contains(var):
                color = CallbackGraph.PATH_HIGHLIGHT_COLOR
            else:
                color = 'black'
            self._graph.edge(tail_name, head_name, color=color)

    def _draw_comm(self, comm, highlight: bool) -> None:
        for labelled_edge in self._labelled_edges:
            if comm.topic_name == labelled_edge.topic_name:
                labelled_edge.draw(comm)
                return
        head_name = comm.callback_to.unique_name
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
                callback.unique_name,
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
        tail_name = comm.callback_from.unique_name
        tail_node_name = comm.callback_from.node_name
        if (
            tail_node_name in CallbackGraph.IGNORE_NODES
            or head_node_name in CallbackGraph.IGNORE_NODES
        ):
            return

        if highlight:
            color = CallbackGraph.PATH_HIGHLIGHT_COLOR
        else:
            color = 'black'

        self._graph.edge(tail_name, head_name,
                         label=comm.topic_name, color=color)

    def _draw_node_to_callback(self, comm, head_name, head_node_name):
        tail_node = Util.find_one(
            self._arch.nodes, lambda x: x.node_name == comm.publisher.node_name
        )
        if tail_node is None or len(tail_node.callbacks) == 0:
            return None

        # Choose only one temporary callback that you need to connect to.
        tail_callback = tail_node.callbacks[0]
        tail_name = tail_callback.unique_name
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
                head_name = comm.callback_to.unique_name
                tail_name = self._to_tail_name(comm)
                self._callback_graph._graph.node(
                    tail_name, color=self._color, shape='cds', label=self.label
                )
                self._callback_graph._graph.edge(
                    tail_name, head_name, color=self._color, label=self.label
                )
