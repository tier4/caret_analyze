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

from ...callback import CallbackBase
from ...communication import Communication
from ...communication import VariablePassing
from ...path import Path


def chain_latency(
    path: Path,
    export_path: Optional[str] = None,
    granularity: str = 'node',
    treat_drop_as_delay=True,
    lstrip_s=0,
    rstrip_s=0,
) -> Optional[Source]:
    granularity = granularity or 'callback'
    assert granularity in ['callback', 'node', 'end-to-end']

    graph = Digraph()
    graph.engine = 'dot'

    graph.attr('node', shape='box')

    if granularity == 'callback':
        graph_attr = get_attr_callback(path, treat_drop_as_delay, lstrip_s, rstrip_s)
    elif granularity == 'node':
        graph_attr = get_attr_node(path, treat_drop_as_delay, lstrip_s, rstrip_s)
    elif granularity == 'end-to-end':
        graph_attr = get_attr_end_to_end(path, treat_drop_as_delay, lstrip_s, rstrip_s)

    for node_attr in graph_attr.nodes:
        graph.node(node_attr.node, node_attr.label)
    for edge_attr in graph_attr.edges:
        graph.edge(edge_attr.node_from, edge_attr.node_to, edge_attr.label)

    source = Source(graph.source)
    if export_path is not None:
        file_path_wo_ext = export_path.split('.')[0]
        ext = export_path.split('.')[-1]
        source.render(file_path_wo_ext, format=ext)
        return None

    return graph


class GraphNode:

    def __init__(self, node: str, label: str) -> None:
        self.node = node
        self.label = label


class GraphEdge:

    def __init__(self, node_from: str, node_to: str, label: str) -> None:
        self.node_from = node_from
        self.node_to = node_to
        self.label = label


class GraphAttr:

    def __init__(self, nodes: List[GraphNode], edges: List[GraphEdge]):
        self.nodes = nodes
        self.edges = edges


def to_label(latency: np.array) -> str:
    label = (
        'min: {:.2f} ms\n'.format(np.min(latency * 1.0e-6))
        + 'avg: {:.2f} ms\n'.format(np.average(latency * 1.0e-6))
        + 'max: {:.2f} ms'.format(np.max(latency * 1.0e-6))
    )
    return label


def to_node_paths(path: Path) -> List[Path]:
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
             path.communications, path.variable_passings))

    return paths


def get_attr_callback(path, treat_drop_as_delay, lstrip_s, rstrip_s) -> GraphAttr:
    graph_nodes: List[GraphNode] = []
    graph_edges: List[GraphEdge] = []

    for component in path:
        _, latency = component.to_timeseries(
            remove_dropped=True,
            treat_drop_as_delay=treat_drop_as_delay,
            lstrip_s=lstrip_s,
            rstrip_s=rstrip_s,
        )

        if isinstance(component, CallbackBase):
            label = f'{component.node_name}\n{component.callback_name}\n'
            label += to_label(latency)
            graph_nodes.append(GraphNode(component.unique_name, label))

        elif isinstance(component, Communication):
            label = component.topic_name
            label += '\n' + to_label(latency)

            if component.callback_from is None:
                continue

            graph_edges.append(GraphEdge(
                component.callback_from.unique_name,
                component.callback_to.unique_name,
                label,
            ))

        elif isinstance(component, VariablePassing):
            label = to_label(latency)
            graph_edges.append(
                GraphEdge(
                    component.callback_from.unique_name,
                    component.callback_to.unique_name,
                    label,
                ))

    return GraphAttr(graph_nodes, graph_edges)


def get_attr_node(path, treat_drop_as_delay, lstrip_s, rstrip_s) -> GraphAttr:
    node_paths = to_node_paths(path)
    graph_nodes: List[GraphNode] = []
    for node_path in node_paths:
        _, latency = node_path.to_timeseries(
            remove_dropped=True,
            treat_drop_as_delay=treat_drop_as_delay,
            lstrip_s=lstrip_s,
            rstrip_s=rstrip_s,
        )
        node_name = node_path.callbacks[0].node_name
        label = node_name
        label += '\n' + to_label(latency)
        graph_nodes.append(GraphNode(node_name, label))
    node_names = [path.callbacks[0].node_name for path in node_paths]

    graph_edges: List[GraphEdge] = []
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
                remove_dropped=True,
                treat_drop_as_delay=treat_drop_as_delay,
                lstrip_s=lstrip_s,
                rstrip_s=rstrip_s,
            )
            label = comm_path.topic_name
            label += '\n' + to_label(pubsub_latency)
        else:
            _, pubsub_latency = comm_path.to_timeseries(
                remove_dropped=True,
                lstrip_s=lstrip_s,
                rstrip_s=rstrip_s,
            )
            if comm_path.rmw_implementation == 'rmw_fastrtps_cpp':
                _, dds_latency = comm_path.to_dds_latency().to_timeseries(
                    remove_dropped=True,
                    treat_drop_as_delay=treat_drop_as_delay,
                    lstrip_s=lstrip_s,
                    rstrip_s=rstrip_s,
                )
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
                label += '\n' + to_label(pubsub_latency)

        callback_from = comm_path.callback_from
        if callback_from is None:
            continue

        graph_edges.append(
            GraphEdge(callback_from.node_name, comm_path.callback_to.node_name, label))

    return GraphAttr(graph_nodes, graph_edges)


def get_attr_end_to_end(path, treat_drop_as_delay, lstrip_s, rstrip_s) -> GraphAttr:
    node_paths = to_node_paths(path)

    graph_nodes: List[GraphNode] = []

    for node_path in [node_paths[0], node_paths[-1]]:
        _, latency = node_path.to_timeseries(
            remove_dropped=True,
            treat_drop_as_delay=treat_drop_as_delay,
            lstrip_s=lstrip_s,
            rstrip_s=rstrip_s,
        )
        node_name = node_path.callbacks[0].node_name
        label = node_name + '\n' + to_label(latency)
        graph_nodes.append(GraphNode(node_name, label))

    _, latency = path.to_timeseries(
        remove_dropped=True,
        lstrip_s=lstrip_s,
        rstrip_s=rstrip_s,
    )

    start_node_name = node_paths[0].callbacks[0].node_name
    end_node_name = node_paths[-1].callbacks[0].node_name
    graph_edges: List[GraphEdge] = []
    graph_edges.append(
        GraphEdge(start_node_name, end_node_name, to_label(latency))
    )

    return GraphAttr(graph_nodes, graph_edges)
