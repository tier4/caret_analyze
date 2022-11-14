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

from graphviz import Digraph, Source
import numpy as np
import pandas as pd

from ...common import type_check_decorator
from ...exceptions import InvalidArgumentError
from ...runtime.path import Path


@type_check_decorator
def chain_latency(
    path: Path,
    export_path: Optional[str] = None,
    granularity: str = 'node',
    treat_drop_as_delay=False,
    lstrip_s=0,
    rstrip_s=0,
) -> Optional[Source]:
    granularity = granularity or 'node'
    if granularity not in ['node', 'end-to-end']:
        raise InvalidArgumentError('granularity must be [ node / end-to-end ]')

    graph = Digraph()
    graph.engine = 'dot'

    graph.attr('node', shape='box')

    if granularity == 'node':
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


def to_label(latency: np.ndarray) -> str:
    latency = latency[[not pd.isnull(_) for _ in latency]]
    label = (
        'min: {:.2f} ms\n'.format(np.min(latency * 1.0e-6))
        + 'avg: {:.2f} ms\n'.format(np.average(latency * 1.0e-6))
        + 'max: {:.2f} ms'.format(np.max(latency * 1.0e-6))
    )
    return label


def get_attr_node(
    path: Path,
    treat_drop_as_delay: bool,
    lstrip_s: float,
    rstrip_s: float
) -> GraphAttr:
    graph_nodes: List[GraphNode] = []
    remove_dropped = False

    for node_path in path.node_paths:
        node_name = node_path.node_name
        label = node_name

        if node_path.column_names != []:
            _, latency = node_path.to_timeseries(
                remove_dropped=remove_dropped,
                treat_drop_as_delay=treat_drop_as_delay,
                lstrip_s=lstrip_s,
                rstrip_s=rstrip_s,
            )
            label += '\n' + to_label(latency)
        graph_nodes.append(GraphNode(node_name, label))

    graph_edges: List[GraphEdge] = []
    for comm_path in path.communications:
        _, pubsub_latency = comm_path.to_timeseries(
            remove_dropped=remove_dropped,
            treat_drop_as_delay=treat_drop_as_delay,
            lstrip_s=lstrip_s,
            rstrip_s=rstrip_s,
        )
        label = comm_path.topic_name
        label += '\n' + to_label(pubsub_latency)

        graph_edges.append(
            GraphEdge(comm_path.publish_node_name, comm_path.subscribe_node_name, label))

    return GraphAttr(graph_nodes, graph_edges)


def get_attr_end_to_end(
    path: Path,
    treat_drop_as_delay: bool,
    lstrip_s: float,
    rstrip_s: float
) -> GraphAttr:
    node_paths = path.node_paths
    remove_dropped = False

    graph_nodes: List[GraphNode] = []

    for node_path in [node_paths[0], node_paths[-1]]:
        node_name = node_path.node_name
        label = node_name
        if len(node_path.column_names) != 0:
            _, latency = node_path.to_timeseries(
                remove_dropped=remove_dropped,
                treat_drop_as_delay=treat_drop_as_delay,
                lstrip_s=lstrip_s,
                rstrip_s=rstrip_s,
            )
            label += '\n' + to_label(latency)
        graph_nodes.append(GraphNode(node_name, label))

    _, latency = path.to_timeseries(
        remove_dropped=remove_dropped,
        treat_drop_as_delay=treat_drop_as_delay,
        lstrip_s=lstrip_s,
        rstrip_s=rstrip_s,
    )

    start_node_name = node_paths[0].node_name
    end_node_name = node_paths[-1].node_name
    graph_edges: List[GraphEdge] = []
    graph_edges.append(
        GraphEdge(start_node_name, end_node_name, to_label(latency))
    )

    return GraphAttr(graph_nodes, graph_edges)
