# Copyright 2021 TIER IV, Inc.
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

from graphviz import Digraph, Source
import numpy as np
import pandas as pd

from ...common import type_check_decorator
from ...exceptions import InvalidArgumentError
from ...runtime.path import Path


@type_check_decorator
def chain_latency(
    path: Path,
    export_path: str | None = None,
    granularity: str = 'node',
    treat_drop_as_delay=False,
    lstrip_s=0,
    rstrip_s=0,
) -> Digraph | None:
    """
    Chain latency.

    Parameters
    ----------
    path : Path
        Path.
    export_path : str | None
        Export path.
    granularity : str
        Granularity.
    treat_drop_as_delay : bool
        Treat drop as delay.
    lstrip_s : float, optional
        Start time of cropping range, by default 0.
    rstrip_s: float, optional
        End point of cropping range, by default 0.

    Returns
    -------
    Digraph | None
        Created directed graph.

    Raises
    ------
    UnsupportedTypeError
        Argument granularity is not "node", "end-to-end".

    """
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

    def __init__(self, nodes: list[GraphNode], edges: list[GraphEdge]):
        self.nodes = nodes
        self.edges = edges


def to_label(latency: np.ndarray) -> str:
    """
    To label.

    Parameters
    ----------
    latency : np.ndarray
        Latency.

    Returns
    -------
    str
        Label.

    """
    latency = latency[[not pd.isnull(_) for _ in latency]]
    if len(latency) > 0:
        label = (
            'min: {:.2f} ms\n'.format(float(np.min(latency * 1.0e-6)))
            + 'avg: {:.2f} ms\n'.format(float(np.average(latency * 1.0e-6)))
            + 'max: {:.2f} ms'.format(float(np.max(latency * 1.0e-6)))
        )
    else:
        label = 'Latency cannot be calculated'
    return label


def get_attr_node(
    path: Path,
    treat_drop_as_delay: bool,
    lstrip_s: float,
    rstrip_s: float
) -> GraphAttr:
    """
    Get attribute node.

    Parameters
    ----------
    path : Path
        Path.
    treat_drop_as_delay : bool
        Treat drop as delay.
    lstrip_s : float, optional
        Start time of cropping range, by default 0.
    rstrip_s: float, optional
        End point of cropping range, by default 0.

    Returns
    -------
    GraphAttr
        Graph attribute node.

    """
    def calc_latency_from_path_df(target_columns: list[str]) -> np.ndarray:
        target_df = path.to_dataframe(
            remove_dropped=remove_dropped,
            treat_drop_as_delay=treat_drop_as_delay,
            lstrip_s=lstrip_s,
            rstrip_s=rstrip_s,
        )[target_columns]
        source_stamps_ns = np.array(target_df.iloc[:, 0].values)
        dest_stamps_ns = np.array(target_df.iloc[:, -1].values)
        latency_ns = dest_stamps_ns - source_stamps_ns
        if remove_dropped:
            latency_ns = latency_ns.astype('int64')
        return latency_ns

    graph_nodes: list[GraphNode] = []

    remove_dropped = False

    for i, node_path in enumerate(path.node_paths):
        node_name = node_path.node_name
        label = node_name

        if i == 0 and path.include_first_callback:
            first_cb_columns = path.column_names[0:2]
            latency = calc_latency_from_path_df(first_cb_columns)
            label += '\n' + to_label(latency)

        elif i == len(path.node_paths)-1 and path.include_last_callback:
            last_cb_columns = path.column_names[-2:]
            latency = calc_latency_from_path_df(last_cb_columns)
            label += '\n' + to_label(latency)

        elif node_path.column_names != []:
            _, latency = node_path.to_timeseries(
                remove_dropped=remove_dropped,
                treat_drop_as_delay=treat_drop_as_delay,
                lstrip_s=lstrip_s,
                rstrip_s=rstrip_s,
            )
            label += '\n' + to_label(latency)

        graph_nodes.append(GraphNode(node_name, label))

    graph_edges: list[GraphEdge] = []
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
    """
    Get attribute end to end.

    Parameters
    ----------
    path : Path
        Path.
    treat_drop_as_delay : bool
        Treat drop as delay.
    lstrip_s : float, optional
        Start time of cropping range, by default 0.
    rstrip_s: float, optional
        End point of cropping range, by default 0.

    Returns
    -------
    GraphAttr
        End to end graph attribute node.

    """
    node_paths = path.node_paths
    remove_dropped = False

    graph_nodes: list[GraphNode] = []

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
    graph_edges: list[GraphEdge] = []
    graph_edges.append(
        GraphEdge(start_node_name, end_node_name, to_label(latency))
    )

    return GraphAttr(graph_nodes, graph_edges)
