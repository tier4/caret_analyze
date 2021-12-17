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

from typing import Dict, Optional, Union

from graphviz import Digraph, Source

from ...runtime import CallbackBase, Node, Publisher, Subscription
from ...value_objects import (CallbackStructValue, CallbackType,
                              NodeStructValue, PublisherStructValue,
                              SubscriptionStructValue)

NodeClass = Union[Node, NodeStructValue]
CallbackClass = Union[CallbackBase, CallbackStructValue]
PublisherClass = Union[Publisher, PublisherStructValue]
SubscriptionClass = Union[Subscription, SubscriptionStructValue]


def callback_graph(
    node: NodeClass,
    export_path: Optional[str] = None,
) -> Optional[Source]:
    import os

    dot = CallbackGraph(node).to_dot()
    source = Source(dot)
    if export_path is not None:
        ext = export_path.split('.')[-1]
        file_path_wo_ext = export_path[:-(len(ext)+1)]
        dirname = os.path.dirname(file_path_wo_ext)
        basename = os.path.basename(file_path_wo_ext)
        source.render(basename, directory=dirname, format=ext, cleanup=True)
        return None
    return source


class CallbackGraph:
    IGNORE_NODES = ['/rviz2']
    PATH_HIGHLIGHT_COLOR = 'darkgreen'
    PATH_HIGHLIGHT_FILL_COLOR = 'darkseagreen1'

    def __init__(
        self,
        node: NodeClass
    ):
        self._graph = Digraph(format='svg', engine='dot')
        self._graph.name = 'Hover the mouse over a callback.'
        self._graph.attr(compound='true', rankdir='LR',
                         style='rounded', newrank='true')

        self._labelled_edges: Dict[str, LabelledEdge] = {}
        pub_topic_name = list(node.publish_topic_names)
        sub_topic_name = list(node.subscribe_topic_names or [])
        topic_names = set(sub_topic_name + pub_topic_name)
        for topic_name in topic_names:
            self._labelled_edges[topic_name] = LabelledEdge(self, topic_name)

        self._draw_graph(node)

    def to_dot(self):
        return self._graph.source

    def _to_ns(self, node_name: str) -> str:
        splitted = node_name.split('/')
        return '/'.join(splitted[:-1])

    def _draw_graph(self, node: NodeClass) -> None:
        self._draw_node(node)

        for pub in node.publishers:
            self._labelled_edges[pub.topic_name].draw_publisher(pub)

        for sub in node.subscriptions:
            self._labelled_edges[sub.topic_name].draw_subscription(sub)

        if node.variable_passings is not None:
            for var in node.variable_passings:
                head_name = var.callback_name_read
                tail_name = var.callback_name_write
                color = 'black'
                penwidth = '1.0'
                self._graph.edge(tail_name, head_name,
                                 color=color, penwidth=penwidth)

    def _get_tooltip(self, callback: CallbackClass) -> str:
        if callback.callback_type == CallbackType.TIMER:
            period_ns_str = '{:,}'.format(callback.period_ns)  # type: ignore
            return f'period ns: {period_ns_str}\n symbol: {callback.symbol}'

        if callback.callback_type == CallbackType.SUBSCRIPTION:
            return f'topic name: {callback.subscribe_topic_name}\n symbol: {callback.symbol}'

        raise NotImplementedError()

    def _draw_callbacks(self, node_cluster, node: NodeClass):
        if node.callbacks is None:
            return

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

    def _draw_node(self, node: NodeClass) -> None:
        # Nesting clusters is not available due to an error in graphviz.
        # Graphviz-2.49.1 used for verification.

        # draw node cluster
        with self._graph.subgraph(
            name=self._to_cluster_name(node.node_name, prefix='node_'),
            graph_attr={'label': node.node_name,
                        'bgcolor': 'white', 'color': 'black'},
        ) as node_cluster:

            self._draw_callbacks(node_cluster, node)

    def _to_cluster_name(self, node_name: str, prefix=''):
        return 'cluster_' + prefix + node_name


class LabelledEdge:

    def __init__(self, graph: CallbackGraph, topic_name, color='blue'):
        self._graph = graph
        self.topic_name = topic_name
        self._color = color

    def _to_tail_name(self, node_name: str):
        return self.topic_name + node_name

    def _to_head_name(self, node_name: str):
        return self.topic_name + node_name

    @property
    def label(self) -> str:
        return f'{self.topic_name}'

    def draw_publisher(self, publisher: PublisherClass):
        head_name = self._to_head_name(publisher.topic_name)

        self._graph._graph.node(
            head_name, color=self._color, shape='cds',
            label=self.label, _attributes={'orientation': '180'}
        )

        if publisher.callback_names is not None:
            for callback_name in publisher.callback_names:
                self._draw_edge_from_callback(
                    callback_name, head_name)
        else:
            self._draw_edge_from_node(publisher, head_name)

    def draw_subscription(self, subscription: SubscriptionClass):
        head_name = subscription.callback_name
        tail_name = self._to_tail_name(subscription.node_name)
        self._graph._graph.node(
            tail_name, color=self._color, shape='cds', label=self.label
        )
        self._graph._graph.edge(
            tail_name, head_name, color=self._color,
        )

    def _draw_edge_from_node(self, publisher: PublisherClass, head_name):
        if publisher.callback_names is None:
            return None

        # Choose only one temporary callback that you need to connect to.
        tail_name = publisher.callback_names[0]

        publish_node_cluster_name = self._graph._to_cluster_name(
            publisher.node_name, prefix='node_')
        tooltip = 'The callback to publish is not specified.'

        self._graph._graph.edge(
            tail_name,
            head_name,
            ltail=publish_node_cluster_name,
            color='red',
            dir='both',
            _attributes={'arrowtail': 'dot'},
            tooltip=tooltip,
        )

    def _draw_edge_from_callback(self, callback_name: str, head_name):
        tail_name = callback_name

        color = 'black'
        penwidth = '1.0'

        self._graph._graph.edge(tail_name,
                                head_name,
                                color=color,
                                penwidth=penwidth)
