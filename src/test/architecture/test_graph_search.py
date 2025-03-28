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

from caret_analyze.architecture.architecture import \
    DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
from caret_analyze.architecture.graph_search import (CallbackPathSearcher,
                                                     Graph, GraphCore,
                                                     GraphEdge, GraphEdgeCore,
                                                     GraphNode, GraphPath,
                                                     GraphPathCore,
                                                     NodePathSearcher)
from caret_analyze.architecture.struct import (CallbackStruct,
                                               CommunicationStruct,
                                               NodePathStruct, NodeStruct,
                                               PathStruct,
                                               PublisherStruct, SubscriptionStruct,
                                               VariablePassingStruct)
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.value_objects import (CommunicationStructValue,
                                         NodePathStructValue)


import pytest


class TestGraphNode:

    def test_eq(self):
        assert GraphNode('/node0/callback0') == GraphNode('/node0/callback0')
        assert GraphNode('/node0/callback0') != GraphNode('/node1/callback1')


class TestGraphPath:

    def test_to_graph_nodes_multi_nodes(self):
        path = GraphPath()
        assert len(path) == 0
        assert len(path.nodes) == 0

        nodes = [
            GraphNode('/node0/callback0'),
            GraphNode('/node1/callback1'),
            GraphNode('/node2/callback2'),
        ]
        path.append(GraphEdge(nodes[0], nodes[1]))
        path.append(GraphEdge(nodes[1], nodes[2]))

        assert len(path) == 2
        assert nodes == path.nodes

    def test_to_graph_nodes_single_node(self):
        path = GraphPath()

        nodes = [GraphNode('/node0/callback0')]
        path.append(GraphEdge(nodes[0], nodes[0]))

        assert len(path) == 1
        assert nodes == path.nodes


class TestGraphCore:

    def test_search_simple_case(self):
        g = GraphCore()
        g.add_edge(0, 1)

        r = g.search_paths(0, 1)
        assert r == [[GraphEdgeCore(0, 1)]]

    def test_search_merge_case(self):
        g = GraphCore()

        g.add_edge(0, 1)
        g.add_edge(0, 2)
        g.add_edge(1, 3)
        g.add_edge(2, 3)
        g.add_edge(3, 4)

        r = g.search_paths(0, 4)
        assert [GraphEdgeCore(0, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 4)] in r
        assert [GraphEdgeCore(0, 2), GraphEdgeCore(2, 3), GraphEdgeCore(3, 4)] in r

    def test_search_loop_case(self):
        g = GraphCore()
        g.add_edge(0, 1)
        g.add_edge(1, 0)

        r = g.search_paths(0, 1)
        assert r == [
            GraphPathCore([GraphEdgeCore(0, 1)])
        ]

        r = g.search_paths(0, 0)
        r == [
            GraphPathCore([GraphEdgeCore(0, 1), GraphEdgeCore(1, 0)])
        ]

    def test_search_complex_loop_case(self):
        g = GraphCore()

        g.add_edge(0, 1)
        g.add_edge(0, 2)
        g.add_edge(1, 3)
        g.add_edge(3, 4)
        g.add_edge(3, 2)
        g.add_edge(2, 1)
        g.add_edge(2, 4)

        r = g.search_paths(0, 4)
        assert len(r) == 5
        assert [GraphEdgeCore(0, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 4)] in r
        assert [
            GraphEdgeCore(0, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 2), GraphEdgeCore(2, 4)
            ] in r
        assert [
            GraphEdgeCore(0, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 4)
            ] in r
        assert [
            GraphEdgeCore(0, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3),
            GraphEdgeCore(3, 2), GraphEdgeCore(2, 4)
            ] in r
        assert [GraphEdgeCore(0, 2), GraphEdgeCore(2, 4)] in r

    def test_search_complex_loop_case_with_label(self):
        g = GraphCore()

        g.add_edge(0, 1, '0')
        g.add_edge(0, 2, '1')
        g.add_edge(0, 2, '1_')
        g.add_edge(1, 3, '2')
        g.add_edge(3, 4, '3')
        g.add_edge(3, 2, '4')
        g.add_edge(2, 1, '5')
        g.add_edge(2, 4, '6')

        r = g.search_paths(0, 4)
        assert len(r) == 8
        assert [
            GraphEdgeCore(0, 1, '0'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')
            ] in r
        assert [
            GraphEdgeCore(0, 1, '0'), GraphEdgeCore(1, 3, '2'),
            GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')
            ] in r
        assert [
            GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 1, '5'),
            GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')
            ] in r
        assert [
            GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 1, '5'),
            GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')
            ] in r
        assert [
            GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 4, '6')] in r
        assert [
            GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 1, '5'),
            GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')
            ] in r
        assert [
            GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 1, '5'),
            GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')
            ] in r
        assert [
            GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 4, '6')] in r

    def test_search_simple_loop_case(self):
        g = GraphCore()

        g.add_edge(0, 1)
        g.add_edge(1, 3)
        g.add_edge(1, 2)
        g.add_edge(2, 1)

        r = g.search_paths(0, 3)
        assert len(r) == 2
        assert [
            GraphEdgeCore(0, 1), GraphEdgeCore(1, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3)
            ] in r
        assert [GraphEdgeCore(0, 1), GraphEdgeCore(1, 3)] in r

    # def test_measure_performance(self):
    #     num = 5000
    #     g = GraphCore()
    #     for i in range(num-1):
    #         g.add_edge(i, i+1)

    #     g.search_paths(0, num-1)

    def test_search_returning_to_start_case(self):
        g = GraphCore()
        g.add_edge(0, 1)
        g.add_edge(1, 2)
        g.add_edge(2, 3)
        g.add_edge(3, 0)
        r = g.search_paths(0, 0)
        assert len(r) == 1
        assert [
            GraphEdgeCore(0, 1), GraphEdgeCore(1, 2), GraphEdgeCore(2, 3), GraphEdgeCore(3, 0)
            ] in r


class TestGraph:

    def test_search_simple_case(self):
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        g = Graph()
        g.add_edge(node_0, node_1)

        r = g.search_paths(node_0, node_1)
        expect = GraphPath([GraphEdge(node_0, node_1)])
        assert r == [expect]

    def test_search_merge_case(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')
        node_4 = GraphNode('4')

        g.add_edge(node_0, node_1)
        g.add_edge(node_0, node_2)
        g.add_edge(node_1, node_3)
        g.add_edge(node_2, node_3)
        g.add_edge(node_3, node_4)

        r = g.search_paths(node_0, node_4)
        assert GraphPath([
            GraphEdge(node_0, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)]
            ) in r
        assert GraphPath([
            GraphEdge(node_0, node_2), GraphEdge(node_2, node_3), GraphEdge(node_3, node_4)
            ]) in r

    def test_search_loop_case(self):
        g = GraphCore()
        g.add_edge(0, 1)
        g.add_edge(1, 0)

        r = g.search_paths(0, 1)
        assert r == [
            [GraphEdgeCore(0, 1)]
        ]

        r = g.search_paths(0, 0)
        r == [
            [GraphEdgeCore(0, 1), GraphEdgeCore(1, 0)]
        ]

    def test_search_complex_loop_case(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')
        node_4 = GraphNode('4')

        g.add_edge(node_0, node_1)
        g.add_edge(node_0, node_2)
        g.add_edge(node_1, node_3)
        g.add_edge(node_3, node_4)
        g.add_edge(node_3, node_2)
        g.add_edge(node_2, node_1)
        g.add_edge(node_2, node_4)

        r = g.search_paths(node_0, node_4)
        assert len(r) == 5
        assert [
            GraphEdge(node_0, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)
            ] in r
        assert [
            GraphEdge(node_0, node_1), GraphEdge(node_1, node_3),
            GraphEdge(node_3, node_2), GraphEdge(node_2, node_4)
            ] in r
        assert [
            GraphEdge(node_0, node_2), GraphEdge(node_2, node_1),
            GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)
            ] in r
        assert [
            GraphEdge(node_0, node_2), GraphEdge(node_2, node_1),
            GraphEdge(node_1, node_3), GraphEdge(node_3, node_2), GraphEdge(node_2, node_4)
            ] in r
        assert [
            GraphEdge(node_0, node_2), GraphEdge(node_2, node_4)] in r

    def test_search_complex_loop_case_with_label(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')
        node_4 = GraphNode('4')

        g.add_edge(node_0, node_1, '0')
        g.add_edge(node_0, node_2, '1')
        g.add_edge(node_0, node_2, '1_')
        g.add_edge(node_1, node_3, '2')
        g.add_edge(node_3, node_4, '3')
        g.add_edge(node_3, node_2, '4')
        g.add_edge(node_2, node_1, '5')
        g.add_edge(node_2, node_4, '6')

        r = g.search_paths(node_0, node_4)
        assert len(r) == 8
        assert [
            GraphEdge(node_0, node_1, '0'), GraphEdge(node_1, node_3, '2'),
            GraphEdge(node_3, node_4, '3')
            ] in r
        assert [
            GraphEdge(node_0, node_1, '0'), GraphEdge(node_1, node_3, '2'),
            GraphEdge(node_3, node_2, '4'), GraphEdge(node_2, node_4, '6')
            ] in r
        assert [
            GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_1, '5'),
            GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_4, '3')
            ] in r
        assert [
            GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_1, '5'),
            GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_2, '4'),
            GraphEdge(node_2, node_4, '6')
            ] in r
        assert [
            GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_4, '6')] in r
        assert [
            GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_1, '5'),
            GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_4, '3')
            ] in r
        assert [
            GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_1, '5'),
            GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_2, '4'),
            GraphEdge(node_2, node_4, '6')
            ] in r
        assert [
            GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_4, '6')] in r

    def test_search_simple_loop_case(self):
        g = Graph()

        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')

        g.add_edge(node_0, node_1)
        g.add_edge(node_1, node_3)
        g.add_edge(node_1, node_2)
        g.add_edge(node_2, node_1)

        r = g.search_paths(node_0, node_3)
        assert len(r) == 2
        assert [
            GraphEdge(node_0, node_1), GraphEdge(node_1, node_2),
            GraphEdge(node_2, node_1), GraphEdge(node_1, node_3)] in r
        assert [
            GraphEdge(node_0, node_1), GraphEdge(node_1, node_3)] in r


class TestCallbackPathSearcher:

    def test_empty(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'callbacks', ())
        mocker.patch.object(node_mock, 'variable_passings', ())
        searcher = CallbackPathSearcher(
            node_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING,
        )

        sub_cb_mock = mocker.Mock(spec=CallbackStruct)
        pub_cb_mock = mocker.Mock(spec=CallbackStruct)

        with pytest.raises(ItemNotFoundError):
            searcher.search(sub_cb_mock, pub_cb_mock, node_mock)

    def test_search(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)

        sub_cb_mock = mocker.Mock(spec=CallbackStruct)
        pub_cb_mock = mocker.Mock(spec=CallbackStruct)

        mocker.patch.object(node_mock, 'callbacks', [])
        mocker.patch.object(node_mock, 'variable_passings', [])

        searcher_mock = mocker.Mock(spec=Graph)
        mocker.patch(
            'caret_analyze.architecture.graph_search.Graph',
            return_value=searcher_mock
        )
        mocker.patch.object(searcher_mock, 'search_paths',
                            return_value=[GraphPath()])
        searcher = CallbackPathSearcher(
            node_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING,
        )

        path_mock = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(searcher, '_to_paths', return_value=[path_mock])

        sub_cb_mock = mocker.Mock(spec=CallbackStruct)
        pub_cb_mock = mocker.Mock(spec=CallbackStruct)

        paths = searcher.search(sub_cb_mock, pub_cb_mock, node_mock)
        assert paths == (path_mock,)

    def test_to_path(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)

        sub_cb_mock = mocker.Mock(spec=CallbackStruct)
        sub_cb_mock.construction_order = 5
        pub_cb_mock = mocker.Mock(spec=CallbackStruct)
        pub_cb_mock.construction_order = 15
        var_pas_mock = mocker.Mock(spec=VariablePassingStruct)

        sub_topic_name = '/sub'
        pub_topic_name = '/pub'

        mocker.patch.object(node_mock, 'node_name', '/node')
        mocker.patch.object(node_mock, 'callbacks', [pub_cb_mock, sub_cb_mock])
        mocker.patch.object(node_mock, 'variable_passings', [var_pas_mock])

        searcher = CallbackPathSearcher(
            node_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING,
        )

        sub_info_mock = mocker.Mock(spec=SubscriptionStruct)
        pub_info_mock = mocker.Mock(spec=PublisherStruct)

        mocker.patch.object(sub_info_mock, 'topic_name', sub_topic_name)
        mocker.patch.object(pub_info_mock, 'topic_name', pub_topic_name)

        searcher_sub_info_mock = mocker.Mock(spec=SubscriptionStruct)
        searcher_pub_info_mock = mocker.Mock(spec=PublisherStruct)

        mocker.patch.object(searcher_sub_info_mock, 'topic_name', sub_topic_name)
        mocker.patch.object(searcher_pub_info_mock, 'topic_name', pub_topic_name)

        mocker.patch.object(sub_cb_mock, 'callback_name', 'cb0')
        mocker.patch.object(pub_cb_mock, 'callback_name', 'cb1')
        mocker.patch.object(var_pas_mock, 'callback_name_read', 'cb1')
        mocker.patch.object(var_pas_mock, 'callback_name_write', 'cb0')

        graph_node_mock_0 = GraphNode(
            CallbackPathSearcher._to_node_point_name(sub_cb_mock.callback_name, 'read')
        )
        graph_node_mock_1 = GraphNode(
            CallbackPathSearcher._to_node_point_name(sub_cb_mock.callback_name, 'write')
        )
        graph_node_mock_2 = GraphNode(
            CallbackPathSearcher._to_node_point_name(pub_cb_mock.callback_name, 'read')
        )
        graph_node_mock_3 = GraphNode(
            CallbackPathSearcher._to_node_point_name(pub_cb_mock.callback_name, 'write')
        )

        chain = [sub_cb_mock, var_pas_mock, pub_cb_mock]
        graph_path_mock = mocker.Mock(spec=GraphPath)
        mocker.patch.object(graph_path_mock, 'nodes',
                            [
                                graph_node_mock_0,
                                graph_node_mock_1,
                                graph_node_mock_2,
                                graph_node_mock_3,
                            ])

        node_path = searcher._to_path(
            graph_path_mock, searcher_sub_info_mock, searcher_pub_info_mock)
        expected = NodePathStruct(
            '/node', sub_info_mock, pub_info_mock, tuple(chain), None)
        assert node_path.to_value() == expected.to_value()

        node_path = searcher._to_path(graph_path_mock, None, searcher_pub_info_mock)
        expected = NodePathStruct(
            '/node', None, pub_info_mock, tuple(chain), None)
        assert node_path.to_value() == expected.to_value()

        node_path = searcher._to_path(graph_path_mock, searcher_sub_info_mock, None)
        expected = NodePathStruct(
            '/node', sub_info_mock, None, tuple(chain), None)
        assert node_path.to_value() == expected.to_value()

    def test_to_paths(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)

        graph_path_mock = mocker.Mock(spec=GraphPath)

        sub_info_mock = mocker.Mock(spec=SubscriptionStruct)
        pub_info_mock1 = mocker.Mock(spec=PublisherStruct)
        pub_info_mock2 = mocker.Mock(spec=PublisherStruct)

        node_path_mock1 = mocker.Mock(spec=NodePathStruct)
        node_path_mock2 = mocker.Mock(spec=NodePathStruct)
        node_path_mock3 = mocker.Mock(spec=NodePathStruct)

        mocker.patch.object(node_mock, 'callbacks', [])
        mocker.patch.object(node_mock, 'variable_passings', [])

        def dummy_to_path(callbacks_graph_path, subscription, publisher):
            assert callbacks_graph_path == graph_path_mock
            assert subscription == sub_info_mock
            if publisher is None:
                return node_path_mock1
            elif publisher == pub_info_mock1:
                return node_path_mock2
            elif publisher == pub_info_mock2:
                return node_path_mock3
            else:
                assert False

        searcher = CallbackPathSearcher(
            node_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING,
        )
        mocker.patch.object(searcher, '_to_path', side_effect=dummy_to_path)

        node_paths = searcher._to_paths(
            graph_path_mock,
            None, None)
        assert len(node_paths) == 0

        node_paths = searcher._to_paths(
            graph_path_mock,
            None, [])
        assert len(node_paths) == 0

        node_paths = searcher._to_paths(
            graph_path_mock,
            sub_info_mock, None)
        assert len(node_paths) == 1
        assert node_paths[0] == node_path_mock1

        node_paths = searcher._to_paths(
            graph_path_mock,
            sub_info_mock, [])
        assert len(node_paths) == 1
        assert node_paths[0] == node_path_mock1

        node_paths = searcher._to_paths(
            graph_path_mock,
            sub_info_mock, [pub_info_mock1, pub_info_mock2])
        assert len(node_paths) == 2
        assert node_paths[0] == node_path_mock2
        assert node_paths[1] == node_path_mock3

    def test_max_callback_construction_order(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)

        sub_cb_mock = mocker.Mock(spec=CallbackStruct)
        sub_cb_mock.construction_order = \
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING - 1
        sub_cb_mock.callback_name = 'sub_callback'

        pub_cb_mock = mocker.Mock(spec=CallbackStruct)
        pub_cb_mock.construction_order = \
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING + 1
        pub_cb_mock.callback_name = 'pub_callback'

        var_pass_mock = mocker.Mock(spec=VariablePassingStruct)
        var_pass_mock.callback_name_read = 'sub_callback'
        var_pass_mock.callback_name_write = 'pub_callback'

        node_mock.callbacks = [sub_cb_mock, pub_cb_mock]
        node_mock.variable_passings = [var_pass_mock]

        searcher = CallbackPathSearcher(
            node_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING,
        )

        graph = searcher._graph

        expected_nodes = {
            'sub_callback@read',
            'pub_callback@write'
        }

        registered_node_names = {node.node_name for node in graph._nodes}

        assert expected_nodes.issubset(registered_node_names), \
            'Expected nodes are not registered in the graph.'


class TestNodePathSearcher:

    def test_empty(self, mocker):
        searcher = NodePathSearcher(
            (),
            (),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        with pytest.raises(ItemNotFoundError):
            searcher.search('node_name_not_exist', 'node_name_not_exist', max_node_depth=0)

        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'paths', [])
        mocker.patch.object(node_mock, 'node_name', 'node')
        mocker.patch.object(node_mock, 'publish_topic_names', [])
        mocker.patch.object(node_mock, 'subscribe_topic_names', [])
        searcher = NodePathSearcher(
            (node_mock,),
            (),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        paths = searcher.search('node', 'node')
        assert paths == []

    def test_search(self, mocker):
        graph_mock = mocker.Mock(spec=Graph)
        mocker.patch(
            'caret_analyze.architecture.graph_search.Graph',
            return_value=graph_mock)
        searcher = NodePathSearcher(
            (),
            (),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        src_node = GraphNode('start_node_name')
        dst_node = GraphNode('end_node_name')

        graph_path_mock = mocker.Mock(spec=GraphPathCore)
        mocker.patch.object(graph_mock, 'search_paths',
                            return_value=[graph_path_mock])

        path_mock = mocker.Mock(spec=PathStruct)
        mocker.patch.object(searcher, '_to_path', return_value=path_mock)
        paths = searcher.search('start_node_name', 'end_node_name')

        assert paths == [path_mock]
        assert graph_mock.search_paths.call_args == (
            (src_node, dst_node), {'max_depth': 0})

    def test_to_path(self, mocker):
        node_name = '/node'
        topic_name = '/topic'
        sub_const_name = '@0'
        pub_const_name = '@0'

        node_mock = mocker.Mock(spec=NodeStruct)
        comm_mock = mocker.Mock(spec=CommunicationStruct)
        node_path_mock = mocker.Mock(spec=NodePathStruct)

        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'node_name', node_name)
        mocker.patch.object(node_path_mock, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock, 'subscription_construction_order', 0)
        node_path_value_mock = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock, 'to_value', return_value=node_path_value_mock)

        mocker.patch.object(comm_mock, 'publish_node_name', node_name)
        mocker.patch.object(comm_mock, 'subscribe_node_name', node_name)
        mocker.patch.object(comm_mock, 'topic_name', topic_name)
        mocker.patch.object(comm_mock, 'publisher_construction_order', 0)
        mocker.patch.object(comm_mock, 'subscription_construction_order', 0)
        comm_value_mock = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock, 'to_value', return_value=comm_value_mock)
        callback = mocker.Mock()
        mocker.patch.object(callback, 'construction_order', 1)
        mocker.patch.object(comm_mock, 'subscribe_callback', callback)

        mocker.patch.object(node_mock, 'paths', [node_path_mock])

        mocker.patch.object(
            NodePathSearcher, '_create_head_dummy_node_path', return_value=node_path_mock)

        mocker.patch.object(
            NodePathSearcher, '_create_tail_dummy_node_path', return_value=node_path_mock)

        searcher = NodePathSearcher(
            (node_mock,),
            (comm_mock,),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        graph_path_mock = mocker.Mock(spec=GraphPath)
        edge_mock = mocker.Mock(GraphEdge)
        mocker.patch.object(
            graph_path_mock, 'nodes',
            [GraphNode(node_name)],
        )
        mocker.patch.object(edge_mock, 'node_name_from', node_name)
        mocker.patch.object(edge_mock, 'node_name_to', node_name)
        mocker.patch.object(edge_mock, 'label', topic_name+sub_const_name+pub_const_name)
        mocker.patch.object(
            graph_path_mock, 'edges', [edge_mock]
        )
        pub_mock = mocker.Mock(spec=PublisherStruct)
        mocker.patch.object(NodePathSearcher, '_get_publisher', return_value=pub_mock)
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        mocker.patch.object(NodePathSearcher, '_get_subscription', return_value=sub_mock)
        path = searcher._to_path(graph_path_mock)

        expected = PathStruct(
            None, (node_path_mock, comm_mock, node_path_mock)
        )
        assert path.to_value() == expected.to_value()

    def test_single_node_cyclic_case(self, mocker):
        node_name = '/node'
        topic_name = '/chatter'

        node_mock = mocker.Mock(spec=NodeStruct)

        node_path_mock = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_mock, 'paths', (node_path_mock,))
        mocker.patch.object(node_mock, 'node_name', node_name)
        mocker.patch.object(node_mock, 'publish_topic_names', [topic_name])
        mocker.patch.object(node_mock, 'subscribe_topic_names', [topic_name])

        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'node_name', node_name)
        mocker.patch.object(node_path_mock, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock, 'subscription_construction_order', 0)
        node_path_value_mock = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock, 'to_value', return_value=node_path_value_mock)

        comm_mock = mocker.Mock(spec=CommunicationStruct)
        mocker.patch.object(comm_mock, 'topic_name', topic_name)
        mocker.patch.object(comm_mock, 'publish_node_name', node_name)
        mocker.patch.object(comm_mock, 'subscribe_node_name', node_name)
        mocker.patch.object(comm_mock, 'publisher_construction_order', 0)
        mocker.patch.object(comm_mock, 'subscription_construction_order', 0)
        comm_value_mock = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock, 'to_value', return_value=comm_value_mock)

        callback = mocker.Mock()
        mocker.patch.object(callback, 'construction_order', 1)
        mocker.patch.object(comm_mock, 'subscribe_callback', callback)

        mocker.patch.object(
            NodePathSearcher, '_create_head_dummy_node_path', return_value=node_path_mock)

        mocker.patch.object(
            NodePathSearcher, '_create_tail_dummy_node_path', return_value=node_path_mock)

        searcher = NodePathSearcher(
            (node_mock,),
            (comm_mock,),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        paths = searcher.search(node_name, node_name)

        expect = PathStruct(
            None,
            (node_path_mock, comm_mock, node_path_mock),
        )
        assert [v.to_value() for v in paths] == [expect.to_value()]

    def test_to_path_construction_order(self, mocker):
        node_name_0 = '0'
        node_name_1 = '1'
        node_name_2 = '2'
        node_name_3 = '3'
        node_name_4 = '4'
        node_name_5 = '5'
        sub_const_name_0 = '@0'
        pub_const_name_0 = '@0'
        sub_const_name_1 = '@1'
        pub_const_name_1 = '@1'

        node_mock_0 = mocker.Mock(spec=NodeStruct)
        node_mock_1 = mocker.Mock(spec=NodeStruct)
        node_mock_2 = mocker.Mock(spec=NodeStruct)
        node_mock_3 = mocker.Mock(spec=NodeStruct)
        node_mock_4 = mocker.Mock(spec=NodeStruct)
        node_mock_5 = mocker.Mock(spec=NodeStruct)
        comm_mock_0 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_1 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_2 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_3 = mocker.Mock(spec=CommunicationStruct)
        node_path_mock_0 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_1 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_2 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_3 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_4 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_5 = mocker.Mock(spec=NodePathStruct)

        mocker.patch.object(node_mock_0, 'node_name', node_name_0)
        mocker.patch.object(node_mock_1, 'node_name', node_name_1)
        mocker.patch.object(node_mock_2, 'node_name', node_name_2)
        mocker.patch.object(node_mock_3, 'node_name', node_name_3)
        mocker.patch.object(node_mock_4, 'node_name', node_name_4)
        mocker.patch.object(node_mock_5, 'node_name', node_name_5)

        mocker.patch.object(node_mock_0, 'callbacks', [])
        mocker.patch.object(node_mock_1, 'callbacks', [])
        mocker.patch.object(node_mock_2, 'callbacks', [])
        mocker.patch.object(node_mock_3, 'callbacks', [])
        mocker.patch.object(node_mock_4, 'callbacks', [])
        mocker.patch.object(node_mock_5, 'callbacks', [])

        mocker.patch.object(node_path_mock_0, 'publish_topic_name', '0->1')
        mocker.patch.object(node_path_mock_0, 'subscribe_topic_name', None)
        mocker.patch.object(node_path_mock_0, 'node_name', node_name_0)
        mocker.patch.object(node_path_mock_0, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock_0, 'subscription_construction_order', 0)
        node_path_value_mock_0 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_0, 'to_value', return_value=node_path_value_mock_0)

        mocker.patch.object(node_path_mock_1, 'publish_topic_name', '1->2')
        mocker.patch.object(node_path_mock_1, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_1, 'node_name', node_name_1)
        mocker.patch.object(node_path_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_1, 'publisher_construction_order', 0)
        node_path_value_mock_1 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_1, 'to_value', return_value=node_path_value_mock_1)

        mocker.patch.object(node_path_mock_2, 'publish_topic_name', None)
        mocker.patch.object(node_path_mock_2, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_2, 'node_name', node_name_2)
        mocker.patch.object(node_path_mock_2, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_2, 'publisher_construction_order', 0)
        node_path_struct_mock_2 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_2, 'to_value', return_value=node_path_struct_mock_2)

        mocker.patch.object(comm_mock_0, 'publish_node_name', '0')
        mocker.patch.object(comm_mock_0, 'subscribe_node_name', '1')
        mocker.patch.object(comm_mock_0, 'topic_name', '0->1')
        mocker.patch.object(comm_mock_0, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_0, 'publisher_construction_order', 0)
        comm_mock_struct_0 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_0, 'to_value', return_value=comm_mock_struct_0)
        callback_0 = mocker.Mock()
        mocker.patch.object(callback_0, 'construction_order', 1)
        mocker.patch.object(comm_mock_0, 'subscribe_callback', callback_0)

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')
        mocker.patch.object(comm_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_1, 'publisher_construction_order', 0)
        comm_mock_struct_1 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_1, 'to_value', return_value=comm_mock_struct_1)
        callback_1 = mocker.Mock()
        mocker.patch.object(callback_1, 'construction_order', 2)
        mocker.patch.object(comm_mock_1, 'subscribe_callback', callback_1)

        mocker.patch.object(node_mock_0, 'paths', [node_path_mock_0])
        mocker.patch.object(node_mock_1, 'paths', [node_path_mock_1])
        mocker.patch.object(node_mock_2, 'paths', [node_path_mock_2])
        mocker.patch.object(node_mock_3, 'paths', [node_path_mock_3])

        # already tested with test_search_paths_three_nodes()
        mocker.patch.object(
            NodePathSearcher, '_create_head_dummy_node_path', return_value=node_path_mock_0)
        # already tested with test_search_paths_three_nodes()
        mocker.patch.object(
            NodePathSearcher, '_create_tail_dummy_node_path', return_value=node_path_mock_2)

        searcher = NodePathSearcher(
            (node_mock_0, node_mock_1, node_mock_2,),
            (comm_mock_0, comm_mock_1),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        graph_path_mock = mocker.Mock(spec=GraphPath)
        edge_mock_0 = mocker.Mock(GraphEdge)
        edge_mock_1 = mocker.Mock(GraphEdge)
        graph_node_mock_0 = mocker.Mock(spec=GraphNode)
        graph_node_mock_1 = mocker.Mock(spec=GraphNode)
        graph_node_mock_2 = mocker.Mock(spec=GraphNode)
        mocker.patch.object(
            graph_path_mock, 'nodes',
            [GraphNode(node_name_0)],
        )

        mocker.patch.object(graph_node_mock_0, 'node_name', node_name_0)
        mocker.patch.object(graph_node_mock_1, 'node_name', node_name_1)
        mocker.patch.object(graph_node_mock_2, 'node_name', node_name_2)

        mocker.patch.object(edge_mock_0, 'node_name_from', node_name_0)
        mocker.patch.object(edge_mock_0, 'node_name_to', node_name_1)
        mocker.patch.object(edge_mock_0, 'label', '0->1'+sub_const_name_0+pub_const_name_0)
        mocker.patch.object(edge_mock_0, 'node_from', graph_node_mock_0)
        mocker.patch.object(edge_mock_0, 'node_to', graph_node_mock_1)
        mocker.patch.object(edge_mock_1, 'node_name_from', node_name_1)
        mocker.patch.object(edge_mock_1, 'node_name_to', node_name_2)
        mocker.patch.object(edge_mock_1, 'label', '1->2'+sub_const_name_0+pub_const_name_0)
        mocker.patch.object(edge_mock_1, 'node_from', graph_node_mock_1)
        mocker.patch.object(edge_mock_1, 'node_to', graph_node_mock_2)
        mocker.patch.object(
            graph_path_mock, 'edges', [edge_mock_0, edge_mock_1]
        )

        # already tested with test_search_paths_three_nodes()
        pub_mock = mocker.Mock(spec=PublisherStruct)
        mocker.patch.object(NodePathSearcher, '_get_publisher', return_value=pub_mock)
        # already tested with test_search_paths_three_nodes()
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        mocker.patch.object(NodePathSearcher, '_get_subscription', return_value=sub_mock)
        path = searcher._to_path(graph_path_mock)

        expected = PathStruct(
            None, (node_path_mock_0, comm_mock_0, node_path_mock_1, comm_mock_1, node_path_mock_2)
        )
        assert path.to_value() == expected.to_value()

        # Even if the topic_name is the same, if the subscription_construction_order and
        # publisher_construction_order are different, the paths will be different.
        mocker.patch.object(node_path_mock_3, 'publish_topic_name', '0->1')
        mocker.patch.object(node_path_mock_3, 'subscribe_topic_name', None)
        mocker.patch.object(node_path_mock_3, 'node_name', node_name_3)
        mocker.patch.object(node_path_mock_3, 'publisher_construction_order', 1)
        mocker.patch.object(node_path_mock_3, 'subscription_construction_order', 1)
        node_path_value_mock_3 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_3, 'to_value', return_value=node_path_value_mock_3)

        mocker.patch.object(node_path_mock_4, 'publish_topic_name', '1->2')
        mocker.patch.object(node_path_mock_4, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_4, 'node_name', node_name_4)
        mocker.patch.object(node_path_mock_4, 'publisher_construction_order', 1)
        mocker.patch.object(node_path_mock_4, 'subscription_construction_order', 1)
        node_path_value_mock_4 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_4, 'to_value', return_value=node_path_value_mock_4)

        mocker.patch.object(node_path_mock_5, 'publish_topic_name', None)
        mocker.patch.object(node_path_mock_5, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_5, 'node_name', node_name_5)
        mocker.patch.object(node_path_mock_5, 'publisher_construction_order', 1)
        mocker.patch.object(node_path_mock_5, 'subscription_construction_order', 1)
        node_path_value_mock_5 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_5, 'to_value', return_value=node_path_value_mock_5)

        mocker.patch.object(node_mock_4, 'paths', [node_path_mock_4])
        mocker.patch.object(node_mock_5, 'paths', [node_path_mock_5])

        mocker.patch.object(comm_mock_0, 'publish_node_name', '0')
        mocker.patch.object(comm_mock_0, 'subscribe_node_name', '1')
        mocker.patch.object(comm_mock_0, 'topic_name', '0->1')
        mocker.patch.object(comm_mock_0, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_0, 'publisher_construction_order', 0)
        comm_mock_struct_0 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_0, 'to_value', return_value=comm_mock_struct_0)
        callback_0 = mocker.Mock()
        mocker.patch.object(callback_0, 'construction_order', 1)
        mocker.patch.object(comm_mock_0, 'subscribe_callback', callback_0)

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')
        mocker.patch.object(comm_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_1, 'publisher_construction_order', 0)
        comm_mock_struct_1 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_1, 'to_value', return_value=comm_mock_struct_1)
        callback_1 = mocker.Mock()
        mocker.patch.object(callback_1, 'construction_order', 2)
        mocker.patch.object(comm_mock_1, 'subscribe_callback', callback_1)

        mocker.patch.object(comm_mock_2, 'publish_node_name', '3')
        mocker.patch.object(comm_mock_2, 'subscribe_node_name', '4')
        mocker.patch.object(comm_mock_2, 'topic_name', '0->1')
        mocker.patch.object(comm_mock_2, 'subscription_construction_order', 1)
        mocker.patch.object(comm_mock_2, 'publisher_construction_order', 1)
        comm_mock_struct_2 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_2, 'to_value', return_value=comm_mock_struct_2)
        callback_2 = mocker.Mock()
        mocker.patch.object(callback_2, 'construction_order', 3)
        mocker.patch.object(comm_mock_2, 'subscribe_callback', callback_2)

        mocker.patch.object(comm_mock_3, 'publish_node_name', '4')
        mocker.patch.object(comm_mock_3, 'subscribe_node_name', '5')
        mocker.patch.object(comm_mock_3, 'topic_name', '1->2')
        mocker.patch.object(comm_mock_3, 'subscription_construction_order', 1)
        mocker.patch.object(comm_mock_3, 'publisher_construction_order', 1)
        comm_mock_struct_3 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_3, 'to_value', return_value=comm_mock_struct_3)
        callback_3 = mocker.Mock()
        mocker.patch.object(callback_3, 'construction_order', 4)
        mocker.patch.object(comm_mock_3, 'subscribe_callback', callback_3)

        mocker.patch.object(graph_node_mock_0, 'node_name', node_name_3)
        mocker.patch.object(graph_node_mock_1, 'node_name', node_name_4)
        mocker.patch.object(graph_node_mock_2, 'node_name', node_name_5)
        mocker.patch.object(edge_mock_0, 'node_name_from', node_name_3)
        mocker.patch.object(edge_mock_0, 'node_name_to', node_name_4)
        mocker.patch.object(edge_mock_0, 'label', '0->1'+sub_const_name_1+pub_const_name_1)
        mocker.patch.object(edge_mock_0, 'node_from', graph_node_mock_0)
        mocker.patch.object(edge_mock_0, 'node_to', graph_node_mock_1)
        mocker.patch.object(edge_mock_1, 'node_name_from', node_name_4)
        mocker.patch.object(edge_mock_1, 'node_name_to', node_name_5)
        mocker.patch.object(edge_mock_1, 'label', '1->2'+sub_const_name_1+pub_const_name_1)
        mocker.patch.object(edge_mock_1, 'node_from', graph_node_mock_1)
        mocker.patch.object(edge_mock_1, 'node_to', graph_node_mock_2)
        mocker.patch.object(
            graph_path_mock, 'edges', [edge_mock_0, edge_mock_1]
        )
        mocker.patch.object(
            NodePathSearcher, '_create_head_dummy_node_path', return_value=node_path_mock_3)
        mocker.patch.object(
            NodePathSearcher, '_create_tail_dummy_node_path', return_value=node_path_mock_5)

        searcher = NodePathSearcher(
            (node_mock_0, node_mock_1, node_mock_2, node_mock_3, node_mock_4, node_mock_5),
            (comm_mock_0, comm_mock_1, comm_mock_2, comm_mock_3),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        path = searcher._to_path(graph_path_mock)

        expected = PathStruct(
            None, (node_path_mock_3, comm_mock_2, node_path_mock_4, comm_mock_3, node_path_mock_5)
        )
        assert path.to_value() == expected.to_value()

    def test_find_comm(self, mocker):
        node_mock = mocker.Mock(spec=NodeStruct)
        node_path_mock = mocker.Mock(spec=NodePathStruct)
        node_path_value_mock = mocker.Mock(spec=NodePathStructValue)
        comm_mock_1 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_2 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_struct = mocker.Mock(spec=CommunicationStructValue)

        mocker.patch.object(node_path_mock, 'publish_topic_name', '0->1')
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', None)
        mocker.patch.object(node_path_mock, 'node_name', '0')
        mocker.patch.object(node_path_mock, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock, 'to_value', return_value=node_path_value_mock)

        mocker.patch.object(node_mock, 'node_name', '0')
        mocker.patch.object(node_mock, 'paths', [node_path_mock])

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')
        mocker.patch.object(comm_mock_1, 'subscription_construction_order', 1)
        mocker.patch.object(comm_mock_1, 'publisher_construction_order', 0)
        mocker.patch.object(comm_mock_1, 'to_value', return_value=comm_mock_struct)
        callback_1 = mocker.Mock()
        mocker.patch.object(callback_1, 'construction_order', 1)
        mocker.patch.object(comm_mock_1, 'subscribe_callback', callback_1)
        mocker.patch.object(comm_mock_2, 'publish_node_name', '3')
        mocker.patch.object(comm_mock_2, 'subscribe_node_name', '4')
        mocker.patch.object(comm_mock_2, 'topic_name', '0->1')
        mocker.patch.object(comm_mock_2, 'subscription_construction_order', 1)
        mocker.patch.object(comm_mock_2, 'publisher_construction_order', 1)
        comm_mock_struct_2 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_2, 'to_value', return_value=comm_mock_struct_2)
        callback_2 = mocker.Mock()
        mocker.patch.object(callback_2, 'construction_order', 2)
        mocker.patch.object(comm_mock_2, 'subscribe_callback', callback_2)

        searcher = NodePathSearcher(
            (node_mock,),
            (comm_mock_1, comm_mock_2),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        comm = searcher._find_comm('3', '4', '0->1', 1, 1)
        assert comm == comm_mock_2

        with pytest.raises(ItemNotFoundError):
            searcher._find_comm('1', '2', '1->2', 0, 0)

    def test_find_node_path(self, mocker):
        node_mock_1 = mocker.Mock(spec=NodeStruct)
        node_mock_2 = mocker.Mock(spec=NodeStruct)
        node_path_mock_1 = mocker.Mock(spec=NodePathStruct)
        node_path_mock_2 = mocker.Mock(spec=NodePathStruct)
        node_path_value_mock_1 = mocker.Mock(spec=NodePathStructValue)
        node_path_value_mock_2 = mocker.Mock(spec=NodePathStructValue)
        comm_mock = mocker.Mock(spec=CommunicationStruct)
        callback = mocker.Mock()
        mocker.patch.object(callback, 'construction_order', 1)
        mocker.patch.object(comm_mock, 'subscribe_callback', callback)

        mocker.patch.object(node_path_mock_1, 'publish_topic_name', '1->2')
        mocker.patch.object(node_path_mock_1, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_1, 'node_name', '0')
        mocker.patch.object(node_path_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_1, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock_1, 'to_value', return_value=node_path_value_mock_1)

        mocker.patch.object(node_path_mock_2, 'publish_topic_name', None)
        mocker.patch.object(node_path_mock_2, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_2, 'node_name', '1')
        mocker.patch.object(node_path_mock_2, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_2, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock_2, 'to_value', return_value=node_path_value_mock_2)

        mocker.patch.object(node_mock_1, 'node_name', '0')
        mocker.patch.object(node_mock_1, 'paths', [node_path_mock_1])
        mocker.patch.object(node_mock_2, 'node_name', '1')
        mocker.patch.object(node_mock_2, 'paths', [node_path_mock_2])

        searcher = NodePathSearcher(
            (node_mock_1, node_mock_2),
            (comm_mock,),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )
        node_path = searcher._find_node_path('0->1', '1->2', '0', 0, 0)
        assert node_path == node_path_mock_1

        with pytest.raises(ItemNotFoundError):
            searcher._find_node_path('0->1', '1->2', '0', 1, 0)

    def test_max_callback_construction_order(self, mocker):
        graph_mock = mocker.Mock(spec=Graph)
        mocker.patch('caret_analyze.architecture.graph_search.Graph', return_value=graph_mock)

        node_mock1 = mocker.Mock(spec=NodeStruct)
        node_mock1.node_name = 'node1'
        node_mock1.paths = []

        node_mock2 = mocker.Mock(spec=NodeStruct)
        node_mock2.node_name = 'node2'
        node_mock2.paths = []

        comm_mock1 = mocker.Mock(spec=CommunicationStruct)
        comm_mock1.topic_name = 'topic1'
        comm_mock1.publish_node_name = 'node1'
        comm_mock1.subscribe_node_name = 'node2'
        comm_mock1.subscription_construction_order = 1
        comm_mock1.publisher_construction_order = 1
        callback1 = mocker.Mock()
        mocker.patch.object(
            callback1,
            'construction_order',
            (DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING - 1)
        )
        mocker.patch.object(comm_mock1, 'subscribe_callback', callback1)

        comm_mock2 = mocker.Mock(spec=CommunicationStruct)
        comm_mock2.topic_name = 'topic2'
        comm_mock2.publish_node_name = 'node1'
        comm_mock2.subscribe_node_name = 'node2'
        comm_mock2.subscription_construction_order = 2
        comm_mock2.publisher_construction_order = 2
        callback2 = mocker.Mock()
        mocker.patch.object(
            callback2,
            'construction_order',
            (DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING + 1)
        )
        mocker.patch.object(comm_mock2, 'subscribe_callback', callback2)

        NodePathSearcher(
            (node_mock1, node_mock2),
            (comm_mock1, comm_mock2),
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        expected_edge_label1 = (
            f'{comm_mock1.topic_name}'
            f'@{comm_mock1.subscription_construction_order}'
            f'@{comm_mock1.publisher_construction_order}'
        )

        graph_mock.add_edge.assert_called_once_with(
            GraphNode(comm_mock1.publish_node_name),
            GraphNode(comm_mock1.subscribe_node_name),
            expected_edge_label1
        )

        assert graph_mock.add_edge.call_count == 1

        graph_mock.search_paths.return_value = []

        start_node = GraphNode('node1')
        goal_node = GraphNode('node2')
        paths = graph_mock.search_paths(start_node, goal_node)

        assert paths == [], 'Expected no paths to be found due to max callback construction order.'
