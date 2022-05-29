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

from caret_analyze.architecture.graph_search.graph_search import (
    Graph,
    GraphCore,
    GraphEdge,
    GraphEdgeCore,
    GraphNode,
    GraphPath,
    GraphPathCore)

from caret_analyze.exceptions import InvalidArgumentError

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

    def test_search_double_loop_case(self):
        g = GraphCore()
        g.add_edge(0, 1)
        g.add_edge(1, 0)

        g.add_edge(0, 2)

        g.add_edge(2, 3)
        g.add_edge(3, 2)

        r = g.search_paths(0, 2)
        assert r == [
            [GraphEdgeCore(0, 2)],
            [GraphEdgeCore(0, 1), GraphEdgeCore(1, 0), GraphEdgeCore(0, 2)],
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

    def test_search_paths_seq_simple(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')

        g.add_edge(node_0, node_1)
        g.add_edge(node_1, node_2)

        with pytest.raises(InvalidArgumentError):
            g.search_paths([node_0])

        r = g.search_paths([node_0, node_2])
        assert len(r) == 1
        assert r[0] == [
            GraphEdge(node_0, node_1),
            GraphEdge(node_1, node_2),
        ]

        r = g.search_paths([node_0, node_1, node_2])
        assert len(r) == 1
        assert r[0] == [
            GraphEdge(node_0, node_1),
            GraphEdge(node_1, node_2),
        ]

    def test_search_paths_seq_with_label(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')

        g.add_edge(node_0, node_1, '/topic_a')
        g.add_edge(node_0, node_1, '/topic_b')
        g.add_edge(node_1, node_2, '/topic_c')
        g.add_edge(node_1, node_2, '/topic_d')

        expect = [
            [
                GraphEdge(node_0, node_1, '/topic_a'),
                GraphEdge(node_1, node_2, '/topic_c'),
            ],
            [
                GraphEdge(node_0, node_1, '/topic_b'),
                GraphEdge(node_1, node_2, '/topic_c'),
            ],
            [
                GraphEdge(node_0, node_1, '/topic_a'),
                GraphEdge(node_1, node_2, '/topic_d'),
            ],
            [
                GraphEdge(node_0, node_1, '/topic_b'),
                GraphEdge(node_1, node_2, '/topic_d'),
            ],
        ]

        r = g.search_paths([node_0, node_2])
        assert len(r) == 4
        assert r[0] in expect
        assert r[1] in expect
        assert r[2] in expect
        assert r[3] in expect

        r = g.search_paths([node_0, node_1, node_2])
        assert len(r) == 4
        assert r[0] in expect
        assert r[1] in expect
        assert r[2] in expect
        assert r[3] in expect

    def test_search_paths_seq_loop(self):
        g = Graph()
        node_0 = GraphNode('0')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')

        g.add_edge(node_0, node_1)
        g.add_edge(node_1, node_2)
        g.add_edge(node_2, node_1)
        g.add_edge(node_1, node_3)

        r = g.search_paths([node_0, node_1, node_3])
        assert len(r) == 2
        expect = [
            [
                GraphEdge(node_0, node_1),
                GraphEdge(node_1, node_3),
            ],
            [
                GraphEdge(node_0, node_1),
                GraphEdge(node_1, node_2),
                GraphEdge(node_2, node_1),
                GraphEdge(node_1, node_3),
            ],
        ]
        assert r[0] in expect
        assert r[1] in expect

# class TestCallbackPathSearcher:

#     def test_empty(self, mocker: MockerFixture):
#         node_mock = mocker.Mock(spec=Node)
#         mocker.patch.object(node_mock, 'callbacks', ())
#         mocker.patch.object(node_mock, 'variable_passings', ())
#         searcher = CallbackPathSearcher(node_mock)

#         sub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         pub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         paths = searcher.search(sub_cb_mock, pub_cb_mock)
#         assert paths == ()

#     def test_search(self, mocker: MockerFixture):
#         node_mock = mocker.Mock(spec=Node)

#         sub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         pub_cb_mock = mocker.Mock(spec=CallbackStruct)

#         mocker.patch.object(node_mock, 'callbacks', [])
#         mocker.patch.object(node_mock, 'variable_passings', [])

#         searcher_mock = mocker.Mock(spec=Graph)
#         mocker.patch(
#             'caret_analyze.architecture.graph_search.Graph',
#             return_value=searcher_mock
#         )
#         mocker.patch.object(searcher_mock, 'search_paths',
#                             return_value=[GraphPath()])
#         searcher = CallbackPathSearcher(node_mock)

#         path_mock = mocker.Mock(spec=NodePathStructValue)
#         mocker.patch.object(searcher, '_to_paths', return_value=[path_mock])

#         sub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         pub_cb_mock = mocker.Mock(spec=CallbackStruct)

#         paths = searcher.search(sub_cb_mock, pub_cb_mock)
#         assert paths == (path_mock,)

#     def test_to_path(self, mocker: MockerFixture):
#         node_mock = mocker.Mock(spec=Node)

#         sub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         pub_cb_mock = mocker.Mock(spec=CallbackStruct)
#         var_pas_mock = mocker.Mock(spec=VariablePassingStructValue)

#         sub_topic_name = '/sub'
#         pub_topic_name = '/pub'

#         mocker.patch.object(node_mock, 'node_name', '/node')
#         mocker.patch.object(node_mock, 'callbacks', [pub_cb_mock, sub_cb_mock])
#         mocker.patch.object(node_mock, 'variable_passings', [var_pas_mock])

#         searcher = CallbackPathSearcher(node_mock)

#         sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)
#         pub_info_mock = mocker.Mock(spec=PublisherStructValue)

#         mocker.patch.object(sub_info_mock, 'topic_name', sub_topic_name)
#         mocker.patch.object(pub_info_mock, 'topic_name', pub_topic_name)

#         mocker.patch.object(sub_cb_mock, 'callback_name', 'cb0')
#         mocker.patch.object(pub_cb_mock, 'callback_name', 'cb1')
#         mocker.patch.object(var_pas_mock, 'callback_name_read', 'cb1')
#         mocker.patch.object(var_pas_mock, 'callback_name_write', 'cb0')

#         graph_node_mock_0 = GraphNode(
#             CallbackPathSearcher._to_node_point_name(sub_cb_mock.callback_name, 'read')
#         )
#         graph_node_mock_1 = GraphNode(
#             CallbackPathSearcher._to_node_point_name(sub_cb_mock.callback_name, 'write')
#         )
#         graph_node_mock_2 = GraphNode(
#             CallbackPathSearcher._to_node_point_name(pub_cb_mock.callback_name, 'read')
#         )
#         graph_node_mock_3 = GraphNode(
#             CallbackPathSearcher._to_node_point_name(pub_cb_mock.callback_name, 'write')
#         )

#         chain = [sub_cb_mock, var_pas_mock, pub_cb_mock]
#         graph_path_mock = mocker.Mock(spec=GraphPath)
#         mocker.patch.object(graph_path_mock, 'nodes',
#                             [
#                                 graph_node_mock_0,
#                                 graph_node_mock_1,
#                                 graph_node_mock_2,
#                                 graph_node_mock_3,
#                             ])

#         node_path = searcher._to_path(
#             graph_path_mock, sub_topic_name, pub_topic_name)
#         expected = NodePathStructValue(
#             '/node', sub_info_mock, pub_info_mock, tuple(chain), None)
#         assert node_path == expected

#         node_path = searcher._to_path(graph_path_mock, None, pub_topic_name)
#         expected = NodePathStructValue(
#             '/node', None, pub_info_mock, tuple(chain), None)
#         assert node_path == expected

#         node_path = searcher._to_path(graph_path_mock, sub_topic_name, None)
#         expected = NodePathStructValue(
#             '/node', sub_info_mock, None, tuple(chain), None)
#         assert node_path == expected
