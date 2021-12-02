# Copyright 2021 Research Institute of Systems Planning, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apacato_node_branch_indexese.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List

import pytest
from pytest_mock import MockerFixture

from caret_analyze.architecture.graph_search import (CallbackPathSearcher, GraphNode,
                                                     GraphPathCore, Graph, GraphPath, GraphEdge,
                                                     NodePathSearcher, GraphCore, GraphEdgeCore)
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.value_objects import (CallbackStructValue,
                                         CommunicationStructValue,
                                         NodePathStructValue,
                                         NodeStructValue,
                                         PathStructValue,
                                         PublisherStructValue,
                                         VariablePassingStructValue,
                                         SubscriptionStructValue)


class TestGraphBranch:

    def test_eq(self):
        node0 = GraphNode('/node0/callback0')
        node1 = GraphNode('/node1/callback1')

        assert GraphBranch(node0, node1) == GraphBranch(node0, node1)
        assert GraphBranch(node0, node0) != GraphBranch(node0, node1)


class TestGraphNode:

    def test_eq(self):
        assert GraphNode('/node0/callback0') == GraphNode('/node0/callback0')
        assert GraphNode('/node0/callback0') != GraphNode('/node1/callback1')


class TestGraphPath:

    def test_to_graph_nodes_multi_nodes(self):
        path = GraphPathCore()
        assert len(path) == 0
        assert len(path.to_graph_nodes()) == 0

        nodes = [
            GraphNode('/node0/callback0'),
            GraphNode('/node1/callback1'),
            GraphNode('/node2/callback2'),
        ]
        path.append(GraphBranch(nodes[0], nodes[1]))
        path.append(GraphBranch(nodes[1], nodes[2]))

        assert len(path) == 2
        assert nodes == path.to_graph_nodes()

    def test_to_graph_nodes_single_node(self):
        path = GraphPathCore()

        nodes = [GraphNode('/node0/callback0')]
        path.append(GraphBranch(nodes[0], nodes[0]))

        assert len(path) == 1
        assert nodes == path.to_graph_nodes()


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
        assert [GraphEdgeCore(0, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 2), GraphEdgeCore(2, 4)] in r
        assert [GraphEdgeCore(0, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 4)] in r
        assert [GraphEdgeCore(0, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3), GraphEdgeCore(3, 2), GraphEdgeCore(2, 4)] in r
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
        assert [GraphEdgeCore(0, 1, '0'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')] in r
        assert [GraphEdgeCore(0, 1, '0'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')] in r
        assert [GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 1, '5'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')] in r
        assert [GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 1, '5'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')] in r
        assert [GraphEdgeCore(0, 2, '1'), GraphEdgeCore(2, 4, '6')] in r
        assert [GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 1, '5'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 4, '3')] in r
        assert [GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 1, '5'), GraphEdgeCore(1, 3, '2'), GraphEdgeCore(3, 2, '4'), GraphEdgeCore(2, 4, '6')] in r
        assert [GraphEdgeCore(0, 2, '1_'), GraphEdgeCore(2, 4, '6')] in r

    def test_search_simple_loop_case(self):
        g = GraphCore()

        g.add_edge(0, 1)
        g.add_edge(1, 3)
        g.add_edge(1, 2)
        g.add_edge(2, 1)

        r = g.search_paths(0, 3)
        assert len(r) == 2
        assert [GraphEdgeCore(0, 1), GraphEdgeCore(1, 2), GraphEdgeCore(2, 1), GraphEdgeCore(1, 3)] in r
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
        assert GraphPath([GraphEdge(node_0, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)]) in r
        assert GraphPath([GraphEdge(node_0, node_2), GraphEdge(node_2, node_3), GraphEdge(node_3, node_4)]) in r

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
        assert [GraphEdge(node_0, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)] in r
        assert [GraphEdge(node_0, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_2), GraphEdge(node_2, node_4)] in r
        assert [GraphEdge(node_0, node_2), GraphEdge(node_2, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_4)] in r
        assert [GraphEdge(node_0, node_2), GraphEdge(node_2, node_1), GraphEdge(node_1, node_3), GraphEdge(node_3, node_2), GraphEdge(node_2, node_4)] in r
        assert [GraphEdge(node_0, node_2), GraphEdge(node_2, node_4)] in r

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
        assert [GraphEdge(node_0, node_1, '0'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_4, '3')] in r
        assert [GraphEdge(node_0, node_1, '0'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_2, '4'), GraphEdge(node_2, node_4, '6')] in r
        assert [GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_1, '5'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_4, '3')] in r
        assert [GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_1, '5'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_2, '4'), GraphEdge(node_2, node_4, '6')] in r
        assert [GraphEdge(node_0, node_2, '1'), GraphEdge(node_2, node_4, '6')] in r
        assert [GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_1, '5'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_4, '3')] in r
        assert [GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_1, '5'), GraphEdge(node_1, node_3, '2'), GraphEdge(node_3, node_2, '4'), GraphEdge(node_2, node_4, '6')] in r
        assert [GraphEdge(node_0, node_2, '1_'), GraphEdge(node_2, node_4, '6')] in r

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
        assert [GraphEdge(node_0, node_1), GraphEdge(node_1, node_2), GraphEdge(node_2, node_1), GraphEdge(node_1, node_3)] in r
        assert [GraphEdge(node_0, node_1), GraphEdge(node_1, node_3)] in r


class TestGraphSearcher:

    def test_search_simple_case(self):
        node0 = GraphNode('/node0/callback0')
        node1 = GraphNode('/node1/callback1')

        branches: List[GraphBranch] = []
        branches.append(GraphBranch(node0, node1))
        searcher = GraphSearcher(branches)

        paths = searcher.search(node0, node1)
        path = paths[0]
        assert path.to_graph_nodes() == [node0, node1]

    def test_search_merge_case(self):
        node_s = GraphNode('/node_start/callback0')
        node_0 = GraphNode('/node_0/callback0')
        node_1 = GraphNode('/node_1/callback0')
        node_e = GraphNode('/node_end/callback0')

        branches: List[GraphBranch] = []

        branches.append(GraphBranch(node_s, node_0))
        branches.append(GraphBranch(node_s, node_1))
        branches.append(GraphBranch(node_0, node_e))
        branches.append(GraphBranch(node_1, node_e))

        searcher = GraphSearcher(branches)

        paths = searcher.search(node_s, node_e)
        assert len(paths) == 2

        assert paths[0].to_graph_nodes() == [node_s, node_0, node_e]
        nodes = paths[1].to_graph_nodes()
        assert paths[1].to_graph_nodes() == [node_s, node_1, node_e]

    def test_search_loop_case(self):
        node_s = GraphNode('/node_start/callback0')
        node_e = GraphNode('/node_end/callback0')

        branches: List[GraphBranch] = []

        branches.append(GraphBranch(node_s, node_e))
        branches.append(GraphBranch(node_e, node_s))

        searcher = GraphSearcher(branches)

        paths = searcher.search(node_s, node_e)
        assert len(paths) == 1
        assert paths[0].to_graph_nodes() == [node_s, node_e]

        paths = searcher.search(node_s, node_s)
        assert len(paths) == 1
        assert paths[0].to_graph_nodes() == [node_s, node_e, node_s]

    def test_search_complex_loop_case(self):

        node_s = GraphNode('s')
        node_1 = GraphNode('1')
        node_2 = GraphNode('2')
        node_3 = GraphNode('3')
        node_e = GraphNode('e')

        nodes: List[GraphNode] = []
        nodes.append(node_s)
        nodes.append(node_1)
        nodes.append(node_2)
        nodes.append(node_3)
        nodes.append(node_e)

        branches: List[GraphBranch] = []

        branches.append(GraphBranch(node_s, node_1))
        branches.append(GraphBranch(node_s, node_2))
        branches.append(GraphBranch(node_1, node_3))
        branches.append(GraphBranch(node_3, node_e))
        branches.append(GraphBranch(node_3, node_2))
        branches.append(GraphBranch(node_2, node_1))
        branches.append(GraphBranch(node_2, node_e))

        searcher = GraphSearcher(branches, nodes)

        paths = searcher.search(node_s, node_e)
        assert len(paths) == 5
        assert paths[0].to_graph_nodes() == [
            node_s, node_1, node_e
        ]
        assert paths[1].to_graph_nodes() == [
            node_s, node_3, node_2, node_e
        ]
        assert paths[2].to_graph_nodes() == [
            node_s, node_2, node_1, node_3, node_e
        ]
        assert paths[3].to_graph_nodes() == [
            node_s, node_2, node_3, node_2, node_e
        ]
        assert paths[4].to_graph_nodes() == [
            node_s, node_2, node_e
        ]

    # def test_measure_performance(self):
    #     num = 2500
    #     nodes = []
    #     for i in range(num):
    #         nodes.append(GraphNode(str(i)))

    #     branches = []
    #     for i in range(num-1):
    #         branches.append(GraphBranch(nodes[i], nodes[i+1]))

    #     searcher = GraphSearcher(branches)
    #     paths = searcher.search(nodes[0], nodes[-1])
    #     assert len(paths) == 1
    #     assert paths[0].to_graph_nodes() == nodes


class TestCallbackPathSearcher:
    def test_empty(self, mocker: MockerFixture):
        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'callback_values', ())
        mocker.patch.object(node_mock, 'variable_passing_values', ())
        searcher = CallbackPathSearcher(node_mock)

        sub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        pub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        paths = searcher.search(sub_cb_mock, pub_cb_mock)
        assert paths == ()

    def test_search(self, mocker: MockerFixture):
        node_mock = mocker.Mock(spec=NodeStructValue)

        sub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        pub_cb_mock = mocker.Mock(spec=CallbackStructValue)

        searcher_mock = mocker.Mock(spec=GraphSearcher)
        mocker.patch(
            'caret_analyze.architecture.graph_search.GraphSearcher',
            return_value=searcher_mock
        )
        mocker.patch.object(searcher_mock, 'search',
                            return_value=[GraphPathCore()])
        searcher = CallbackPathSearcher(node_mock, (), ())

        path_mock = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(searcher, '_to_paths', return_value=[path_mock])

        sub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        pub_cb_mock = mocker.Mock(spec=CallbackStructValue)

        paths = searcher.search(sub_cb_mock, pub_cb_mock)
        assert paths == (path_mock,)

    def test_to_path(self, mocker: MockerFixture):
        node_mock = mocker.Mock(spec=NodeStructValue)

        sub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        pub_cb_mock = mocker.Mock(spec=CallbackStructValue)
        var_pas_mock = mocker.Mock(spec=VariablePassingStructValue)

        sub_topic_name = '/sub'
        pub_topic_name = '/pub'

        searcher = CallbackPathSearcher(
            node_mock, (sub_cb_mock, pub_cb_mock), (var_pas_mock,))

        sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)
        pub_info_mock = mocker.Mock(spec=PublisherStructValue)

        mocker.patch.object(sub_info_mock, 'topic_name', sub_topic_name)
        mocker.patch.object(pub_info_mock, 'topic_name', pub_topic_name)

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
        graph_path_mock = mocker.Mock(spec=GraphPathCore)
        mocker.patch.object(graph_path_mock, 'to_graph_nodes',
                            return_value=[
                                graph_node_mock_0,
                                graph_node_mock_1,
                                graph_node_mock_2,
                                graph_node_mock_3,
                                ])

        node_path = searcher._to_path(
            graph_path_mock, sub_topic_name, pub_topic_name)
        expected = NodePathStructValue(
            '/node', sub_info_mock, pub_info_mock, tuple(chain), None)
        assert node_path == expected

        node_path = searcher._to_path(graph_path_mock, None, pub_topic_name)
        expected = NodePathStructValue(
            '/node', None, pub_info_mock, tuple(chain), None)
        assert node_path == expected

        node_path = searcher._to_path(graph_path_mock, sub_topic_name, None)
        expected = NodePathStructValue(
            '/node', sub_info_mock, None, tuple(chain), None)
        assert node_path == expected


class TestNodePathSearcher:
    def test_empty(self, mocker: MockerFixture):
        searcher = NodePathSearcher((), ())
        with pytest.raises(ItemNotFoundError):
            searcher.search('node_name_not_exist', 'node_name_not_exist')

        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'paths_info', [])
        mocker.patch.object(node_mock, 'node_name', 'node')
        mocker.patch.object(node_mock, 'publish_topic_names', [])
        mocker.patch.object(node_mock, 'subscribe_topic_names', [])
        searcher = NodePathSearcher((node_mock,), ())
        paths = searcher.search('node', 'node')
        assert paths == []

    def test_search(self, mocker: MockerFixture):
        graph_searcher_mock = mocker.Mock(spec=GraphSearcher)
        mocker.patch(
            'caret_analyze.architecture.graph_search.GraphSearcher',
            return_value=graph_searcher_mock)
        searcher = NodePathSearcher((), ())

        src_graph_mock = mocker.Mock(spec=GraphNode)
        mocker.patch.object(searcher, '_to_src_graph_nodes',
                            return_value=[src_graph_mock])

        dst_graph_mock = mocker.Mock(spec=GraphNode)
        mocker.patch.object(searcher, '_to_dst_graph_nodes',
                            return_value=[dst_graph_mock])

        graph_path_mock = mocker.Mock(spec=GraphPathCore)
        mocker.patch.object(graph_searcher_mock, 'search',
                            return_value=[graph_path_mock])

        path_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(searcher, '_to_path', return_value=path_mock)
        paths = searcher.search('start_node_name', 'end_node_name')

        assert paths == [path_mock]
        assert graph_searcher_mock.search.call_args == (
            (src_graph_mock, dst_graph_mock),)

    def test_to_dst_graph_nodes(self, mocker: MockerFixture):
        node_name = '/node'
        topic_name = '/topic'

        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'node_name', node_name)
        mocker.patch.object(node_mock, 'paths_info', ())
        mocker.patch.object(node_mock, 'subscribe_topic_names', (topic_name,))

        searcher = NodePathSearcher((node_mock,), ())

        graph_nodes = searcher._to_dst_graph_nodes(node_name)

        graph_name = searcher._to_node_point_name(node_name, topic_name, 'sub')
        expect = GraphNode(graph_name)

        assert graph_nodes == [expect]

    def test_to_src_graph_nodes(self, mocker: MockerFixture):
        node_name = '/node'
        topic_name = '/topic'

        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'node_name', node_name)
        mocker.patch.object(node_mock, 'paths_info', ())
        mocker.patch.object(node_mock, 'publish_topic_names', (topic_name,))

        searcher = NodePathSearcher((node_mock,), ())

        graph_nodes = searcher._to_src_graph_nodes(node_name)

        graph_name = searcher._to_node_point_name(node_name, topic_name, 'pub')
        expect = GraphNode(graph_name)

        assert graph_nodes == [expect]

    def test_to_path(self, mocker: MockerFixture):
        node_name = '/node'
        topic_name = '/topic'

        node_mock = mocker.Mock(spec=NodeStructValue)
        comm_mock = mocker.Mock(spec=CommunicationStructValue)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'node_name', node_name)

        mocker.patch.object(comm_mock, 'publish_node_name', node_name)
        mocker.patch.object(comm_mock, 'subscribe_node_name', node_name)
        mocker.patch.object(comm_mock, 'topic_name', topic_name)

        mocker.patch.object(node_mock, 'paths_info', [node_path_mock])

        searcher = NodePathSearcher((node_mock,), (comm_mock,))
        graph_path_mock = mocker.Mock(spec=GraphPathCore)
        mocker.patch.object(
            graph_path_mock, 'to_graph_nodes',
            return_value=[
                GraphNode(searcher._to_node_point_name(node_name, topic_name, 'pub')),
                GraphNode(searcher._to_node_point_name(node_name, topic_name, 'sub')),
            ]
        )
        path = searcher._to_path(graph_path_mock)
        expected = PathStructValue(
            None, (node_path_mock, comm_mock, node_path_mock)
        )
        assert path == expected

    def test_single_node_cyclic_case(self, mocker: MockerFixture):
        node_name = '/node'
        topic_name = '/chatter'

        node_mock = mocker.Mock(spec=NodeStructValue)

        node_path_mock = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_mock, 'paths_info', (node_path_mock,))
        mocker.patch.object(node_mock, 'node_name', node_name)
        mocker.patch.object(node_mock, 'publish_topic_names', [topic_name])
        mocker.patch.object(node_mock, 'subscribe_topic_names', [topic_name])

        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(node_path_mock, 'node_name', node_name)

        comm_mock = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock, 'topic_name', topic_name)
        mocker.patch.object(comm_mock, 'publish_node_name', node_name)
        mocker.patch.object(comm_mock, 'subscribe_node_name', node_name)

        searcher = NodePathSearcher((node_mock,), (comm_mock,))
        paths = searcher.search(node_name, node_name)

        expect = PathStructValue(
            None,
            (node_path_mock, comm_mock, node_path_mock),
        )
        assert paths == [expect]
