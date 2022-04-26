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
    GraphEdge,
    GraphNode,
    GraphPath,
    GraphPathCore
)

from caret_analyze.architecture.graph_search.node_graph import (
    NodePathSearcher
)
from caret_analyze.architecture.graph_search.path_searcher import PathBase


from caret_analyze.value_objects import (
    CommunicationStructValue,
    NodePathStructValue,
    NodeStructValue,
    PathStructValue,
    PublisherStructValue,
    SubscriptionStructValue,
)

from caret_analyze.exceptions import InvalidArgumentError

from pytest_mock import MockerFixture
import pytest


class TestNodePathSearcher:

    def test_empty(self, mocker: MockerFixture):
        searcher = NodePathSearcher((), ())

        with pytest.raises(InvalidArgumentError):
            searcher.search('node_name_not_exist', 'node_name_not_exist', 0)

        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'paths', [])
        mocker.patch.object(node_mock, 'node_name', 'node')
        mocker.patch.object(node_mock, 'publish_topic_names', [])
        mocker.patch.object(node_mock, 'subscribe_topic_names', [])
        searcher = NodePathSearcher((node_mock,), ())

        with pytest.raises(InvalidArgumentError):
            searcher.search('node', 'node')

        searcher = NodePathSearcher((), ())
        with pytest.raises(InvalidArgumentError):
            searcher.search('node', 'node')

    def test_search(self, mocker: MockerFixture):
        graph_mock = mocker.Mock(spec=Graph)
        mocker.patch(
            'caret_analyze.architecture.graph_search.node_graph.PathSearcher',
            return_value=graph_mock)
        searcher = NodePathSearcher((), ())

        path_mock = mocker.Mock(spec=PathBase)
        mocker.patch.object(graph_mock, 'search_paths', return_value=[path_mock])

        path_struct_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch('caret_analyze.architecture.graph_search.node_graph.PathStructValue',
                     return_value=path_struct_mock)
        paths = searcher.search('start_node_name', 'end_node_name')

        assert paths == [path_struct_mock]
