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

from typing import List
from trace_analysis.graph_search import GraphNode, GraphBranch, GraphPath, GraphSearcher


class TestGraphBranch:
    def test_eq(self):
        node0 = GraphNode("/node0/callback0")
        node1 = GraphNode("/node1/callback1")

        assert GraphBranch(node0, node1) == GraphBranch(node0, node1)
        assert GraphBranch(node0, node0) != GraphBranch(node0, node1)


class TestGraphNode:
    def test_eq(self):
        assert GraphNode("/node0/callback0") == GraphNode("/node0/callback0")
        assert GraphNode("/node0/callback0") != GraphNode("/node1/callback1")


class TestGraphPath:
    def test_to_graph_nodes(self):
        path = GraphPath()
        assert len(path) == 0
        assert len(path.to_graph_nodes()) == 0

        nodes = [
            GraphNode("/node0/callback0"),
            GraphNode("/node1/callback1"),
            GraphNode("/node2/callback2"),
        ]
        path.append(GraphBranch(nodes[0], nodes[1]))
        path.append(GraphBranch(nodes[1], nodes[2]))

        assert len(path) == 2
        assert nodes == path.to_graph_nodes()


class TestGraphSearcher:
    def test_search(self):
        node0 = GraphNode("/node0/callback0")
        node1 = GraphNode("/node1/callback1")

        branches: List[GraphBranch] = []
        branches.append(GraphBranch(node0, node1))
        searcher = GraphSearcher(branches)

        paths = searcher.search(node0, node1)
        path = paths[0]
        assert path.to_graph_nodes() == [node0, node1]
