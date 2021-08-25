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

from trace_analysis.record.lttng import Lttng
from trace_analysis.architecture.architecture import IGNORE_TOPICS
from trace_analysis.architecture import LttngArchitectureImporter


class TestLttngArchitectureImporter:
    def test_create_node(self):
        container = Lttng("test/lttng_samples/talker_listener")
        importer = LttngArchitectureImporter(None)

        node_name = "/listener"
        node = importer._create_node(node_name, container, ignore_topics=IGNORE_TOPICS)
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 0
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks) == 1

        node_name = "/listener"
        node = importer._create_node(node_name, container, ignore_topics=[])
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 2
        assert len(node.unlinked_publishes) == 2
        assert len(node.callbacks) == 2

        node_name = "/talker"
        node = importer._create_node(node_name, container, ignore_topics=IGNORE_TOPICS)
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 1
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks) == 1

        node_name = "/talker"
        node = importer._create_node(node_name, container, ignore_topics=[])
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 3
        assert len(node.unlinked_publishes) == 3
        assert len(node.callbacks) == 2

    def test_exec(self):
        importer = LttngArchitectureImporter(None)
        importer.exec("test/lttng_samples/talker_listener", ignore_topics=IGNORE_TOPICS)
        assert len(importer.nodes) == 2
        assert len(importer.path_aliases) == 0
        assert len(importer.communications) == 1
        assert len(importer.variable_passings) == 0
