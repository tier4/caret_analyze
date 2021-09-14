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

from caret_analyze.architecture.architecture import IGNORE_TOPICS
from caret_analyze.architecture.lttng import LttngArchitectureImporter
from caret_analyze.record.lttng import Lttng

import pytest


class TestLttngArchitectureImporter:

    def test_create_node(self):
        container = Lttng('sample/lttng_samples/talker_listener')
        importer = LttngArchitectureImporter(None)

        node_name = '/listener'
        node = importer._create_node(
            node_name, container, ignore_topics=IGNORE_TOPICS)
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 0
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks) == 1

        node_name = '/listener'
        node = importer._create_node(node_name, container, ignore_topics=[])
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 2
        assert len(node.unlinked_publishes) == 2
        assert len(node.callbacks) == 2

        node_name = '/talker'
        node = importer._create_node(
            node_name, container, ignore_topics=IGNORE_TOPICS)
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 1
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks) == 1

        node_name = '/talker'
        node = importer._create_node(node_name, container, ignore_topics=[])
        assert node.node_name == node_name
        assert len(node.variable_passings) == 0
        assert len(node.publishes) == 3
        assert len(node.unlinked_publishes) == 3
        assert len(node.callbacks) == 2

    @pytest.mark.parametrize(
        'path, nodes, aliases, comms, var_pass',
        [
            ('sample/lttng_samples/talker_listener', 2, 0, 1, 0),
            ('sample/lttng_samples/end_to_end_sample/fastrtps', 6, 0, 5, 0),
        ],
    )
    def test_execute(self, path, nodes, aliases, comms, var_pass):
        importer = LttngArchitectureImporter(None)
        importer.execute(path, ignore_topics=IGNORE_TOPICS)
        assert len(importer.nodes) == nodes
        assert len(importer.path_aliases) == aliases
        assert len(importer.communications) == comms
        assert len(importer.variable_passings) == var_pass
