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

from caret_analyze.architecture.lttng import ArchitectureLttng
from caret_analyze.trace.lttng import Lttng
from caret_analyze.value_objects.callback_info import (
    SubscriptionCallbackInfo, TimerCallbackInfo)
from caret_analyze.value_objects.publisher import Publisher
from pytest_mock import MockerFixture


class TestArchitectureLttng:

    def test_empty_architecture(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_node_names', return_value=[])
        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(lttng_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[])

        reader = ArchitectureLttng(lttng_mock)

        assert reader.get_node_names() == []
        assert reader.get_timer_callbacks('node') == []
        assert reader.get_subscription_callbacks('node') == []
        assert reader.get_publishers('node') == []
        assert reader.get_variable_passings('node') == []
        assert reader.get_path_aliases() == []

    def test_full_architecture(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch.object(lttng_mock, 'get_node_names', return_value=['node'])

        timer_callback_info = TimerCallbackInfo('node', 'timer_callback', 'symbol_name', 1)
        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[timer_callback_info])

        sub_callback_info = SubscriptionCallbackInfo(
                'node', 'sub_callback', 'sub_symbol', 'topic_name')
        mocker.patch.object(
                lttng_mock, 'get_subscription_callbacks', return_value=[sub_callback_info])

        pub_info = Publisher('node', 'topic_name', None)
        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[pub_info])

        reader = ArchitectureLttng(lttng_mock)

        assert reader.get_node_names() == ['node']
        assert reader.get_timer_callbacks('node') == [timer_callback_info]
        assert reader.get_subscription_callbacks('node') == [sub_callback_info]
        assert reader.get_publishers('node') == [pub_info]
        assert reader.get_variable_passings('node') == []
        assert reader.get_path_aliases() == []

#     def test_create_node(self):
#         container = Lttng('sample/lttng_samples/talker_listener')
#         importer = LttngArchitectureImporter(None)

#         node_name = '/listener'
#         node = importer._create_node(
#             node_name, container, ignore_topics=IGNORE_TOPICS)
#         assert node.node_name == node_name
#         assert len(node.variable_passings) == 0
#         assert len(node.publishes) == 0
#         assert len(node.unlinked_publishes) == 0
#         assert len(node.callbacks) == 1

#         node_name = '/listener'
#         node = importer._create_node(node_name, container, ignore_topics=[])
#         assert node.node_name == node_name
#         assert len(node.variable_passings) == 0
#         assert len(node.publishes) == 2
#         assert len(node.unlinked_publishes) == 2
#         assert len(node.callbacks) == 2

#         node_name = '/talker'
#         node = importer._create_node(
#             node_name, container, ignore_topics=IGNORE_TOPICS)
#         assert node.node_name == node_name
#         assert len(node.variable_passings) == 0
#         assert len(node.publishes) == 1
#         assert len(node.unlinked_publishes) == 0
#         assert len(node.callbacks) == 1

#         node_name = '/talker'
#         node = importer._create_node(node_name, container, ignore_topics=[])
#         assert node.node_name == node_name
#         assert len(node.variable_passings) == 0
#         assert len(node.publishes) == 3
#         assert len(node.unlinked_publishes) == 3
#         assert len(node.callbacks) == 2

#     @pytest.mark.parametrize(
#         'path, nodes, aliases, comms, var_pass',
#         [
#             ('sample/lttng_samples/talker_listener', 2, 0, 1, 0),
#             ('sample/lttng_samples/end_to_end_sample/fastrtps', 6, 0, 5, 0),
#             ('sample/lttng_samples/end_to_end_sample/cyclonedds', 6, 0, 5, 0),
#         ],
#     )
#     def test_execute(self, path, nodes, aliases, comms, var_pass):
#         importer = LttngArchitectureImporter(None)
#         importer.execute(path, ignore_topics=IGNORE_TOPICS)
#         assert len(importer.nodes) == nodes
#         assert len(importer.path_aliases) == aliases
#         assert len(importer.communications) == comms
#         assert len(importer.variable_passings) == var_pass
