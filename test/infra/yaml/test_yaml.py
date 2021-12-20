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

from caret_analyze.exceptions import InvalidYamlFormatError
from caret_analyze.infra.yaml.architecture_reader_yaml import \
    ArchitectureReaderYaml
from caret_analyze.value_objects import (CallbackGroupType, CallbackType,
                                         ExecutorType, InheritUniqueStamp,
                                         SubscriptionCallbackValue,
                                         TimerCallbackValue)
from caret_analyze.value_objects.node import NodeValue

import pytest
from pytest_mock import MockerFixture


class TestArchitectureReaderYaml:

    def test_empty_yaml(self, mocker: MockerFixture):
        mocker.patch('builtins.open', mocker.mock_open(read_data=''))
        node = NodeValue('node_name', None)

        with pytest.raises(InvalidYamlFormatError):
            reader = ArchitectureReaderYaml('file_name')

        mocker.patch('builtins.open', mocker.mock_open(read_data='x'))
        reader = ArchitectureReaderYaml('file_name')

        with pytest.raises(InvalidYamlFormatError):
            reader.get_nodes()

        with pytest.raises(InvalidYamlFormatError):
            reader.get_timer_callbacks(node)

        with pytest.raises(InvalidYamlFormatError):
            reader.get_subscription_callbacks(node)

        with pytest.raises(InvalidYamlFormatError):
            reader.get_publishers(node)

        with pytest.raises(InvalidYamlFormatError):
            reader.get_subscriptions(node)

        with pytest.raises(InvalidYamlFormatError):
            reader.get_paths()

        with pytest.raises(InvalidYamlFormatError):
            reader.get_variable_passings(node)

        with pytest.raises(InvalidYamlFormatError):
            reader.get_executors()

        with pytest.raises(InvalidYamlFormatError):
            reader.get_callback_groups(node)

    def test_get_named_paths(self, mocker: MockerFixture):
        architecture_text = """
named_paths:
- path_name: target_path
  node_chain:
  - node_name: /talker
    publish_topic_name: /chatter
  - node_name: /listener
    subscribe_topic_name: /chatter
executors: []
nodes: []
        """
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        paths_info = reader.get_paths()

        assert len(paths_info) == 1
        path_info = paths_info[0]
        assert path_info.path_name == 'target_path'
        assert len(path_info.node_path_values) == 2

        talker_path = path_info.node_path_values[0]
        assert talker_path.node_name == '/talker'
        assert talker_path.publish_topic_name == '/chatter'
        assert talker_path.subscribe_topic_name is None

        listener_path = path_info.node_path_values[1]
        assert listener_path.node_name == '/listener'
        assert listener_path.publish_topic_name is None
        assert listener_path.subscribe_topic_name == '/chatter'

    def test_executors(self, mocker: MockerFixture):
        architecture_text = """
named_paths: []
executors:
- executor_type: single_threaded_executor
  executor_name: executor_0
  callback_group_names:
  - /talker/callback_group_0
  - /listener/callback_group_0
nodes: []
        """
        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        executors = reader.get_executors()
        assert len(executors) == 1
        executor = executors[0]
        assert executor.executor_name == 'executor_0'
        assert executor.executor_type == ExecutorType.SINGLE_THREADED_EXECUTOR
        assert executor.callback_group_ids == (
            '/talker/callback_group_0',
            '/listener/callback_group_0',
        )

    def test_get_callback_groups(self, mocker: MockerFixture):
        architecture_text = """
named_paths: []
executors: []
nodes:
- node_name: /listener
  callback_groups:
  - callback_group_type: reentrant
    callback_group_name: callback_group_0
    callback_names:
    - /listener/timer_callback_0
- node_name: /talker
  callback_groups:
  - callback_group_type: mutually_exclusive
    callback_group_name: callback_group_1
    callback_names:
    - /talker/timer_callback_1
        """
        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        cbgs = reader.get_callback_groups(NodeValue('/listener', None))
        assert len(cbgs) == 1
        cbg = cbgs[0]
        assert cbg.node_name == '/listener'
        assert cbg.callback_group_type == CallbackGroupType.REENTRANT
        assert cbg.callback_ids == ('/listener/timer_callback_0',)

        cbgs = reader.get_callback_groups(NodeValue('/talker', None))
        assert len(cbgs) == 1
        cbg = cbgs[0]
        assert cbg.node_name == '/talker'
        assert cbg.callback_group_type == CallbackGroupType.MUTUALLY_EXCLUSIVE
        assert cbg.callback_ids == ('/talker/timer_callback_1',)

    def test_node_callback(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /node
  callbacks:
  - callback_name: timer_callback_0
    callback_type: timer_callback
    period_ns: 1
    symbol: timer_symbol
  - callback_name: subscription_callback_0
    callback_type: subscription_callback
    topic_name: /chatter
    symbol: sub_symbol
  publishes:
  - topic_name: /chatter
    callback_names:
    - timer_callback_0
  subscribes:
  - topic_name: /chatter
    callback_name: subscription_callback_0
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        timer_cbs = reader.get_timer_callbacks(NodeValue('/node', None))
        assert len(timer_cbs) == 1
        timer_cb = timer_cbs[0]
        assert isinstance(timer_cb, TimerCallbackValue)
        assert timer_cb.callback_type == CallbackType.TIMER
        assert timer_cb.symbol == 'timer_symbol'
        assert timer_cb.node_id == '/node'
        assert timer_cb.subscribe_topic_name is None
        assert timer_cb.publish_topic_names == ('/chatter',)
        assert timer_cb.period_ns == 1

        sub_cbs = reader.get_subscription_callbacks(NodeValue('/node', None))
        assert len(sub_cbs) == 1
        sub_cb = sub_cbs[0]
        assert isinstance(sub_cb, SubscriptionCallbackValue)
        assert sub_cb.callback_type == CallbackType.SUBSCRIPTION
        assert sub_cb.symbol == 'sub_symbol'
        assert sub_cb.node_id == '/node'
        assert sub_cb.subscribe_topic_name == '/chatter'

    def test_message_contexts(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /ping_pong
  message_contexts:
  - context_type: inherit_unique_stamp
    publisher_topic_name: /pong
    subscription_topic_name: /ping
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        contexts = reader.get_message_contexts(NodeValue('/ping_pong', None))
        assert len(contexts) == 1

        context = contexts[0]
        assert context['context_type'] == InheritUniqueStamp.TYPE_NAME
        assert context['publisher_topic_name'] == '/pong'
        assert context['subscription_topic_name'] == '/ping'

    def test_publishers_info(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /listener
  publishes:
  - topic_name: /xxx
    callback_names:
    - /listener/timer_callback_0
    - /listener/timer_callback_1
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        timer_pubs = reader.get_publishers(NodeValue('/listener', None))
        assert len(timer_pubs) == 1
        timer_pub = timer_pubs[0]
        assert timer_pub.node_id == '/listener'
        assert timer_pub.topic_name == '/xxx'
        assert timer_pub.callback_ids == (
            '/listener/timer_callback_0',
            '/listener/timer_callback_1')

    def test_subscriptions_info(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /listener
  subscribes:
  - topic_name: /xxx
    callback_name: /listener/timer_callback_0
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        subs = reader.get_subscriptions(NodeValue('/listener', None))
        assert len(subs) == 1
        sub = subs[0]
        assert sub.node_name == '/listener'
        assert sub.topic_name == '/xxx'
        assert sub.callback_id == '/listener/timer_callback_0'

    def test_nodes(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /listener
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        nodes = reader.get_nodes()
        nodes == (NodeValue('/listener', '/listener'),)

    def test_get_variable_passings_info(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases: []
executors: []
nodes:
- node_name: /listener
  variable_passings:
  - callback_name_write: /listener/timer_callback_0
    callback_name_read: /listener/timer_callback_1
- node_name: /talker
        """

        mocker.patch('builtins.open', mocker.mock_open(
            read_data=architecture_text))
        reader = ArchitectureReaderYaml('file_name')

        var_passes_info = reader.get_variable_passings(NodeValue('/listener', None))
        assert len(var_passes_info) == 1
        var_pass_info = var_passes_info[0]
        assert var_pass_info.node_name == '/listener'
        assert var_pass_info.callback_id_write == '/listener/timer_callback_0'
        assert var_pass_info.callback_id_read == '/listener/timer_callback_1'

        assert reader.get_variable_passings(NodeValue('/talker', None)) == []
