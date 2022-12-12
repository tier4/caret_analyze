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


from logging import WARNING

from string import Template

from caret_analyze.architecture import Architecture
from caret_analyze.architecture.architecture_loaded import ArchitectureLoaded
from caret_analyze.architecture.architecture_reader_factory import \
    ArchitectureReaderFactory
from caret_analyze.architecture.graph_search import NodePathSearcher
from caret_analyze.architecture.reader_interface import ArchitectureReader
from caret_analyze.architecture.struct import (CommunicationStruct,
                                               ExecutorStruct, NodePathStruct,
                                               NodeStruct, PathStruct,
                                               TimerCallbackStruct)
from caret_analyze.exceptions import InvalidArgumentError, ItemNotFoundError
from caret_analyze.value_objects import (CommunicationStructValue, NodePathStructValue,
                                         NodeStructValue, PathStructValue)

import pytest


class TestArchitecture:

    def test_empty_architecture(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        mocker.patch.object(loaded_mock, 'nodes', [])
        mocker.patch.object(loaded_mock, 'paths', [])
        mocker.patch.object(loaded_mock, 'communications', [])
        mocker.patch.object(loaded_mock, 'executors', [])

        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        mocker.patch.object(loaded_mock, 'nodes', [])
        mocker.patch.object(loaded_mock, 'communications', [])
        mocker.patch.object(loaded_mock, 'executors', [])
        mocker.patch.object(loaded_mock, 'paths', [])
        arch = Architecture('file_type', 'file_path')

        assert len(arch.nodes) == 0
        assert len(arch.executors) == 0
        assert len(arch.paths) == 0
        assert len(arch.communications) == 0

    def test_get_node(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        node_mock = mocker.Mock(spec=NodeStruct)
        node_struct_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_struct_mock, 'node_name', 'node_name')
        mocker.patch.object(node_mock, 'to_value', return_value=node_struct_mock)
        mocker.patch.object(node_mock, 'callbacks', [])

        mocker.patch.object(loaded_mock, 'paths', [])
        mocker.patch.object(loaded_mock, 'nodes', [node_mock])
        mocker.patch.object(loaded_mock, 'communications', [])
        mocker.patch.object(loaded_mock, 'executors', [])

        searcher_mock = mocker.Mock(spec=NodePathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.NodePathSearcher',
                     return_value=searcher_mock)

        arch = Architecture('file_type', 'file_path')
        node = arch.get_node('node_name')
        assert node == node_mock.to_value()

        with pytest.raises(ItemNotFoundError):
            arch.get_node('node_not_exist')

    def test_full_architecture(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        node_mock = mocker.Mock(spec=NodeStruct)
        executor_mock = mocker.Mock(spec=ExecutorStruct)
        path_mock = mocker.Mock(spec=PathStruct)
        comm_mock = mocker.Mock(spec=CommunicationStruct)

        mocker.patch.object(node_mock, 'callbacks', [])
        mocker.patch.object(loaded_mock, 'nodes', [node_mock])
        mocker.patch.object(loaded_mock, 'paths', [path_mock])
        mocker.patch.object(loaded_mock, 'communications', [comm_mock])
        mocker.patch.object(loaded_mock, 'executors', [executor_mock])
        mocker.patch.object(path_mock, 'path_name', 'path')

        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        searcher_mock = mocker.Mock(spec=NodePathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.NodePathSearcher',
                     return_value=searcher_mock)

        arch = Architecture('file_type', 'file_path')

        assert len(arch.nodes) == 1
        assert arch.nodes[0] == node_mock.to_value()

        assert len(arch.executors) == 1
        assert arch.executors[0] == executor_mock.to_value()

        assert len(arch.paths) == 1
        assert arch.paths[0] == path_mock.to_value()

        assert len(arch.communications) == 1
        assert arch.communications[0] == comm_mock.to_value()

    def test_path(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        path = PathStruct('path0', ())

        mocker.patch.object(loaded_mock, 'nodes', [])
        mocker.patch.object(loaded_mock, 'paths', [path])
        mocker.patch.object(loaded_mock, 'communications', [])
        mocker.patch.object(loaded_mock, 'executors', [])

        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        arch = Architecture('file_type', 'file_path')

        # remove
        assert len(arch.paths) == 1
        arch.remove_path('path0')
        assert len(arch.paths) == 0
        with pytest.raises(InvalidArgumentError):
            arch.remove_path('path0')

        # add
        arch.add_path('path1', path)
        assert len(arch.paths) == 1
        with pytest.raises(InvalidArgumentError):
            arch.add_path('path1', path)

        # get
        with pytest.raises(InvalidArgumentError):
            arch.get_path('path0')
        path_ = arch.get_path('path1')

        # update
        arch.update_path('path2', path_)
        arch.get_path('path2')
        with pytest.raises(InvalidArgumentError):
            arch.update_path('path2', path_)

    def test_search_paths(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        start_node_mock = mocker.Mock(spec=NodeStruct)
        end_node_mock = mocker.Mock(spec=NodeStruct)

        mocker.patch.object(start_node_mock, 'node_name', 'start_node')
        mocker.patch.object(end_node_mock, 'node_name', 'end_node')

        mocker.patch.object(start_node_mock, 'callbacks', [])
        mocker.patch.object(end_node_mock, 'callbacks', [])
        mocker.patch.object(loaded_mock, 'nodes', [start_node_mock, end_node_mock])
        mocker.patch.object(loaded_mock, 'paths', [])
        mocker.patch.object(loaded_mock, 'communications', [])
        mocker.patch.object(loaded_mock, 'executors', [])

        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)
        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        searcher_mock = mocker.Mock(spec=NodePathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.NodePathSearcher',
                     return_value=searcher_mock)
        path_mock = mocker.Mock(spec=PathStruct)
        path_struct_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_mock, 'to_value', return_value=path_struct_mock)
        mocker.patch.object(searcher_mock, 'search', return_value=[path_mock])

        arch = Architecture('file_type', 'file_path')

        with pytest.raises(ItemNotFoundError):
            arch.search_paths('not_exist', 'not_exist')

        path = arch.search_paths('start_node', 'end_node')
        assert path == [path_mock.to_value()]

    def test_search_paths_three_nodes(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        node_mock_0 = mocker.Mock(spec=NodeStruct)
        node_mock_1 = mocker.Mock(spec=NodeStruct)
        node_mock_2 = mocker.Mock(spec=NodeStruct)

        mocker.patch.object(node_mock_0, 'node_name', '0')
        mocker.patch.object(node_mock_1, 'node_name', '1')
        mocker.patch.object(node_mock_2, 'node_name', '2')

        mocker.patch.object(node_mock_0, 'callbacks', [])
        mocker.patch.object(node_mock_1, 'callbacks', [])
        mocker.patch.object(node_mock_2, 'callbacks', [])

        node_path_mock_0 = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_path_mock_0, 'subscribe_topic_name', None)
        mocker.patch.object(node_path_mock_0, 'node_name', '0')
        mocker.patch.object(node_path_mock_0, 'publish_topic_name', '0->1')
        node_path_struct_mock_0 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_0, 'to_value', return_value=node_path_struct_mock_0)
        node_path_mock_1 = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_path_mock_1, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_1, 'node_name', '1')
        mocker.patch.object(node_path_mock_1, 'publish_topic_name', '1->2')
        node_path_struct_mock_1 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_1, 'to_value', return_value=node_path_struct_mock_1)
        node_path_mock_2 = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_path_mock_2, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_2, 'node_name', '2')
        mocker.patch.object(node_path_mock_2, 'publish_topic_name', None)
        node_path_struct_mock_2 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_2, 'to_value', return_value=node_path_struct_mock_2)

        mocker.patch.object(node_mock_0, 'paths', [node_path_mock_0])
        mocker.patch.object(node_mock_1, 'paths', [node_path_mock_1])
        mocker.patch.object(node_mock_2, 'paths', [node_path_mock_2])

        comm_mock_0 = mocker.Mock(spec=CommunicationStruct)
        comm_mock_1 = mocker.Mock(spec=CommunicationStruct)

        mocker.patch.object(comm_mock_0, 'publish_node_name', '0')
        mocker.patch.object(comm_mock_0, 'subscribe_node_name', '1')
        mocker.patch.object(comm_mock_0, 'topic_name', '0->1')
        comm_mock_struct_0 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_0, 'to_value', return_value=comm_mock_struct_0)

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')
        comm_mock_struct_1 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_1, 'to_value', return_value=comm_mock_struct_1)

        mocker.patch.object(loaded_mock, 'nodes', [node_mock_0, node_mock_1, node_mock_2])
        mocker.patch.object(loaded_mock, 'paths', [])

        mocker.patch.object(loaded_mock, 'communications', [comm_mock_0, comm_mock_1])
        mocker.patch.object(loaded_mock, 'executors', [])

        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)
        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        arch = Architecture('file_type', 'file_path')

        paths = arch.search_paths('0', '2')
        assert len(paths) == 1

        paths = arch.search_paths('0', '1', '2')
        assert len(paths) == 1

    def test_verify_callback_uniqueness(self, mocker, caplog):
        node_mock = mocker.Mock(spec=NodeStruct)
        callback_mock = mocker.Mock(spec=TimerCallbackStruct)
        mocker.patch.object(callback_mock, 'period_ns', 100)
        mocker.patch.object(
            callback_mock, 'callback_type_name', 'type')

        caplog.set_level(WARNING)

        mocker.patch.object(node_mock, 'callbacks', [callback_mock])
        caplog.clear()
        Architecture._verify([node_mock])
        assert len(caplog.record_tuples) == 0

        mocker.patch.object(node_mock, 'callbacks', [callback_mock, callback_mock])
        caplog.clear()
        Architecture._verify([node_mock])
        assert len(caplog.record_tuples) == 1

        mocker.patch.object(node_mock, 'callbacks', [callback_mock, callback_mock, callback_mock])
        caplog.clear()
        Architecture._verify([node_mock])
        assert len(caplog.record_tuples) == 1

    template_architecture_assign = Template("""
named_paths: []
executors:
- executor_type: single_threaded_executor
  executor_name: executor_0
  callback_group_names:
  - /callback_group_0
nodes:
- node_name: /pong_node
  callback_groups:
  - callback_group_type: mutually_exclusive
    callback_group_name: /callback_group_0
    callback_names:
    - subscription_callback_0
    - timer_callback_1
  callbacks:
    - callback_name: subscription_callback_0
      callback_type: subscription_callback
      topic_name: /pong
      symbol: sub_symbol
    - callback_name: timer_callback_1
      callback_type: timer_callback
      period_ns: 1
      symbol: timer_symbol
$passings
  subscribes:
    - topic_name: /pong
      callback_name: subscription_callback_0
$publishes
$contexts
""")

    passings_text = """
  variable_passings:
    - callback_name_write: subscription_callback_0
      callback_name_read: timer_callback_1
"""

    publishes_text = """
  publishes:
    - topic_name: /ping
      callback_names:
        - timer_callback_1
"""

    contexts_text = """
  message_contexts:
    - context_type: callback_chain
      subscription_topic_name: /pong
      publisher_topic_name: /ping
"""

    def test_assign_publisher(self, mocker):
        # assign publisher to template
        architecture_text = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes='',
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_publisher('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes=self.publishes_text,
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # assign publisher to be full architecture
        architecture_text = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes='',
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_publisher('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes=self.publishes_text,
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid assign
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_publisher('/not_exist_node', '/ping', 'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_publisher('/not_exist_node', '/ping', 'not_exist_callback_1')

    def test_assign_passings(self, mocker):
        # assign passing to template
        architecture_text = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes='',
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_message_passings('/pong_node', 'timer_callback_1', 'subscription_callback_0')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes='',
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # assign passing to be full architecture
        architecture_text = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes=self.publishes_text,
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_message_passings('/pong_node', 'timer_callback_1', 'subscription_callback_0')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes=self.publishes_text,
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid assign
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_passings('/not_exist_node', 'timer_callback_1', 'subscription_callback_0')
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_passings('/pong_node', 'not_exist_callback_1', 'subscription_callback_0')
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_passings('/pong_node', 'timer_callback_1', 'not_exist_callback_0')

    def test_assign_message_contexts(self, mocker):
        # assign message context to template
        architecture_text = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes=self.publishes_text,
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_message_context('/pong_node', 'callback_chain', '/pong', '/ping')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings='',
                                                         publishes=self.publishes_text,
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # assign message context to be full architecture
        architecture_text = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes=self.publishes_text,
                                                         contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.assign_message_context('/pong_node', 'callback_chain', '/pong', '/ping')

        architecture_text_expected = \
            self.template_architecture_assign.substitute(passings=self.passings_text,
                                                         publishes=self.publishes_text,
                                                         contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid assign
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_context('/not_exist_node', 'callback_chain', '/pong', '/ping')
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_context('/pong_node', 'callback_chain', '/not_exist_topic', '/ping')
        with pytest.raises(ItemNotFoundError):
            arch_expected.assign_message_context('/pong_node', 'callback_chain', '/pong', '/not_exist_topic')

    # define template text of rename function
    template_architecture_rename = Template("""
named_paths:
- path_name: target_path_0
  node_chain:
  - node_name: /node_0
    publish_topic_name: /topic_0
  - node_name: $node
    subscribe_topic_name: /topic_0
- path_name: $path
  node_chain:
  - node_name: $node
    publish_topic_name: $topic
  - node_name: /node_0
    subscribe_topic_name: $topic
executors:
- executor_type: single_threaded_executor
  executor_name: executor_0
  callback_group_names:
  - /callback_group_0
- executor_type: single_threaded_executor
  executor_name: $executor
  callback_group_names:
  - /callback_group_1
nodes:
- node_name: /node_0
  callback_groups:
  - callback_group_type: mutually_exclusive
    callback_group_name: /callback_group_1
    callback_names:
    - /callback_0
    - $callback_1
  callbacks:
  - callback_name: /callback_0
    callback_type: timer_callback
    period_ns: 200000000
    symbol: timer
  - callback_name: $callback_1
    callback_type: subscription_callback
    topic_name: $topic
    symbol: sub
  publishes:
  - topic_name: /topic_0
    callback_names:
    - /callback_0
  subscribes:
  - topic_name: $topic
    callback_name: $callback_1
- node_name: $node
  callback_groups:
  - callback_group_type: mutually_exclusive
    callback_group_name: /callback_group_0
    callback_names:
    - $callback_2
    - /callback_3
  callbacks:
  - callback_name: $callback_2
    callback_type: timer_callback
    period_ns: 200000000
    symbol: timer
  - callback_name: /callback_3
    callback_type: subscription_callback
    topic_name: /topic_0
    symbol: sub
  publishes:
  - topic_name: $topic
    callback_names:
    - $callback_2
  subscribes:
  - topic_name: /topic_0
    callback_name: /callback_3""")

    def test_rename_node(self, mocker):
        architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.get_node('/node_0')
        arch.get_node('/node_1')
        with pytest.raises(ItemNotFoundError):
            arch.get_node('/changed_node')

        arch.rename_node('/node_1', '/changed_node')

        arch.get_node('/node_0')
        arch.get_node('/changed_node')
        with pytest.raises(ItemNotFoundError):
            arch.get_node('/node_1')

        # define node renamed text
        renamed_architecture_text = \
            self.template_architecture_rename.substitute(node='/changed_node',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=renamed_architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.paths) == set(arch_expected.paths)
        assert set(arch.executors) == set(arch_expected.executors)

    def test_rename_executor(self, mocker):
        architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.get_executor('executor_0')
        arch.get_executor('executor_1')
        with pytest.raises(ItemNotFoundError):
            arch.get_executor('changed_executor')

        arch.rename_executor('executor_1', 'changed_executor')

        arch.get_executor('executor_0')
        arch.get_executor('changed_executor')
        with pytest.raises(ItemNotFoundError):
            arch.get_executor('executor_1')

        # define executor renamed text
        renamed_architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='changed_executor',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=renamed_architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.paths) == set(arch_expected.paths)
        assert set(arch.executors) == set(arch_expected.executors)

    def test_rename_topic(self, mocker):
        architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.rename_topic('/topic_1', '/changed_topic')

        # define topic renamed text
        renamed_architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/changed_topic',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=renamed_architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.paths) == set(arch_expected.paths)
        assert set(arch.executors) == set(arch_expected.executors)

    def test_rename_callback(self, mocker):
        architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.get_callback('/callback_0')
        arch.get_callback('/callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.get_callback('/changed_callback_0')

        arch.rename_callback('/callback_1', '/changed_callback_0')
        arch.rename_callback('/callback_2', '/changed_callback_1')

        arch.get_callback('/callback_0')
        arch.get_callback('/changed_callback_0')
        with pytest.raises(ItemNotFoundError):
            arch.get_callback('/callback_1')

        # define callback renamed text
        renamed_architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/changed_callback_0',
                                                         callback_2='/changed_callback_1')
        mocker.patch('builtins.open', mocker.mock_open(read_data=renamed_architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.paths) == set(arch_expected.paths)
        assert set(arch.executors) == set(arch_expected.executors)

    def test_rename_path(self, mocker):
        architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='target_path_1',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.get_path('target_path_0')
        arch.get_path('target_path_1')
        with pytest.raises(InvalidArgumentError):
            arch.get_path('changed_path')

        arch.rename_path('target_path_1', 'changed_path')
        arch.get_path('target_path_0')
        arch.get_path('changed_path')
        with pytest.raises(InvalidArgumentError):
            arch.get_path('target_path_1')

        # define path renamed text
        renamed_architecture_text = \
            self.template_architecture_rename.substitute(node='/node_1',
                                                         executor='executor_1',
                                                         topic='/topic_1',
                                                         path='changed_path',
                                                         callback_1='/callback_1',
                                                         callback_2='/callback_2')
        mocker.patch('builtins.open', mocker.mock_open(read_data=renamed_architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.paths) == set(arch_expected.paths)
        assert set(arch.executors) == set(arch_expected.executors)
