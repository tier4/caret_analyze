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

        mocker.patch.object(loaded_mock, 'paths', ())
        mocker.patch.object(loaded_mock, 'nodes', (node_mock,))
        mocker.patch.object(loaded_mock, 'communications', ())

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

    def test_rename_function(self, mocker):
        # define test case
        architecture_text = """
named_paths:
- path_name: target_path_0
  node_chain:
  - node_name: /talker
    publish_topic_name: /topic_0
  - node_name: /listener
    subscribe_topic_name: /topic_1
- path_name: target_path_1
  node_chain:
  - node_name: /talker
    publish_topic_name: /topic_0
  - node_name: /listener
    subscribe_topic_name: /topic_1
executors:
- executor_type: single_threaded_executor
  executor_name: executor_0
  callback_group_names:
  - /talker/callback_group_0
  - /listener/callback_group_0
- executor_type: single_threaded_executor
  executor_name: executor_1
  callback_group_names:
  - /talker/callback_group_1
  - /listener/callback_group_1
nodes:
- node_name: /node_0
- node_name: /node_1
  callbacks:
  - callback_name: timer_callback_0
    callback_type: timer_callback
    period_ns: 1
    symbol: timer_symbol
  - callback_name: subscription_callback_0
    callback_type: subscription_callback
    topic_name: /topic_0
    symbol: sub_symbol
  publishes:
  - topic_name: /topic_0
    callback_names:
    - timer_callback_0
  subscribes:
  - topic_name: /topic_1
    callback_name: subscription_callback_0
        """

        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        # test rename_node()
        node_names = arch.node_names
        expect_node_names = ['/node_0', '/node_1']
        assert set(node_names) == set(expect_node_names)

        arch.rename_node('/node_1', '/changed_node')

        node_names = arch.node_names
        expect_node_names = ['/node_0', '/changed_node']
        assert set(node_names) == set(expect_node_names)

        # test rename_executor()
        executor_names = arch.executor_names
        expect_executor_names = ['executor_0', 'executor_1']
        assert set(executor_names) == set(expect_executor_names)

        arch.rename_executor('executor_1', 'changed_executor')

        executor_names = arch.executor_names
        expect_executor_names = ['executor_0', 'changed_executor']
        assert set(executor_names) == set(expect_executor_names)

        """
        # test rename_topic()
        topic_names = arch.topic_names
        expect_topic_names = ['/topic_0', '/topic_1']
        # assert set(topic_names) == set(expect_topic_names)

        arch.rename_topic('/topic_1', '/changed_topic')

        topic_names = arch.topic_names
        expect_topic_names = ['/topic_0', '/changed_topic']
        assert set(topic_names) == set(expect_topic_names)

        # test rename_callback()
        callback_names = [c.callback_name for c in arch.callbacks]
        expect_callback_names = ['/callback_0', '/callback_1']
        assert set(callback_names) == set(expect_callback_names)

        arch.rename_callback('/callback_1', '/changed_callback')

        callback_names = [c.callback_name for c in arch.callbacks]
        expect_callback_names = ['/callback_0', '/changed_callback']
        assert set(callback_names) == set(expect_callback_names)

        # test rename_topic()
        path_names = arch.path_names
        expect_path_names = ['target_path_0', 'target_path_1']
        assert set(path_names) == set(expect_path_names)

        arch.rename_path('target_path_1', '/changed_path')

        path_names = arch.path_names
        expect_path_names = ['target_path_0', '/changed_path']
        assert set(path_names) == set(expect_path_names)
        """
