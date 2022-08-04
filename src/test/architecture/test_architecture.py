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

from typing import Optional, Tuple

from caret_analyze.architecture import Architecture
from caret_analyze.architecture.architecture_loaded import ArchitectureLoaded
from caret_analyze.architecture.architecture_reader_factory import \
    ArchitectureReaderFactory
from caret_analyze.architecture.graph_search import NodePathSearcher
from caret_analyze.architecture.reader_interface import ArchitectureReader
from caret_analyze.exceptions import InvalidArgumentError, ItemNotFoundError
from caret_analyze.value_objects import (CommunicationStructValue,
                                         ExecutorStructValue, NodePathStructValue,
                                         NodeStructValue, PathStructValue,
                                         PublisherStructValue, SubscriptionStructValue,
                                         TimerCallbackStructValue)

import pytest


@pytest.fixture
def create_publisher():
    def _create_publisher(node_name: str, topic_name: str):
        pub = PublisherStructValue(node_name, topic_name, None)
        return pub
    return _create_publisher


@pytest.fixture
def create_subscription():
    def _create_subscription(node_name: str, topic_name: str):
        sub = SubscriptionStructValue(node_name, topic_name, None)
        return sub
    return _create_subscription


@pytest.fixture
def create_node_path(create_publisher, create_subscription):
    def _create_node_path(
        node_name: str,
        sub_topic_name: Optional[str],
        pub_topic_name: Optional[str],
    ):
        sub = None
        if sub_topic_name is not None:
            sub = create_subscription(node_name, sub_topic_name)
        pub = None
        if pub_topic_name is not None:
            pub = create_publisher(node_name, pub_topic_name)

        node_path = NodePathStructValue(
            node_name, sub, pub, None, None
        )
        return node_path
    return _create_node_path


@pytest.fixture
def create_arch(mocker):
    def _create_arch(node_paths, comms):
        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'paths', node_paths)
        mocker.patch.object(node_mock, 'callbacks', [])
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)
        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)
        mocker.patch.object(loaded_mock, 'nodes', [])
        mocker.patch.object(loaded_mock, 'communications', comms)
        mocker.patch.object(loaded_mock, 'executors', [])
        mocker.patch.object(loaded_mock, 'paths', [])
        mocker.patch.object(loaded_mock, 'nodes', [node_mock])
        arch = Architecture('file_type', 'file_path')
        return arch
    return _create_arch


@pytest.fixture
def create_node(create_publisher, create_subscription):
    def _create_node(
        node_name: str,
        sub_topic_name: Optional[str],
        pub_topic_name: Optional[str]
    ) -> NodeStructValue:
        pub: Tuple[PublisherStructValue, ...]
        sub: Tuple[SubscriptionStructValue, ...]

        if pub_topic_name:
            pub = (create_publisher(node_name, pub_topic_name),)
        else:
            pub = ()
        if sub_topic_name:
            sub = (create_subscription(node_name, sub_topic_name),)
        else:
            sub = ()

        node = NodeStructValue(
            node_name, pub, sub, (), (), None, None
        )
        return node
    return _create_node


@pytest.fixture
def create_comm(create_node):
    def _create_comm(
        topic_name: str,
        pub_node_name: str,
        sub_node_name: str
    ):
        node_sub = create_node(sub_node_name, topic_name, None)
        node_pub = create_node(pub_node_name, None, topic_name)
        comm = CommunicationStructValue(
            node_pub, node_sub,
            node_pub.publishers[0], node_sub.subscriptions[0],
            None, None
        )
        return comm
    return _create_comm


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

        node_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock, 'node_name', 'node_name')
        mocker.patch.object(node_mock, 'callbacks', [])

        mocker.patch.object(loaded_mock, 'paths', ())
        mocker.patch.object(loaded_mock, 'nodes', (node_mock,))
        mocker.patch.object(loaded_mock, 'communications', ())

        searcher_mock = mocker.Mock(spec=NodePathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.NodePathSearcher',
                     return_value=searcher_mock)

        arch = Architecture('file_type', 'file_path')
        node = arch.get_node('node_name')
        assert node == node_mock

        with pytest.raises(ItemNotFoundError):
            arch.get_node('node_not_exist')

    def test_full_architecture(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        node_mock = mocker.Mock(spec=NodeStructValue)
        executor_mock = mocker.Mock(spec=ExecutorStructValue)
        path_mock = mocker.Mock(spec=PathStructValue)
        comm_mock = mocker.Mock(spec=CommunicationStructValue)

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
        assert arch.nodes[0] == node_mock

        assert len(arch.executors) == 1
        assert arch.executors[0] == executor_mock

        assert len(arch.paths) == 1
        assert arch.paths[0] == path_mock

        assert len(arch.communications) == 1
        assert arch.communications[0] == comm_mock

    def test_path(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        path = PathStructValue('path0', ())

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

        start_node_mock = mocker.Mock(spec=NodeStructValue)
        end_node_mock = mocker.Mock(spec=NodeStructValue)

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
        path_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(searcher_mock, 'search', return_value=[path_mock])

        arch = Architecture('file_type', 'file_path')

        with pytest.raises(ItemNotFoundError):
            arch.search_paths('not_exist', 'not_exist')

        path = arch.search_paths('start_node', 'end_node')
        assert path == [path_mock]

    def test_search_paths_three_nodes(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)

        node_mock_0 = mocker.Mock(spec=NodeStructValue)
        node_mock_1 = mocker.Mock(spec=NodeStructValue)
        node_mock_2 = mocker.Mock(spec=NodeStructValue)

        mocker.patch.object(node_mock_0, 'node_name', '0')
        mocker.patch.object(node_mock_1, 'node_name', '1')
        mocker.patch.object(node_mock_2, 'node_name', '2')

        mocker.patch.object(node_mock_0, 'callbacks', [])
        mocker.patch.object(node_mock_1, 'callbacks', [])
        mocker.patch.object(node_mock_2, 'callbacks', [])

        node_path_mock_0 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_0, 'subscribe_topic_name', None)
        mocker.patch.object(node_path_mock_0, 'node_name', '0')
        mocker.patch.object(node_path_mock_0, 'publish_topic_name', '0->1')
        node_path_mock_1 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_1, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_1, 'node_name', '1')
        mocker.patch.object(node_path_mock_1, 'publish_topic_name', '1->2')
        node_path_mock_2 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_2, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_2, 'node_name', '2')
        mocker.patch.object(node_path_mock_2, 'publish_topic_name', None)

        mocker.patch.object(node_mock_0, 'paths', [node_path_mock_0])
        mocker.patch.object(node_mock_1, 'paths', [node_path_mock_1])
        mocker.patch.object(node_mock_2, 'paths', [node_path_mock_2])

        comm_mock_0 = mocker.Mock(spec=CommunicationStructValue)
        comm_mock_1 = mocker.Mock(spec=CommunicationStructValue)

        mocker.patch.object(comm_mock_0, 'publish_node_name', '0')
        mocker.patch.object(comm_mock_0, 'subscribe_node_name', '1')
        mocker.patch.object(comm_mock_0, 'topic_name', '0->1')

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')

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

    def test_combine_path(
        self,
        mocker,
        create_node_path,
        create_comm,
        create_arch,
    ):
        arch = create_arch()
        # combine [] + []
        with pytest.raises(InvalidArgumentError):
            path_left = PathStructValue(None, ())
            path_right = PathStructValue(None, ())
            arch.combine_path(path_left, path_right)

        # combine [node] + [comm]
        node_path = create_node_path('pub_node', None, 'topic')
        path_left = PathStructValue(None, [node_path])

        comm = create_comm('topic', 'pub_node', 'sub_node')
        path_right = PathStructValue(None, [comm])

        arch = create_arch([node_path], [comm])

        path = arch.combine_path(path_left, path_right)
        path_expect = PathStructValue(None, (node_path, comm))
        assert path == path_expect

        with pytest.raises(InvalidArgumentError):
            arch.combine_path(path_left, path_right)

        # combine [comm] + [node]
        comm = create_comm('topic', 'pub_node', 'sub_node')
        path_right = PathStructValue(None, [comm])

        node_path = create_node_path('sub_node', 'topic', None)
        path_left = PathStructValue(None, [node_path])

        path = arch.combine_path(path_left, path_right)
        path_expect = PathStructValue(None, (comm, node_path))
        assert path == path_expect

        with pytest.raises(InvalidArgumentError):
            arch.combine_path(path_right, path_left)

        # combine [node, comm, node] + [node, comm, node]
        node_0 = create_node_path('node_0', None, 'topic_0')
        node_1 = create_node_path('node_1', 'topic_0', 'topic_1')
        node_2 = create_node_path('node_2', 'topic_1', None)

        node_1_left = create_node_path('node_1', 'topic_0', None)
        node_1_right = create_node_path('node_1', None, 'topic_1')

        comm_0 = create_comm('topic_0', 'node_0', 'node_1')
        comm_1 = create_comm('topic_1', 'node_1', 'node_2')

        path_left = PathStructValue(
            None, (node_0, comm_0, node_1_left))
        path_right = PathStructValue(
            None, (node_1_right, comm_1, node_2))

        arch = create_arch(
            [node_0, node_1, node_1_left, node_1_right, node_2],
            [comm_0, comm_1]
        )
        path = arch.combine_path(path_left, path_right)
        path_expect = PathStructValue(
            None, (node_0, comm_0, node_1, comm_1, node_2))
        assert path == path_expect

    def test_verify_callback_uniqueness(self, mocker, caplog):
        node_mock = mocker.Mock(spec=NodeStructValue)
        callback_mock = mocker.Mock(spec=TimerCallbackStructValue)
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
