# Copyright 2021 TIER IV, Inc.
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

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
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
                                               TimerCallbackStruct
                                               )
from caret_analyze.exceptions import InvalidArgumentError, ItemNotFoundError
from caret_analyze.value_objects import (CommunicationStructValue, NodePathStructValue,
                                         NodeStructValue, PathStructValue,
                                         PublisherStructValue, SubscriptionStructValue)

import pytest


@pytest.fixture
def create_publisher():
    def _create_publisher(node_name: str, topic_name: str) -> PublisherStructValue:
        pub = PublisherStructValue(node_name, topic_name, None, 0)
        return pub
    return _create_publisher


@pytest.fixture
def create_subscription():
    def _create_subscription(node_name: str, topic_name: str):
        sub = SubscriptionStructValue(node_name, topic_name, None, 0)
        return sub
    return _create_subscription


@pytest.fixture
def create_node_path(
    create_publisher: Callable[[str, str], PublisherStructValue],
    create_subscription: Callable[[str, str], SubscriptionStructValue]
):
    def _create_node_path(
        node_name: str,
        sub_topic_name: str | None,
        pub_topic_name: str | None,
    ) -> NodePathStructValue:
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
    def _create_arch(
        node_paths: tuple[NodePathStructValue],
        comms: tuple[CommunicationStructValue]
    ) -> Architecture:
        node_dict: defaultdict[str, list[NodePathStructValue]] = defaultdict(list)
        for node_path in node_paths:
            node_dict[node_path.node_name].append(node_path)
        node_list = []  # list[mock]

        for node, paths in node_dict.items():
            node_value_mock = mocker.Mock(spec=NodeStructValue)
            mocker.patch.object(node_value_mock, 'paths', tuple(paths))
            mocker.patch.object(node_value_mock, 'callbacks', [])
            mocker.patch.object(node_value_mock, 'node_name', node)

            node_mock = mocker.Mock(spec=NodeStruct)
            mocker.patch.object(node_mock, 'callbacks', [])
            mocker.patch.object(node_mock, 'node_name', node)
            mocker.patch.object(node_mock, 'to_value', return_value=node_value_mock)

            node_list.append(node_mock)

        comm_list = []
        for comm in comms:
            comm_mock = mocker.Mock(spec=CommunicationStruct)
            mocker.patch.object(comm_mock, 'to_value', return_value=comm)
            comm_list.append(comm_mock)

        reader_mock = mocker.Mock(spec=ArchitectureReader)

        mocker.patch.object(ArchitectureReaderFactory,
                            'create_instance', return_value=reader_mock)

        loaded_mock = mocker.Mock(spec=ArchitectureLoaded)
        mocker.patch.object(loaded_mock, 'communications', comm_list)
        mocker.patch.object(loaded_mock, 'executors', [])
        mocker.patch.object(loaded_mock, 'paths', [])
        mocker.patch.object(loaded_mock, 'nodes', node_list)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ArchitectureLoaded',
                     return_value=loaded_mock)

        arch = Architecture('file_type', 'file_path')
        return arch
    return _create_arch


@pytest.fixture
def create_node(
    create_publisher: Callable[[str, str], PublisherStructValue],
    create_subscription: Callable[[str, str], SubscriptionStructValue]
):
    def _create_node(
        node_name: str,
        sub_topic_name: str | None,
        pub_topic_name: str | None
    ) -> NodeStructValue:
        pubs: tuple[PublisherStructValue, ...]
        subs: tuple[SubscriptionStructValue, ...]

        if pub_topic_name:
            pubs = (create_publisher(node_name, pub_topic_name),)
        else:
            pubs = ()
        if sub_topic_name:
            subs = (create_subscription(node_name, sub_topic_name),)
        else:
            subs = ()

        node = NodeStructValue(
            node_name, pubs, subs, (), (), (), None, None
        )
        return node
    return _create_node


@pytest.fixture
def create_comm(create_node: Callable[[str, str | None, str | None], NodeStructValue]):
    def _create_comm(
        topic_name: str,
        pub_node_name: str,
        sub_node_name: str
    ):
        node_sub: NodeStructValue = create_node(sub_node_name, topic_name, None)
        node_pub: NodeStructValue = create_node(pub_node_name, None, topic_name)
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
        mocker.patch.object(node_path_mock_0, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_0, 'publisher_construction_order', 0)
        node_path_struct_mock_0 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_0, 'to_value', return_value=node_path_struct_mock_0)
        node_path_mock_1 = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_path_mock_1, 'subscribe_topic_name', '0->1')
        mocker.patch.object(node_path_mock_1, 'node_name', '1')
        mocker.patch.object(node_path_mock_1, 'publish_topic_name', '1->2')
        mocker.patch.object(node_path_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_1, 'publisher_construction_order', 0)
        node_path_struct_mock_1 = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path_mock_1, 'to_value', return_value=node_path_struct_mock_1)
        node_path_mock_2 = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(node_path_mock_2, 'subscribe_topic_name', '1->2')
        mocker.patch.object(node_path_mock_2, 'node_name', '2')
        mocker.patch.object(node_path_mock_2, 'publish_topic_name', None)
        mocker.patch.object(node_path_mock_2, 'subscription_construction_order', 0)
        mocker.patch.object(node_path_mock_2, 'publisher_construction_order', 0)
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
        mocker.patch.object(comm_mock_0, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_0, 'publisher_construction_order', 0)
        comm_mock_struct_0 = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(comm_mock_0, 'to_value', return_value=comm_mock_struct_0)

        mocker.patch.object(comm_mock_1, 'publish_node_name', '1')
        mocker.patch.object(comm_mock_1, 'subscribe_node_name', '2')
        mocker.patch.object(comm_mock_1, 'topic_name', '1->2')
        mocker.patch.object(comm_mock_1, 'subscription_construction_order', 0)
        mocker.patch.object(comm_mock_1, 'publisher_construction_order', 0)
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

    def test_combine_path(
        self,
        create_node_path,
        create_comm,
        create_arch,
    ):
        # combine [node, comm, node] + [node, comm, node]
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', 'topic_1')
        node_2: NodePathStructValue = create_node_path('node_2', 'topic_1', None)

        node_1_left: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        node_1_right: NodePathStructValue = create_node_path('node_1', None, 'topic_1')
        node_1_left_unmatched: NodePathStructValue = \
            create_node_path('node_1', 'topic_0', 'topic_2')

        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_1: CommunicationStructValue = create_comm('topic_1', 'node_1', 'node_2')

        arch: Architecture = create_arch(
            [node_0, node_1, node_1_left, node_1_right, node_2, node_1_left_unmatched],
            [comm_0, comm_1]
        )

        # # combine [node_0, comm_0, node_1_left] + [node_1_right, comm_1, node_2]
        path_left = PathStructValue(
            None, (node_0, comm_0, node_1_left))
        path_right = PathStructValue(
            None, (node_1_right, comm_1, node_2))

        path_expect = PathStructValue(
            None, (node_0, comm_0, node_1, comm_1, node_2))
        path = arch.combine_path(path_left, path_right)
        assert path == path_expect

        # # combine [node_0, comm_0, node_1_left_unmatched] + [node_1_right, comm_1, node_2]
        path_left = PathStructValue(
            None, (node_0, comm_0, node_1_left_unmatched))
        path_right = PathStructValue(
            None, (node_1_right, comm_1, node_2))
        with pytest.raises(InvalidArgumentError):
            arch.combine_path(path_left, path_right)

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


class TestArchitectureInsertAndUpdate:
    template_architecture = Template("""
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
  publishes:
    - topic_name: /ping
      callback_names:
        - $publisher_callback
$contexts
""")

    passings_text = """
  variable_passings:
    - callback_name_write: subscription_callback_0
      callback_name_read: timer_callback_1
"""

    contexts_text = """
  message_contexts:
    - context_type: use_latest_message
      subscription_topic_name: /pong
      publisher_topic_name: /ping
"""

    contexts_text_undefined = """
  message_contexts:
    - context_type: UNDEFINED
      subscription_topic_name: /pong
      publisher_topic_name: /ping
"""

    def test_update_message_context(self, mocker, caplog):
        # update message context to minimum architecture
        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.update_message_context('/pong_node', 'use_latest_message', '/pong', '/ping')

        architecture_text_expected = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # update message context to be full architecture
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.update_message_context('/pong_node', 'use_latest_message', '/pong', '/ping')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # update UNDEFINED to full architecture
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.update_message_context('/pong_node', 'UNDEFINED', '/pong', '/ping')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text_undefined)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid update
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        with pytest.raises(ItemNotFoundError):
            arch.update_message_context('/not_exist_node', 'callback_chain', '/pong', '/ping')

        arch.update_message_context('/pong_node', 'invalid_contexts', '/pong', '/ping')
        assert any('Failed to load message context.' in message for message in caplog.messages)

        with pytest.raises(ItemNotFoundError):
            arch.update_message_context('/pong_node', 'callback_chain',
                                        '/not_exist_topic', '/ping')
        with pytest.raises(ItemNotFoundError):
            arch.update_message_context('/pong_node', 'callback_chain',
                                        '/pong', '/not_exist_topic')

    def test_insert_publisher_callback(self, mocker):
        # insert publisher callback to publisher to minimum architecture
        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='UNDEFINED',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_publisher_callback('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # insert publisher callback to publisher to be full architecture
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='UNDEFINED',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_publisher_callback('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid insert
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        with pytest.raises(ItemNotFoundError):
            arch.insert_publisher_callback('/not_exist_node', '/ping', 'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.insert_publisher_callback('/pong_node', '/not_exist_topic',
                                           'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.insert_publisher_callback('/pong_node', '/ping', 'not_exist_callback_1')

        # duplicated insert
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_publisher_callback('/pong_node', '/ping', 'timer_callback_1')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

    def test_insert_variable_passing(self, mocker):
        # insert variable passing to minimum architecture
        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_variable_passing('/pong_node', 'subscription_callback_0', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # insert variable passing to be full architecture
        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_variable_passing('/pong_node', 'subscription_callback_0', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid insert
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        with pytest.raises(ItemNotFoundError):
            arch.insert_variable_passing('/not_exist_node', 'subscription_callback_0',
                                         'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.insert_variable_passing('/pong_node', 'not_exist_callback_0',
                                         'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.insert_variable_passing('/pong_node', 'subscription_callback_0',
                                         'not_exist_callback_0')

        # duplicated insert
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.insert_variable_passing('/pong_node', 'subscription_callback_0', 'timer_callback_1')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)


class TestArchitectureRemove:
    template_architecture = Template("""
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
  publishes:
    - topic_name: /ping
      callback_names:
        - $publisher_callback
$contexts
""")

    passings_text = """
  variable_passings:
    - callback_name_write: subscription_callback_0
      callback_name_read: timer_callback_1
"""

    contexts_text = """
  message_contexts:
    - context_type: use_latest_message
      subscription_topic_name: /pong
      publisher_topic_name: /ping
"""

    def test_remove_publisher_callback(self, mocker):
        # remove publisher to publisher function without variable passing
        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.remove_publisher_callback('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='UNDEFINED',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        # remove publisher to publisher function with variable passing
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.remove_publisher_callback('/pong_node', '/ping', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='UNDEFINED',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid remove
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        with pytest.raises(ItemNotFoundError):
            arch.remove_publisher_callback('/pong_node', '/not_exist_topic',
                                           'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.remove_publisher_callback('/not_exist_node', '/ping', 'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.remove_publisher_callback('/pong_node', '/ping', 'not_exist_callback_1')

    def test_remove_variable_passing(self, mocker):
        # remove variable passing to be minimum architecture
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.remove_variable_passing('/pong_node', 'subscription_callback_0', 'timer_callback_1')

        architecture_text_expected = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts='')
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # remove variable passing to full architecture
        architecture_text_expected = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text_expected))
        arch = Architecture('yaml', 'architecture.yaml')

        arch.remove_variable_passing('/pong_node', 'subscription_callback_0', 'timer_callback_1')

        architecture_text = \
            self.template_architecture.substitute(passings='',
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch_expected = Architecture('yaml', 'architecture.yaml')

        assert set(arch.nodes) == set(arch_expected.nodes)
        assert set(arch.communications) == set(arch_expected.communications)
        assert set(arch.executors) == set(arch_expected.executors)
        assert set(arch.paths) == set(arch_expected.paths)

        # invalid remove
        architecture_text = \
            self.template_architecture.substitute(passings=self.passings_text,
                                                  publisher_callback='timer_callback_1',
                                                  contexts=self.contexts_text)
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        arch = Architecture('yaml', 'architecture.yaml')

        with pytest.raises(ItemNotFoundError):
            arch.remove_variable_passing('/not_exist_node', 'subscription_callback_0',
                                         'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.remove_variable_passing('/pong_node', 'not_exist_callback_0',
                                         'timer_callback_1')
        with pytest.raises(ItemNotFoundError):
            arch.remove_variable_passing('/pong_node', 'subscription_callback_0',
                                         'not_exist_callback_0')


class TestArchitectureRename:
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


class TestArchitectureDiff:

    def test_diff_node_names(self, mocker):
        architecture_mock1 = mocker.Mock(spec=Architecture)
        architecture_mock2 = mocker.Mock(spec=Architecture)
        mocker.patch.object(architecture_mock1, 'node_names', ('node1', 'node2'))
        mocker.patch.object(architecture_mock2, 'node_names', ('node1', 'node3', 'node4'))
        node_names_in_mock1, node_names_in_mock2 = \
            Architecture.diff_node_names(architecture_mock1, architecture_mock2)
        assert node_names_in_mock1 == ('node2',)
        assert sorted(node_names_in_mock2) == sorted(('node3', 'node4'))

        mocker.patch.object(architecture_mock1, 'node_names', ('node1',))
        mocker.patch.object(architecture_mock2, 'node_names', ())
        node_names_in_mock1, node_names_in_mock2 = \
            Architecture.diff_node_names(architecture_mock1, architecture_mock2)
        assert node_names_in_mock1 == ('node1',)
        assert node_names_in_mock2 == ()

        mocker.patch.object(architecture_mock1, 'node_names', ())
        mocker.patch.object(architecture_mock2, 'node_names', ('node1',))
        node_names_in_mock1, node_names_in_mock2 = \
            Architecture.diff_node_names(architecture_mock1, architecture_mock2)
        assert node_names_in_mock1 == ()
        assert node_names_in_mock2 == ('node1',)

    def test_diff_topic_names(self, mocker):
        architecture_mock1 = mocker.Mock(spec=Architecture)
        architecture_mock2 = mocker.Mock(spec=Architecture)
        mocker.patch.object(architecture_mock1, 'topic_names', ('topic1', 'topic2'))
        mocker.patch.object(architecture_mock2, 'topic_names', ('topic1', 'topic3', 'topic4'))
        topics_in_mock1, topics_in_mock2 = \
            Architecture.diff_topic_names(architecture_mock1, architecture_mock2)
        assert topics_in_mock1 == ('topic2',)
        assert sorted(topics_in_mock2) == sorted(('topic3', 'topic4'))

        mocker.patch.object(architecture_mock1, 'topic_names', ('topic1',))
        mocker.patch.object(architecture_mock2, 'topic_names', ())
        topics_in_mock1, topics_in_mock2 = \
            Architecture.diff_topic_names(architecture_mock1, architecture_mock2)
        assert topics_in_mock1 == ('topic1',)
        assert topics_in_mock2 == ()

        mocker.patch.object(architecture_mock1, 'topic_names', ())
        mocker.patch.object(architecture_mock2, 'topic_names', ('topic1',))
        topics_in_mock1, topics_in_mock2 = \
            Architecture.diff_topic_names(architecture_mock1, architecture_mock2)
        assert topics_in_mock1 == ()
        assert topics_in_mock2 == ('topic1',)

    def test_diff_node_pubs(self, mocker):
        node_mock1 = mocker.Mock(spec=NodeStructValue)
        node_mock2 = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock1, 'publish_topic_names', ('topic1', 'topic2'))
        mocker.patch.object(node_mock2, 'publish_topic_names', ('topic1', 'topic3', 'topic4'))
        pubs_in_mock1, pubs_in_mock2 = Architecture.diff_node_pubs(node_mock1, node_mock2)
        assert pubs_in_mock1 == ('topic2',)
        assert sorted(pubs_in_mock2) == sorted(('topic3', 'topic4'))

        mocker.patch.object(node_mock1, 'publish_topic_names', ('topic1',))
        mocker.patch.object(node_mock2, 'publish_topic_names', ())
        pubs_in_mock1, pubs_in_mock2 = Architecture.diff_node_pubs(node_mock1, node_mock2)
        assert pubs_in_mock1 == ('topic1',)
        assert pubs_in_mock2 == ()

        mocker.patch.object(node_mock1, 'publish_topic_names', ())
        mocker.patch.object(node_mock2, 'publish_topic_names', ('topic1',))
        pubs_in_mock1, pubs_in_mock2 = Architecture.diff_node_pubs(node_mock1, node_mock2)
        assert pubs_in_mock1 == ()
        assert pubs_in_mock2 == ('topic1',)

    def test_diff_node_subs(self, mocker):
        node_mock1 = mocker.Mock(spec=NodeStructValue)
        node_mock2 = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_mock1, 'subscribe_topic_names', ('topic1', 'topic2'))
        mocker.patch.object(node_mock2, 'subscribe_topic_names', ('topic1', 'topic3', 'topic4'))
        subs_in_mock1, subs_in_mock2 = Architecture.diff_node_subs(node_mock1, node_mock2)
        assert subs_in_mock1 == ('topic2',)
        assert sorted(subs_in_mock2) == sorted(('topic3', 'topic4'))

        mocker.patch.object(node_mock1, 'subscribe_topic_names', ('topic1',))
        mocker.patch.object(node_mock2, 'subscribe_topic_names', ())
        subs_in_mock1, subs_in_mock2 = Architecture.diff_node_subs(node_mock1, node_mock2)
        assert subs_in_mock1 == ('topic1',)
        assert subs_in_mock2 == ()

        mocker.patch.object(node_mock1, 'subscribe_topic_names', ())
        mocker.patch.object(node_mock2, 'subscribe_topic_names', ('topic1',))
        subs_in_mock1, subs_in_mock2 = Architecture.diff_node_subs(node_mock1, node_mock2)
        assert subs_in_mock1 == ()
        assert subs_in_mock2 == ('topic1',)
