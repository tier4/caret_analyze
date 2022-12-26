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

from collections import defaultdict


from typing import Callable, List, Optional, Tuple

from caret_analyze.architecture import Architecture
from caret_analyze.architecture.architecture_loaded import ArchitectureLoaded
from caret_analyze.architecture.architecture_reader_factory import \
    ArchitectureReaderFactory
from caret_analyze.architecture.combine_path import CombinePath
from caret_analyze.architecture.reader_interface import ArchitectureReader
from caret_analyze.architecture.struct import (CommunicationStruct,
                                               NodeStruct)
from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.value_objects import (CommunicationStructValue, NodePathStructValue,
                                         NodeStructValue, PathStructValue,
                                         PublisherStructValue, SubscriptionStructValue)

import pytest


@pytest.fixture
def create_publisher():
    def _create_publisher(node_name: str, topic_name: str) -> PublisherStructValue:
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
def create_node_path(
    create_publisher: Callable[[Optional[str], Optional[str]], PublisherStructValue],
    create_subscription: Callable[[Optional[str], Optional[str]], SubscriptionStructValue]
):
    def _create_node_path(
        node_name: str,
        sub_topic_name: Optional[str],
        pub_topic_name: Optional[str],
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
        node_paths: Tuple[NodePathStructValue],
        comms: Tuple[CommunicationStructValue]
    ) -> Architecture:
        node_dict: defaultdict[str, List[NodePathStructValue]] = defaultdict(list)
        for node_path in node_paths:
            node_dict[node_path.node_name].append(node_path)
        node_list = []  # List[mock]

        for node, paths in node_dict.items():
            node_value_mock = mocker.Mock(spec=NodeStructValue)
            mocker.patch.object(node_value_mock, 'paths', tuple(paths))
            mocker.patch.object(node_value_mock, 'callbacks', [])
            mocker.patch.object(node_value_mock, 'node_name', node)
            # mocker.patch('caret_analyze.value_objects.node.NodeStructValue')

            node_mock = mocker.Mock(spec=NodeStruct)
            mocker.patch.object(node_mock, 'callbacks', [])
            mocker.patch.object(node_mock, 'node_name', node)
            mocker.patch.object(node_mock, 'to_value', return_value=node_value_mock)

            node_list.append(node_mock)

        comm_list = []
        for comm in comms:
            comm_mock = mocker.Mock(spec=CommunicationStruct)
            mocker.patch.object(comm_mock, 'to_value', return_value=comm)
            # mocker.patch('caret_analyze.architecture.struct.communication.CommunicationStruct')
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
def create_get_node(mocker):
    def _create_get_node(
        node_paths: Tuple[NodePathStructValue],
        joint_node_name: str,
    ) -> Callable[[str], NodeStructValue]:
        # node_path_list: List[NodePathStructValue] = []
        # for node_path in node_paths:
        #     if node_path.node_name == joint_node_name:
        #         node_path_list.append(node_path)
        node_value_mock = mocker.Mock(spec=NodeStructValue)
        # mocker.patch.object(node_value_mock, 'paths', tuple(node_path_list))
        mocker.patch.object(node_value_mock, 'paths', tuple(node_paths))
        mocker.patch.object(node_value_mock, 'callbacks', [])
        mocker.patch.object(node_value_mock, 'node_name', joint_node_name)
        def get_node(node_name: str) -> NodeStructValue:
            return node_value_mock
        return get_node
    return _create_get_node

@pytest.fixture
def create_get_communication():
    def _create_get_communication(
        joint_comm: CommunicationStructValue,
    ):
        def get_communication(
            publisher_node_name: str,
            subscription_node_name: str,
            topic_name: str
        ) -> CommunicationStructValue:
            return joint_comm
        return get_communication
    return _create_get_communication



@pytest.fixture
def create_node(
    create_publisher: Callable[[Optional[str], Optional[str]], PublisherStructValue],
    create_subscription: Callable[[Optional[str], Optional[str]], SubscriptionStructValue]
):
    def _create_node(
        node_name: str,
        sub_topic_name: Optional[str],
        pub_topic_name: Optional[str]
    ) -> NodeStructValue:
        pubs: Tuple[PublisherStructValue, ...]
        subs: Tuple[SubscriptionStructValue, ...]

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
def create_comm(create_node: Callable[[str, Optional[str], Optional[str]], NodeStructValue]):
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


class TestCombinePath:

    def test_combine_empty_path(
        self,
        create_get_node,
        create_get_communication,
    ):
        # combine [] + []
        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)
        path_left = PathStructValue(None, ())
        path_right = PathStructValue(None, ())
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__comm__comm(
        self,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # combine [comm] + [comm] = NG
        left_comm: CommunicationStructValue = create_comm('topic_1', 'pub_node', 'sub_node')
        right_comm: CommunicationStructValue = create_comm('topic_2', 'pub_node', 'sub_node')

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (left_comm,))
        path_right = PathStructValue(None, (right_comm,))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

        # #  TODO(miura): [comm_1] + [comm_2] = [comm_1, node_x, comm_2]

    def test_combine__node__node(
        self,
        create_node_path,
        create_get_node,
        create_get_communication,
    ):
        # combine [node] + [node] (difference nodes) = NG
        left_node: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        right_node: NodePathStructValue = create_node_path('node_1', 'topic_0', None)

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (left_node,))
        path_right = PathStructValue(None, (right_node,))

        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

        # #  TODO(miura): [node_1] + [node_2] = [node_1, comm_x, node_2]

    def test_combine__node__comm(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [node_0] + [comm_0] = OK
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        # node_0_unmatched: NodePathStructValue = create_node_path('node_0', None, 'topic_1')
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        # comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_2', 'node_1')

        get_node: Callable[[str], NodeStructValue] = create_get_node([node_0], node_0.node_name)
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0,))
        path_right = PathStructValue(None, (comm_0,))

        path_expect = PathStructValue(None, (node_0, comm_0))
        path = combine_path.combine(path_left, path_right)
        assert path == path_expect

    def test_combine__node_ng__comm(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [node_0_unmatched] + [comm_0] = NG
        # node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_0_unmatched: NodePathStructValue = create_node_path('node_0', None, 'topic_1')
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        # comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_2', 'node_1')

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0_unmatched,))
        path_right = PathStructValue(None, (comm_0,))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__node__comm_ng(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [node_0] + [comm_0_unmatched] = NG
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        # node_0_unmatched: NodePathStructValue = create_node_path('node_0', None, 'topic_1')
        # comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_2', 'node_1')

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0,))
        path_right = PathStructValue(None, (comm_0_unmatched,))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__comm__node(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [comm_0] + [node_1] = OK
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        # node_1_unmatched: NodePathStructValue = create_node_path('node_1', 'topic_1', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        # comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_2')

        get_node: Callable[[str], NodeStructValue] = create_get_node([node_1], node_1.node_name)
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (comm_0,))
        path_right = PathStructValue(None, (node_1,))

        path = combine_path.combine(path_left, path_right)
        path_expect = PathStructValue(None, (comm_0, node_1))
        assert path == path_expect

    def test_combine__comm_ng__node(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [comm_0_unmatched] + [node_1] = NG
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        # node_1_unmatched: NodePathStructValue = create_node_path('node_1', 'topic_1', None)
        # comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_2')

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (comm_0_unmatched,))
        path_right = PathStructValue(None, (node_1,))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__comm__node_ng(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [comm_0] + [node_1_unmatched] = NG
        # node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        node_1_unmatched: NodePathStructValue = create_node_path('node_1', 'topic_1', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        # comm_0_unmatched: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_2')

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (comm_0,))
        path_right = PathStructValue(None, (node_1_unmatched,))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__path__comm(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # combine [node, comm, node] + [comm]
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', 'topic_1')
        node_1_left: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_1: CommunicationStructValue = create_comm('topic_1', 'node_1', 'node_2')

        get_node: Callable[[str], NodeStructValue] = \
            create_get_node([node_1, node_1_left], node_1.node_name)
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0, comm_0, node_1_left))
        path_right = PathStructValue(None, (comm_1,))

        path = combine_path.combine(path_left, path_right)
        path_expect = PathStructValue(None, (node_0, comm_0, node_1, comm_1))
        assert path == path_expect

    def test_combine__comm__path(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):

        # combine [comm] + [node, comm, node]
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', 'topic_1')
        node_1_right: NodePathStructValue = create_node_path('node_1', None, 'topic_1')
        node_2: NodePathStructValue = create_node_path('node_2', 'topic_1', None)

        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_1: CommunicationStructValue = create_comm('topic_1', 'node_1', 'node_2')

        get_node: Callable[[str], NodeStructValue] = \
            create_get_node([node_1, node_1_right], node_1.node_name)
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (comm_0,))
        path_right = PathStructValue(None, (node_1_right, comm_1, node_2))

        path = combine_path.combine(path_left, path_right)
        path_expect = PathStructValue(None, (comm_0, node_1, comm_1, node_2))
        assert path == path_expect

    def test_combine__node_comm__comm_node(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        #  [node_0, comm_0] + [comm_0, node_1] = OK
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')

        comm_0_left: CommunicationStructValue = create_comm('topic_0', 'node_0', None)
        comm_0_right: CommunicationStructValue = create_comm('topic_0', None, 'node_1')

        path_left = PathStructValue(None, (node_0, comm_0_left))
        path_right = PathStructValue(None, (comm_0_right, node_1))

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication(comm_0)

        path_expect = PathStructValue(None, (node_0, comm_0, node_1))
        combine_path = CombinePath(get_node, get_communication)
        path = combine_path.combine(path_left, path_right)
        assert path == path_expect

    def test_combine__node_comm__comm_node__with_none(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [node_0, comm_0_left] + [comm_0_right, node_1] = OK
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')

        comm_0_left: CommunicationStructValue = create_comm('topic_0', 'node_0', None)
        comm_0_right: CommunicationStructValue = create_comm('topic_0', None, 'node_1')
        # comm_0_left_unmatched: CommunicationStructValue = \
        #     create_comm('topic_0', 'node_0', 'node_2')  # node_2 for unmatched

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication(comm_0)
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0, comm_0_left))
        path_right = PathStructValue(None, (comm_0_right, node_1))

        path_expect = PathStructValue(None, (node_0, comm_0, node_1))
        path = combine_path.combine(path_left, path_right)
        assert path == path_expect

    def test_combine__node_comm_ng__comm_node(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # [node_0, comm_0_left_unmatched] + [comm_0_right, node_1] = NG
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')

        # comm_0_left: CommunicationStructValue = create_comm('topic_0', 'node_0', None)
        comm_0_right: CommunicationStructValue = create_comm('topic_0', None, 'node_1')
        comm_0_left_unmatched: CommunicationStructValue = \
            create_comm('topic_0', 'node_0', 'node_2')  # node_2 for unmatched

        get_node: Callable[[str], NodeStructValue] = create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication(comm_0)
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(None, (node_0, comm_0_left_unmatched))
        path_right = PathStructValue(None, (comm_0_right, node_1))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)

    def test_combine__path__path(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # combine [node_0, comm_0, node_1_left] + [node_1_right, comm_1, node_2]
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', 'topic_1')
        node_2: NodePathStructValue = create_node_path('node_2', 'topic_1', None)

        node_1_left: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        node_1_right: NodePathStructValue = create_node_path('node_1', None, 'topic_1')
        # node_1_left_unmatched: NodePathStructValue = \
        #     create_node_path('node_1', 'topic_0', 'topic_2')

        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_1: CommunicationStructValue = create_comm('topic_1', 'node_1', 'node_2')

        get_node: Callable[[str], NodeStructValue] = \
            create_get_node([node_1, node_1_left, node_1_right], node_1.node_name)
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(
            None, (node_0, comm_0, node_1_left))
        path_right = PathStructValue(
            None, (node_1_right, comm_1, node_2))

        path_expect = PathStructValue(
            None, (node_0, comm_0, node_1, comm_1, node_2))
        path = combine_path.combine(path_left, path_right)
        assert path == path_expect

    def test_combine__path_ng__path(
        self,
        create_node_path,
        create_comm,
        create_get_node,
        create_get_communication,
    ):
        # combine [node_0, comm_0, node_1_left_unmatched] + [node_1_right, comm_1, node_2]
        node_0: NodePathStructValue = create_node_path('node_0', None, 'topic_0')
        # node_1: NodePathStructValue = create_node_path('node_1', 'topic_0', 'topic_1')
        node_2: NodePathStructValue = create_node_path('node_2', 'topic_1', None)

        # node_1_left: NodePathStructValue = create_node_path('node_1', 'topic_0', None)
        node_1_right: NodePathStructValue = create_node_path('node_1', None, 'topic_1')
        node_1_left_unmatched: NodePathStructValue = \
            create_node_path('node_1', 'topic_0', 'topic_2')

        comm_0: CommunicationStructValue = create_comm('topic_0', 'node_0', 'node_1')
        comm_1: CommunicationStructValue = create_comm('topic_1', 'node_1', 'node_2')

        get_node: Callable[[str], NodeStructValue] = \
            create_get_node([], '')
        get_communication: Callable[[str, str, str], CommunicationStructValue] = \
            create_get_communication([])
        combine_path = CombinePath(get_node, get_communication)

        path_left = PathStructValue(
            None, (node_0, comm_0, node_1_left_unmatched))
        path_right = PathStructValue(
            None, (node_1_right, comm_1, node_2))
        with pytest.raises(InvalidArgumentError):
            combine_path.combine(path_left, path_right)
