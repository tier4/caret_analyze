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

from typing import Dict, List, Optional, Sequence

from caret_analyze.architecture.architecture_dict import (
    ArchitectureDict,
    CallbackDicts,
    CbgsDicts,
    ExecutorsDicts,
    NamedPathsDicts,
    NodesDicts,
    PubDicts,
    SubDicts,
    TfBroadcasterDicts,
    TfBufferDicts,
    VarPassDicts,
)
from caret_analyze.exceptions import UnsupportedTypeError
from caret_analyze.value_objects import (
    CallbackGroupStructValue,
    CallbackStructValue,
    ExecutorStructValue,
    NodePathStructValue,
    NodeStructValue,
    PathStructValue, PublisherStructValue,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    TimerCallbackStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)

import pytest


@pytest.fixture
def setup_cbgs_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=CbgsDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.CbgsDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_cbs_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=CallbackDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.CallbackDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_tf_br_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=TfBroadcasterDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.TfBroadcasterDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_tf_buff_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=TfBufferDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.TfBufferDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_pub_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=PubDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.PubDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_sub_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=SubDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.SubDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_named_path_mock(mocker):
    def _setup(data):
        named_path_mock = mocker.Mock(spec=NamedPathsDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.NamedPathsDicts',
            return_value=named_path_mock)
        mocker.patch.object(named_path_mock, 'data', data)
        return named_path_mock
    return _setup


@pytest.fixture
def setup_var_pass_mock(mocker):
    def _setup(data):
        mock = mocker.Mock(spec=VarPassDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.VarPassDicts',
            return_value=mock)
        mocker.patch.object(mock, 'data', data)
        return mock
    return _setup


@pytest.fixture
def setup_exec_mock(mocker):
    def _setup(data):
        exec_mock = mocker.Mock(spec=ExecutorsDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.ExecutorsDicts',
            return_value=exec_mock)
        mocker.patch.object(exec_mock, 'data', data)
        return exec_mock
    return _setup


@pytest.fixture
def setup_nodes_mock(mocker):
    def _setup(data):
        node_mock = mocker.Mock(spec=NodesDicts)
        mocker.patch(
            'caret_analyze.architecture.architecture_dict.NodesDicts',
            return_value=node_mock)
        mocker.patch.object(node_mock, 'data', data)
        return node_mock
    return _setup


class TestArchitectureDict:

    def test_empty(
        self,
        setup_named_path_mock,
        setup_exec_mock,
        setup_nodes_mock,
    ):
        dicts = ArchitectureDict((), (), ())

        setup_named_path_mock([])
        setup_exec_mock([])
        setup_nodes_mock([])

        expected: Dict[str, List] = {
            'named_paths': [],
            'executors': [],
            'nodes': []
        }
        assert dicts.to_dict() == expected

    def test_full(
        self,
        setup_named_path_mock,
        setup_exec_mock,
        setup_nodes_mock,
    ):
        exporter = ArchitectureDict((), (), ())

        setup_named_path_mock(['path_dict'])
        setup_exec_mock(['exec_dict'])
        setup_nodes_mock(['node_dict'])

        expected = {
            'named_paths': ['path_dict'],
            'executors': ['exec_dict'],
            'nodes': ['node_dict']
        }

        assert exporter.to_dict() == expected

    def test_str(
        self,
        setup_named_path_mock,
        setup_exec_mock,
        setup_nodes_mock,
    ):
        exporter = ArchitectureDict((), (), ())

        setup_named_path_mock([])
        setup_exec_mock([])
        setup_nodes_mock([])

        expected = """\
named_paths: []
executors: []
nodes: []
"""
        assert str(exporter) == expected


class TestNamedPathsDicts:

    def test_empty(self):
        path_dict = NamedPathsDicts([])
        expect = []
        assert path_dict.data == expect

    @pytest.fixture
    def create_path_mock(self, mocker):
        def _create(path_name: str, node_paths: Sequence[NodePathStructValue]):
            path_mock = mocker.Mock(spec=PathStructValue)
            mocker.patch.object(path_mock, 'path_name', path_name)
            mocker.patch.object(path_mock, 'node_paths', node_paths)
            return path_mock
        return _create

    @pytest.fixture
    def create_node_path_mock(self, mocker):
        def _create(
            node_name: str,
            publish_topic_name: Optional[str],
            subscribe_topic_name: Optional[str],
            tf_frame_broadcaster: Optional[TransformFrameBroadcasterStructValue],
            tf_frame_buffer: Optional[TransformFrameBufferStructValue],
        ):
            path_mock = mocker.Mock(spec=NodePathStructValue)
            mocker.patch.object(path_mock, 'node_name', node_name)
            mocker.patch.object(path_mock, 'publish_topic_name', publish_topic_name)
            mocker.patch.object(path_mock, 'subscribe_topic_name', subscribe_topic_name)
            mocker.patch.object(path_mock, 'tf_frame_broadcaster', tf_frame_broadcaster)
            mocker.patch.object(path_mock, 'tf_frame_buffer', tf_frame_buffer)
            return path_mock
        return _create

    def test_single_node_path(
        self,
        create_path_mock,
        create_node_path_mock,
    ):
        node_path_mock = create_node_path_mock('node', None, None, None, None)
        path_mock = create_path_mock('target_path', [node_path_mock])

        path_dict = NamedPathsDicts([path_mock])
        expect = [
            {
                'path_name': path_mock.path_name,
                'node_chain': [
                    {
                        'node_name': node_path_mock.node_name,
                        'publish_topic_name': node_path_mock.publish_topic_name,
                        'subscribe_topic_name': node_path_mock.subscribe_topic_name
                    }
                ]
            }
        ]
        assert path_dict.data == expect

    def test_multi_node_path(
        self,
        create_path_mock,
        create_node_path_mock,
    ):
        node_path_mock = create_node_path_mock('node0', 'pub', None, None, None)
        node_path_mock_ = create_node_path_mock('node1', None, 'sub', None, None)
        path_mock = create_path_mock('target_path', [node_path_mock, node_path_mock_])

        path_dict = NamedPathsDicts([path_mock])
        expect = [
            {
                'path_name': path_mock.path_name,
                'node_chain': [
                    {
                        'node_name': node_path_mock.node_name,
                        'publish_topic_name': node_path_mock.publish_topic_name,
                        'subscribe_topic_name': node_path_mock.subscribe_topic_name
                    } for node_path_mock in [node_path_mock, node_path_mock_]
                ]
            }
        ]
        assert path_dict.data == expect

    @pytest.fixture
    def create_tf_br_mock(self, mocker):
        def _create(frame_id: str, child_frame_id: str):
            tf_br_mock = mocker.Mock(spec=TransformFrameBroadcasterStructValue)
            mocker.patch.object(tf_br_mock, 'frame_id', frame_id)
            mocker.patch.object(tf_br_mock, 'child_frame_id', child_frame_id)
            return tf_br_mock
        return _create

    @pytest.fixture
    def create_tf_buff_mock(self, mocker):
        def _create(
            listen_frame_id: str,
            listen_child_frame_id: str,
            source_frame_id: str,
            target_frame_id: str
        ):
            tf_buff_mock = mocker.Mock(spec=TransformFrameBufferStructValue)
            mocker.patch.object(tf_buff_mock, 'listen_frame_id', listen_frame_id)
            mocker.patch.object(tf_buff_mock, 'listen_child_frame_id', listen_child_frame_id)
            mocker.patch.object(tf_buff_mock, 'lookup_source_frame_id', source_frame_id)
            mocker.patch.object(tf_buff_mock, 'lookup_target_frame_id', target_frame_id)
            return tf_buff_mock
        return _create

    def test_tf_frame_broadcaster(
        self,
        create_path_mock,
        create_node_path_mock,
        create_tf_br_mock,
    ):
        tf_buff_mock = create_tf_br_mock('frame_id', 'child_frame_id')
        node_path_mock = create_node_path_mock('node0', None, None, tf_buff_mock, None)
        path_mock = create_path_mock('target_path', [node_path_mock])

        path_dict = NamedPathsDicts([path_mock])
        tf_br = node_path_mock.tf_frame_broadcaster
        expect = [
            {
                'path_name': path_mock.path_name,
                'node_chain': [
                    {
                        'node_name': node_path_mock.node_name,
                        'publish_topic_name': node_path_mock.publish_topic_name,
                        'subscribe_topic_name': node_path_mock.subscribe_topic_name,
                        'broadcast_frame_id': tf_br.frame_id,
                        'broadcast_child_frame_id': tf_br.child_frame_id,
                    }
                ]
            }
        ]
        assert path_dict.data == expect

    def test_tf_frame_buffer(
        self,
        create_path_mock,
        create_node_path_mock,
        create_tf_buff_mock,
    ):
        tf_buff_mock = create_tf_buff_mock(
            'listen_frame_id', 'listen_child_frame_id', 'source_frame_id', 'target_frame_id')
        node_path_mock = create_node_path_mock('node0', None, None, None, tf_buff_mock)
        path_mock = create_path_mock('target_path', [node_path_mock])

        path_dict = NamedPathsDicts([path_mock])
        tf_buff = node_path_mock.tf_frame_buffer
        expect = [
            {
                'path_name': path_mock.path_name,
                'node_chain': [
                    {
                        'node_name': node_path_mock.node_name,
                        'publish_topic_name': node_path_mock.publish_topic_name,
                        'subscribe_topic_name': node_path_mock.subscribe_topic_name,
                        'buffer_listen_frame_id': tf_buff.listen_frame_id,
                        'buffer_listen_child_frame_id': tf_buff.listen_child_frame_id,
                        'buffer_lookup_source_frame_id': tf_buff.lookup_source_frame_id,
                        'buffer_lookup_target_frame_id': tf_buff.lookup_target_frame_id,
                    }
                ]
            }
        ]
        assert path_dict.data == expect


class TestExecutorDicts:

    @pytest.fixture
    def create_exec_mock(self, mocker):
        def _create(
            id_name: str,
            type_name: str,
            name: str,
            cbgs: Sequence[CallbackGroupStructValue]
        ):
            exec_struct = mocker.Mock(spec=ExecutorStructValue)
            mocker.patch.object(exec_struct, 'executor_id', id_name)
            mocker.patch.object(exec_struct, 'executor_type_name', type_name)
            mocker.patch.object(exec_struct, 'executor_name', name)
            mocker.patch.object(exec_struct, 'callback_groups', cbgs)
            return exec_struct
        return _create

    @pytest.fixture
    def create_cbg_mock(self, mocker):
        def _create(id_name: str, type_name: str, name: str, node_name: str):
            cbg_mock = mocker.Mock(spec=CallbackGroupStructValue)
            mocker.patch.object(cbg_mock, 'callback_group_id', id_name)
            mocker.patch.object(cbg_mock, 'callback_group_type_name', type_name)
            mocker.patch.object(cbg_mock, 'callback_group_name', name)
            mocker.patch.object(cbg_mock, 'node_name', node_name)
            return cbg_mock
        return _create

    def test_empty(self):
        exec_dicts = ExecutorsDicts([])
        expect = []
        assert exec_dicts.data == expect

    def test_full(
        self,
        create_cbg_mock,
        create_exec_mock,
    ):
        exclusive_cbg_mock = create_cbg_mock(
            'cbgid0', 'mutually_exclusive', 'callback_group_1', 'node0'
        )
        single_threaded_exec_mock = create_exec_mock(
            'id0', 'single_threaded_executor', 'aaa_executor', [exclusive_cbg_mock]
        )

        reentrant_cbg_mock = create_cbg_mock(
            'cbgid1', 'reentrant', 'callback_group_0', 'node1'
        )
        multi_threaded_exec_mock = create_exec_mock(
            'id1', 'multi_threaded_executor', 'bbb_executor', [reentrant_cbg_mock]
        )

        exec_dicts = ExecutorsDicts(
            [single_threaded_exec_mock, multi_threaded_exec_mock]
        )

        expect = [
            {
                'executor_id': single_threaded_exec_mock.executor_id,
                'executor_type': single_threaded_exec_mock.executor_type_name,
                'executor_name': single_threaded_exec_mock.executor_name,
                'callback_group_ids': [
                    single_threaded_exec_mock.callback_groups[0].callback_group_id,
                ]
            },
            {
                'executor_id': multi_threaded_exec_mock.executor_id,
                'executor_type': multi_threaded_exec_mock.executor_type_name,
                'executor_name': multi_threaded_exec_mock.executor_name,
                'callback_group_ids': [
                    multi_threaded_exec_mock.callback_groups[0].callback_group_id,
                ]
            },
        ]

        assert exec_dicts.data == expect

    def test_is_ignored(
        self,
        create_exec_mock
    ):
        exec_0 = create_exec_mock('id', 'multi_threaded_executor', 'executor', [])
        exec_1 = create_exec_mock('skip', 'multi_threaded_executor', 'executor', [])

        exec_dicts = ExecutorsDicts(
            [exec_0, exec_1], lambda x: x.executor_id == 'skip'
        )

        expect = [
            {
                'executor_id': e.executor_id,
                'executor_name': e.executor_name,
                'executor_type': e.executor_type_name,
                'callback_group_ids': []
            } for e in [exec_0]
        ]

        assert exec_dicts.data == expect

    def test_name_sort(
        self,
        create_exec_mock
    ):
        exec_0 = create_exec_mock('id0', 'multi_threaded_executor', 'aaa_executor', [])
        exec_1 = create_exec_mock('id1', 'multi_threaded_executor', 'bbb_executor', [])
        exec_2 = create_exec_mock('id2', 'single_threaded_executor', 'ccc_executor', [])
        exec_3 = create_exec_mock('id3', 'single_threaded_executor', 'ddd_executor', [])

        exec_dicts = ExecutorsDicts(
            [exec_3, exec_0, exec_1, exec_2]
        )

        expect = [
            {
                'executor_id': e.executor_id,
                'executor_name': e.executor_name,
                'executor_type': e.executor_type_name,
                'callback_group_ids': []
            } for e in [exec_0, exec_1, exec_2, exec_3]
        ]

        assert exec_dicts.data == expect


class TestNodeDicts:

    def test_empty(self):
        node_dict = NodesDicts([])
        assert node_dict.data == []

    @pytest.fixture
    def create_node_mock(self, mocker,):
        def _create(
            node_id: str,
            node_name: str,
            callbacks: Optional[CallbackStructValue] = None,
            variable_passings: Optional[VariablePassingStructValue] = None,
            publishers: Optional[PublisherStructValue] = None,
            subscriptions: Optional[SubscriptionStructValue] = None,
            callback_groups: Optional[CallbackGroupStructValue] = None,
            paths: Optional[NodePathStructValue] = None,
            tf_broadcaster: Optional[TransformFrameBroadcasterStructValue] = None,
            tf_buffer: Optional[TransformFrameBufferStructValue] = None
        ):
            node_mock = mocker.Mock(spec=NodeStructValue)
            mocker.patch.object(node_mock, 'node_id', node_id)
            mocker.patch.object(node_mock, 'node_name', node_name)
            mocker.patch.object(node_mock, 'callbacks', callbacks or [])
            mocker.patch.object(node_mock, 'variable_passings', variable_passings or [])
            mocker.patch.object(node_mock, 'publishers', publishers or [])
            mocker.patch.object(node_mock, 'subscriptions', subscriptions or [])
            mocker.patch.object(node_mock, 'callback_groups', callback_groups or [])
            mocker.patch.object(node_mock, 'paths', paths or [])
            mocker.patch.object(node_mock, 'tf_broadcaster', tf_broadcaster)
            mocker.patch.object(node_mock, 'tf_buffer', tf_buffer)
            return node_mock
        return _create

    def test_callbacks(
        self,
        create_node_mock,
        setup_tf_br_mock,
        setup_tf_buff_mock,
        setup_cbs_mock,
    ):
        setup_tf_br_mock(None)
        setup_tf_buff_mock(None)
        setup_cbs_mock(['callback_dict'])

        node_info = create_node_mock(
            **{
                'node_id': 'node_id',
                'node_name': 'node_name',
                'callbacks': ['cb'],
            }
        )

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_id': 'node_id',
                'node_name': 'node_name',
                'callbacks': ['callback_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_variable_passings(
        self,
        mocker,
        create_node_mock,
        setup_tf_br_mock,
        setup_tf_buff_mock,
        setup_cbs_mock,
        setup_var_pass_mock,
    ):
        setup_tf_br_mock(None)
        setup_tf_buff_mock(None)
        setup_cbs_mock(['cb', 'cb'])
        setup_var_pass_mock(['var_pass_dict'])

        cb = mocker.Mock(spec=CallbackStructValue)
        var_pass = mocker.Mock(spec=VariablePassingStructValue)

        node_info = create_node_mock(
            **{
                'node_id': 'node_id',
                'node_name': 'node_name',
                'variable_passings': [var_pass],
                'callbacks': [cb, cb],
            }
        )

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_id': 'node_id',
                'node_name': 'node_name',
                'callbacks': ['cb', 'cb'],
                'callback_groups': [],
                'variable_passings': ['var_pass_dict'],
            }
        ]
        assert node_dict.data == expect

    def test_publish(
        self,
        mocker,
        create_node_mock,
        setup_pub_mock,
    ):
        pub_mock = mocker.Mock(spec=PublisherStructValue)
        node_info = create_node_mock(
            **{
                'node_id': 'node_id',
                'node_name': 'node',
                'publishers': [pub_mock]
            }
        )
        setup_pub_mock(['pub_dict'])

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_id': 'node_id',
                'node_name': 'node',
                'publishes': ['pub_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_subscription(
        self,
        mocker,
        create_node_mock,
        setup_sub_mock,
    ):
        sub_mock = mocker.Mock(spec=SubscriptionStructValue)
        node_mock = create_node_mock(
            **{
                'node_id': 'node_id',
                'node_name': 'node',
                'subscriptions': [sub_mock]
            }
        )
        setup_sub_mock(['sub_dict'])

        node_dict = NodesDicts([node_mock])

        expect = [
            {
                'node_id': 'node_id',
                'node_name': 'node',
                'subscribes': ['sub_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_name_sort(self, mocker, create_node_mock):
        node_0 = create_node_mock('node_id_0', 'node_name_0')
        node_1 = create_node_mock('node_id_1', 'node_name_1')
        node_2 = create_node_mock('node_id_2', 'node_name_2')
        node_3 = create_node_mock('node_id_3', 'node_name_3')

        node_dicts = NodesDicts([node_2, node_3, node_1, node_0])

        expect = [
            {
                'node_id': node.node_id,
                'node_name': node.node_name,
                'callback_groups': []
            } for node in [node_0, node_1, node_2, node_3]
        ]

        assert node_dicts.data == expect


class TestSubDicts:

    @pytest.fixture
    def create_sub_mock(self, mocker):
        def _create(topic_name: str, callback_id: str):
            sub_mock = mocker.Mock(spec=SubscriptionStructValue)
            mocker.patch.object(sub_mock, 'topic_name', topic_name)
            mocker.patch.object(sub_mock, 'callback_id', callback_id)
            return sub_mock
        return _create

    def test_single_item(self, create_sub_mock):
        sub_mock = create_sub_mock('topic_name', 'callback_id')
        sub_dict = SubDicts([sub_mock])

        expect = [{
            'topic_name': 'topic_name',
            'callback_id': 'callback_id'
        }]

        assert sub_dict.data == expect

        sub_mock = create_sub_mock('topic_name', None)
        sub_dict = SubDicts([sub_mock])

        expect = [{
            'topic_name': 'topic_name',
            'callback_id': None
        }]

        assert sub_dict.data == expect

    def test_name_sort(self, create_sub_mock):
        sub_mock_0 = create_sub_mock('A', None)
        sub_mock_1 = create_sub_mock('B', None)
        sub_mock_2 = create_sub_mock('C', None)

        sub_dict = SubDicts([sub_mock_1, sub_mock_2, sub_mock_0])

        expect = [
            {
                'topic_name': sub.topic_name,
                'callback_id': None
            } for sub in [sub_mock_0, sub_mock_1, sub_mock_2]
        ]

        assert sub_dict.data == expect


class TestPubDicts:

    @pytest.fixture
    def create_publisher(self, mocker):
        def _create(topic_name: str, callback_ids: Sequence[str]):
            publisher_mock = mocker.Mock(spec=PublisherStructValue)
            mocker.patch.object(publisher_mock, 'topic_name', topic_name)
            mocker.patch.object(publisher_mock, 'callback_ids', callback_ids)

            return publisher_mock
        return _create

    def test_single_item(self, create_publisher):
        pub_mock = create_publisher('topic_name', ['callback_id'])
        pub_dict = PubDicts([pub_mock])

        expect = [{
            'topic_name': pub_mock.topic_name,
            'callback_ids': pub_mock.callback_ids,
        }]

        assert pub_dict.data == expect

        pub_mock = create_publisher('topic_name', None)
        pub_dict = PubDicts([pub_mock])

        expect = [{
            'topic_name': pub_mock.topic_name,
            'callback_ids': [None],
        }]

        assert pub_dict.data == expect

    def test_name_sort(self, create_publisher):
        pub_mock_0 = create_publisher('A', None)
        pub_mock_1 = create_publisher('B', None)
        pub_mock_2 = create_publisher('C', None)

        pub_dict = PubDicts([pub_mock_1, pub_mock_2, pub_mock_0])

        expect = [
            {
                'topic_name': pub.topic_name,
                'callback_ids': [None]
            } for pub in [pub_mock_0, pub_mock_1, pub_mock_2]
        ]

        assert pub_dict.data == expect


class TestCallbackDicts:

    @pytest.fixture
    def patch_callback(self, mocker):
        def patch(
            callback_mock: CallbackStructValue,
            id_str: str,
            callback_name: str,
            symbol: str
        ):
            mocker.patch.object(callback_mock, 'callback_id', id_str)
            mocker.patch.object(callback_mock, 'callback_name', callback_name)
            mocker.patch.object(callback_mock, 'symbol', symbol)
        return patch

    @pytest.fixture
    def create_timer_cb_mock(self, mocker, patch_callback):
        def _create(
            id_str: str,
            symbol: str,
            callback_name: str,
            period_ns: int
        ):
            callback_mock = mocker.Mock(spec=TimerCallbackStructValue)
            patch_callback(callback_mock, id_str, callback_name, symbol)
            mocker.patch.object(callback_mock, 'period_ns', period_ns)
            return callback_mock
        return _create

    @pytest.fixture
    def create_sub_cb_mock(self, mocker, patch_callback):
        def _create(
            id_str: str,
            symbol: str,
            callback_name: str,
            topic_name: str
        ):
            callback_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)
            patch_callback(callback_mock, id_str, callback_name, symbol)
            mocker.patch.object(callback_mock, 'subscribe_topic_name', topic_name)
            return callback_mock
        return _create

    def test_timer_callback(
        self,
        create_timer_cb_mock
    ):
        callback_mock = create_timer_cb_mock('id', 'symbol', 'callback', 3)

        callback_dict = CallbackDicts((callback_mock,))

        expect = [{
            'callback_id': callback_mock.callback_id,
            'callback_name': callback_mock.callback_name,
            'callback_type': 'timer_callback',
            'period_ns': callback_mock.period_ns,
            'symbol': callback_mock.symbol
        }]

        assert callback_dict.data == expect

    def test_subscription_callback(
        self,
        create_sub_cb_mock
    ):
        callback_mock = create_sub_cb_mock('id', 'symbol', 'callback', 'topic')
        callback_dict = CallbackDicts((callback_mock,))

        expect = [{
            'callback_id': callback_mock.callback_id,
            'callback_name': callback_mock.callback_name,
            'callback_type': 'subscription_callback',
            'topic_name': callback_mock.subscribe_topic_name,
            'symbol': callback_mock.symbol
        }]

        assert callback_dict.data == expect

    def test_unsupported_type_error(self, mocker):
        callback_mock = mocker.Mock(spec=CallbackStructValue)
        with pytest.raises(UnsupportedTypeError):
            CallbackDicts((callback_mock,))

    def test_name_sort(
        self,
        create_sub_cb_mock,
    ):
        callback_mock_0 = create_sub_cb_mock('id0', 'symbol0', 'callback_0', 'A')
        callback_mock_1 = create_sub_cb_mock('id1', 'symbol1', 'callback_1', 'B')
        callback_mock_2 = create_sub_cb_mock('id2', 'symbol2', 'callback_2', 'C')

        callback_dict = CallbackDicts((callback_mock_2, callback_mock_1, callback_mock_0))

        expect = [
            {
                'callback_id': cb.callback_id,
                'callback_name': cb.callback_name,
                'callback_type': 'subscription_callback',
                'topic_name': cb.subscribe_topic_name,
                'symbol': cb.symbol
            } for cb in [callback_mock_0, callback_mock_1, callback_mock_2]
        ]

        assert callback_dict.data == expect


class TestVarPassDicts:

    @pytest.fixture
    def create_var_pass_mock(self, mocker):
        def _create(
            callback_id_write: Optional[str],
            callback_id_read: Optional[str],
        ):
            var_pass_mock = mocker.Mock(spec=VariablePassingStructValue)
            mocker.patch.object(var_pass_mock, 'callback_id_write', callback_id_write)
            mocker.patch.object(var_pass_mock, 'callback_id_read', callback_id_read)
            return var_pass_mock
        return _create

    def test_empty(self):
        var_pass_dicts = VarPassDicts([])
        expect = [{
            'callback_id_write': None,
            'callback_id_read': None,
        }]

        assert var_pass_dicts.data == expect

        var_pass_dicts = VarPassDicts(None)
        expect = [{
            'callback_id_write': None,
            'callback_id_read': None,
        }]

        assert var_pass_dicts.data == expect

    def test_full_content(self, create_var_pass_mock):
        var_pass_mock = create_var_pass_mock('id_write', 'id_read')
        var_pass_dicts = VarPassDicts([var_pass_mock])

        expect = [{
            'callback_id_write': 'id_write',
            'callback_id_read': 'id_read',
        }]

        assert var_pass_dicts.data == expect
