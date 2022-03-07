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

# from threading import Timer
from caret_analyze.architecture import Architecture
from caret_analyze.exceptions import UnsupportedTypeError
from caret_analyze.infra.interface import RecordsProvider
from caret_analyze.runtime.callback import (CallbackBase, SubscriptionCallback,
                                            TimerCallback)
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.executor import CallbackGroup, Executor
from caret_analyze.runtime.node import Node
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.runtime.path import Path
from caret_analyze.runtime.publisher import Publisher
from caret_analyze.runtime.runtime_loaded import (CallbackGroupsLoaded,
                                                  CallbacksLoaded,
                                                  CommunicationsLoaded,
                                                  ExecutorsLoaded,
                                                  NodePathsLoaded, NodesLoaded,
                                                  PathsLoaded,
                                                  PublishersLoaded,
                                                  RuntimeLoaded,
                                                  SubscriptionsLoaded, TimersLoaded,
                                                  VariablePassingsLoaded)
from caret_analyze.runtime.subscription import Subscription
from caret_analyze.runtime.timer import Timer
from caret_analyze.runtime.variable_passing import VariablePassing
from caret_analyze.value_objects import (CallbackGroupStructValue,
                                         CommunicationStructValue,
                                         ExecutorStructValue, ExecutorType,
                                         NodePathStructValue, NodeStructValue,
                                         PathStructValue, PublisherStructValue,
                                         SubscriptionCallbackStructValue,
                                         SubscriptionStructValue,
                                         TimerCallbackStructValue,
                                         TimerStructValue,
                                         VariablePassingStructValue)

import pytest
from pytest_mock import MockerFixture


class TestRuntimeLoaded:

    def test_empty_architecture(self, mocker: MockerFixture):
        arch = mocker.Mock(spec=Architecture)
        mocker.patch.object(arch, 'nodes', ())
        mocker.patch.object(arch, 'communications', ())
        mocker.patch.object(arch, 'paths', ())
        mocker.patch.object(arch, 'executors', ())

        provider_mock = mocker.Mock(spec=RecordsProvider)
        loaded = RuntimeLoaded(arch, provider_mock)

        assert loaded.nodes == []
        assert loaded.executors == []
        assert loaded.communications == []
        assert loaded.paths == []

    def test_full_architecture(self, mocker: MockerFixture):
        node_info_mock = mocker.Mock(spec=NodePathStructValue)
        comm_info_mock = mocker.Mock(spec=CommunicationStructValue)
        path_info_mock = mocker.Mock(spec=PathStructValue)
        exec_info_mock = mocker.Mock(spec=ExecutorStructValue)

        arch = mocker.Mock(spec=Architecture)
        mocker.patch.object(arch, 'nodes', (node_info_mock,))
        mocker.patch.object(arch, 'communications', (comm_info_mock,))
        mocker.patch.object(arch, 'paths', (path_info_mock,))
        mocker.patch.object(arch, 'executors', (exec_info_mock,))

        node_mock = mocker.Mock(spec=Node)
        comm_mock = mocker.Mock(spec=Communication)
        path_mock = mocker.Mock(spec=Path)
        exec_mock = mocker.Mock(spec=Executor)

        node_loaded_mock = mocker.Mock(spec=NodesLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.NodesLoaded',
                     return_value=node_loaded_mock)
        mocker.patch.object(node_loaded_mock, 'data', [node_mock])

        exec_loaded_mock = mocker.Mock(spec=ExecutorsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.ExecutorsLoaded',
                     return_value=exec_loaded_mock)
        mocker.patch.object(exec_loaded_mock, 'data', [exec_mock])

        comm_loaded_mock = mocker.Mock(spec=CommunicationsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.CommunicationsLoaded',
                     return_value=comm_loaded_mock)
        mocker.patch.object(comm_loaded_mock, 'data', [comm_mock])

        path_loaded_mock = mocker.Mock(spec=PathsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.PathsLoaded',
                     return_value=path_loaded_mock)
        mocker.patch.object(path_loaded_mock, 'data', [path_mock])

        provider_mock = mocker.Mock(spec=RecordsProvider)
        loaded = RuntimeLoaded(arch, provider_mock)

        assert loaded.nodes == [node_mock]
        assert loaded.executors == [exec_mock]
        assert loaded.communications == [comm_mock]
        assert loaded.paths == [path_mock]


class TestNodesLoaded:

    def test_empty(self, mocker: MockerFixture):
        node_info_mock = mocker.Mock(spec=NodeStructValue)

        provider_mock = mocker.Mock(spec=RecordsProvider)

        node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(NodesLoaded, '_to_runtime', return_value=node_mock)
        loaded = NodesLoaded((node_info_mock,), provider_mock)

        nodes = loaded.data
        assert nodes == [node_mock]

    def test_to_runtime_optional_none(self, mocker: MockerFixture):
        node_info_mock = mocker.Mock(spec=NodeStructValue)
        mocker.patch.object(node_info_mock, 'node_name', 'node')
        mocker.patch.object(node_info_mock, 'callback_groups', None)
        mocker.patch.object(node_info_mock, 'paths', ())
        mocker.patch.object(node_info_mock, 'variable_passings', None)
        mocker.patch.object(node_info_mock, 'timers', ())
        mocker.patch.object(node_info_mock, 'publishers', ())
        mocker.patch.object(node_info_mock, 'subscriptions', ())

        node_paths_loaded_mock = mocker.Mock(spec=NodePathsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.NodePathsLoaded',
                     return_value=node_paths_loaded_mock)

        mocker.patch.object(node_paths_loaded_mock, 'data', [])

        provider_mock = mocker.Mock(spec=RecordsProvider)
        node = NodesLoaded._to_runtime(node_info_mock, provider_mock)

        assert node.node_name == 'node'
        assert node.callbacks == []
        assert node.timers == []
        assert node.publishers == []
        assert node.subscriptions == []
        assert node.variable_passings == []
        assert node.paths == []

    def test_to_runtime_full(self, mocker: MockerFixture):
        node_info_mock = mocker.Mock(spec=NodeStructValue)

        cbg_info_mock = mocker.Mock(spec=CallbackGroupStructValue)
        var_pass_info_mock = mocker.Mock(spec=VariablePassingStructValue)
        timer_info_mock = mocker.Mock(spec=TimerStructValue)
        pub_info_mock = mocker.Mock(spec=PublisherStructValue)
        sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)

        mocker.patch.object(node_info_mock, 'node_name', 'node')
        mocker.patch.object(node_info_mock, 'callback_groups', (cbg_info_mock))
        mocker.patch.object(node_info_mock, 'paths', ())
        mocker.patch.object(node_info_mock, 'variable_passings', (var_pass_info_mock))
        mocker.patch.object(node_info_mock, 'timers', (timer_info_mock,))
        mocker.patch.object(node_info_mock, 'publishers', (pub_info_mock,))
        mocker.patch.object(node_info_mock, 'subscriptions', (sub_info_mock,))

        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.CallbackGroupsLoaded',
                     return_value=cbg_loaded_mock)

        node_paths_loaded_mock = mocker.Mock(spec=NodePathsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.NodePathsLoaded',
                     return_value=node_paths_loaded_mock)

        var_passes_loaded_mock = mocker.Mock(spec=VariablePassingsLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.VariablePassingsLoaded',
                     return_value=var_passes_loaded_mock)

        cbg_mock = mocker.Mock(spec=CallbackGroup)
        node_path_mock = mocker.Mock(spec=NodePath)
        var_pass_mock = mocker.Mock(spec=NodePath)
        mocker.patch.object(cbg_mock, 'callbacks', ())
        mocker.patch.object(cbg_loaded_mock, 'data', [cbg_mock])
        mocker.patch.object(node_paths_loaded_mock, 'data', [node_path_mock])
        mocker.patch.object(var_passes_loaded_mock, 'data', [var_pass_mock])

        pub_mock = mocker.Mock(spec=Publisher)
        pub_loaded = mocker.Mock(spec=PublishersLoaded)
        mocker.patch.object(pub_loaded, 'data', [pub_mock])
        mocker.patch('caret_analyze.runtime.runtime_loaded.PublishersLoaded',
                     return_value=pub_loaded)

        timer_mock = mocker.Mock(spec=Timer)
        timer_loaded = mocker.Mock(spec=TimersLoaded)
        mocker.patch.object(timer_loaded, 'data', [timer_mock])
        mocker.patch('caret_analyze.runtime.runtime_loaded.TimersLoaded',
                     return_value=timer_loaded)

        sub_mock = mocker.Mock(spec=Subscription)
        sub_loaded = mocker.Mock(spec=SubscriptionsLoaded)
        mocker.patch.object(sub_loaded, 'data', [sub_mock])
        mocker.patch('caret_analyze.runtime.runtime_loaded.SubscriptionsLoaded',
                     return_value=sub_loaded)

        provider_mock = mocker.Mock(spec=RecordsProvider)
        node = NodesLoaded._to_runtime(node_info_mock, provider_mock)

        assert node.node_name == 'node'
        assert node.callback_groups == [cbg_mock]
        assert node.publishers == [pub_mock]
        assert node.subscriptions == [sub_mock]
        assert node.timers == [timer_mock]
        assert node.variable_passings == [var_pass_mock]
        assert node.paths == [node_path_mock]


class TestPublishsersLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        loaded = PublishersLoaded((), provider_mock)

        assert loaded.data == []

    def test_full(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_info_mock = mocker.Mock(spec=PublisherStructValue)

        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(PublishersLoaded, '_to_runtime', return_value=pub_mock)

        loaded = PublishersLoaded((pub_info_mock,), provider_mock)

        assert loaded.data == [pub_mock]

    def test_get_publishers(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_info_mock = mocker.Mock(spec=PublisherStructValue)

        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_mock, 'callback_names', ['callback'])
        mocker.patch.object(pub_mock, 'node_name', 'node_name')
        mocker.patch.object(pub_mock, 'topic_name', 'topic_name')
        mocker.patch.object(PublishersLoaded, '_to_runtime', return_value=pub_mock)

        loaded = PublishersLoaded((pub_info_mock,), provider_mock)

        loaded.get_publishers(None, None, None) == [pub_info_mock]
        assert loaded.get_publishers(
            'node_name',
            'callback',
            'topic_name'
        ) == [pub_mock]

        assert loaded.get_publishers('not_exist', None, None) == []


class TestSubscriptionsLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        loaded = SubscriptionsLoaded((), provider_mock)

        assert loaded.data == []

    def test_full(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)

        sub_mock = mocker.Mock(spec=Subscription)
        mocker.patch.object(SubscriptionsLoaded, '_to_runtime', return_value=sub_mock)

        loaded = SubscriptionsLoaded((sub_info_mock,), provider_mock)

        assert loaded.data == [sub_mock]

    def test_get_subscriptions(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)

        sub_mock = mocker.Mock(spec=Subscription)
        mocker.patch.object(sub_mock, 'callback_name', 'callback')
        mocker.patch.object(sub_mock, 'node_name', 'node_name')
        mocker.patch.object(sub_mock, 'topic_name', 'topic_name')
        mocker.patch.object(SubscriptionsLoaded, '_to_runtime', return_value=sub_mock)

        loaded = SubscriptionsLoaded((sub_info_mock,), provider_mock)

        loaded.get_subscriptions(None, None, None) == [sub_info_mock]
        assert loaded.get_subscriptions(
            callback_name='callback',
            node_name='node_name',
            topic_name='topic_name'
        ) == [sub_mock]

        assert loaded.get_subscriptions('not_exist', None, None) == []


class TestExecutorLoaded:

    def test_empty(self, mocker: MockerFixture):
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        loaded = ExecutorsLoaded((), nodes_loaded_mock)
        assert loaded.data == []

    def test_to_runtime_data_empty_callback_group(self, mocker: MockerFixture):
        exec_info_mock = mocker.Mock(spec=ExecutorStructValue)

        mocker.patch.object(exec_info_mock, 'callback_group_names', ())
        mocker.patch.object(exec_info_mock, 'executor_type',
                            ExecutorType.SINGLE_THREADED_EXECUTOR)
        mocker.patch.object(exec_info_mock, 'executor_name', 'exec_name')

        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        executor = ExecutorsLoaded._to_runtime(exec_info_mock, nodes_loaded_mock)

        assert executor.callback_groups == []
        assert executor.executor_type == ExecutorType.SINGLE_THREADED_EXECUTOR
        assert executor.callbacks == []
        assert executor.executor_name == 'exec_name'

    def test_to_runtime_data_callback_group(self, mocker: MockerFixture):
        exec_info_mock = mocker.Mock(spec=ExecutorStructValue)

        cbg_mock = mocker.Mock(spec=CallbackGroup)

        mocker.patch.object(cbg_mock, 'callback_group_name', 'cbg_name')

        mocker.patch.object(exec_info_mock, 'callback_group_names', ['cbg_name'])
        mocker.patch.object(exec_info_mock, 'executor_type',
                            ExecutorType.SINGLE_THREADED_EXECUTOR)
        mocker.patch.object(exec_info_mock, 'executor_name', 'exec_name')

        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'find_callback_group', return_value=cbg_mock)
        executor = ExecutorsLoaded._to_runtime(exec_info_mock, nodes_loaded_mock)

        assert executor.callback_groups == [cbg_mock]
        assert executor.executor_type == ExecutorType.SINGLE_THREADED_EXECUTOR
        assert executor.executor_name == 'exec_name'


class TestPathsLoaded:

    def test_empty(self, mocker: MockerFixture):
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        comms_loaded_mock = mocker.Mock(spec=CommunicationsLoaded)
        loaded = PathsLoaded((), nodes_loaded_mock, comms_loaded_mock)

        assert loaded.data == []

    def test_to_runtime_empty(self, mocker: MockerFixture):
        path_info_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_info_mock, 'child', [])

        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        comms_loaded_mock = mocker.Mock(spec=CommunicationsLoaded)
        path = PathsLoaded._to_runtime(path_info_mock, nodes_loaded_mock, comms_loaded_mock)

        assert path.communications == []
        assert path.node_paths == []
        assert path.child == []

    def test_valid_path(self, mocker: MockerFixture):
        path_info_mock = mocker.Mock(spec=PathStructValue)

        node_path_info_mock = mocker.Mock(spec=NodePathStructValue)
        comm_info_mock = mocker.Mock(spec=CommunicationStructValue)

        mocker.patch.object(node_path_info_mock, 'callbacks', None)
        mocker.patch.object(path_info_mock, 'child', [node_path_info_mock, comm_info_mock])

        node_path_mock = mocker.Mock(NodePath)
        comm_mock = mocker.Mock(Communication)

        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        comms_loaded_mock = mocker.Mock(spec=CommunicationsLoaded)

        mocker.patch.object(nodes_loaded_mock, 'find_node_path', return_value=node_path_mock)
        mocker.patch.object(comms_loaded_mock, 'find_communication', return_value=comm_mock)

        path = PathsLoaded._to_runtime(path_info_mock, nodes_loaded_mock, comms_loaded_mock)

        assert path.child == [node_path_mock, comm_mock]

    def test_unsupported_error(self, mocker: MockerFixture):
        path_info_mock = mocker.Mock(spec=PathStructValue)
        mocker.patch.object(path_info_mock, 'child', [None])

        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)
        comms_loaded_mock = mocker.Mock(spec=CommunicationsLoaded)
        with pytest.raises(UnsupportedTypeError):
            PathsLoaded._to_runtime(path_info_mock, nodes_loaded_mock, comms_loaded_mock)


class TestCommunicationsLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)

        loaded = CommunicationsLoaded((), provider_mock, nodes_loaded_mock)

        assert loaded.data == []

    def test_single_comm(self, mocker: MockerFixture):
        comm_info_mock = mocker.Mock(spec=CommunicationStructValue)
        provider_mock = mocker.Mock(spec=RecordsProvider)
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)

        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(CommunicationsLoaded, '_to_runtime', return_value=comm_mock)
        loaded = CommunicationsLoaded((comm_info_mock,), provider_mock, nodes_loaded_mock)

        assert loaded.data == [comm_mock]

    def test_to_runtime(self, mocker: MockerFixture):
        comm_info_mock = mocker.Mock(spec=CommunicationStructValue)
        provider_mock = mocker.Mock(spec=RecordsProvider)
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)

        topic_name = '/topic'
        pub_node_name = '/pub_node'
        sub_node_name = '/sub_node'

        node_pub_mock = mocker.Mock(spec=Node)
        node_sub_mock = mocker.Mock(spec=Node)

        mocker.patch.object(comm_info_mock, 'topic_name', topic_name)
        mocker.patch.object(comm_info_mock, 'publish_node_name', pub_node_name)
        mocker.patch.object(comm_info_mock, 'subscribe_node_name', sub_node_name)

        mocker.patch.object(comm_info_mock, 'subscribe_callback_name', None)
        mocker.patch.object(comm_info_mock, 'publish_callback_names', None)

        def find_node(node_name: str):
            if node_name == pub_node_name:
                return node_pub_mock
            else:
                return node_sub_mock

        mocker.patch.object(nodes_loaded_mock, 'find_node', side_effect=find_node)

        sub_mock = mocker.Mock(Subscription)
        pub_mock = mocker.Mock(Publisher)
        mocker.patch.object(node_sub_mock, 'get_subscription', return_value=sub_mock)
        mocker.patch.object(node_pub_mock, 'get_publisher', return_value=pub_mock)

        comm = CommunicationsLoaded._to_runtime(
            comm_info_mock, provider_mock, nodes_loaded_mock)

        assert comm.callback_publish is None
        assert comm.callback_subscription is None
        assert comm.rmw_implementation is None
        assert comm.is_intra_proc_comm is None
        assert comm.subscribe_node_name == sub_node_name
        assert comm.publish_node_name == pub_node_name
        assert comm.topic_name == topic_name
        assert comm.publisher == pub_mock
        assert comm.subscription == sub_mock

    def test_to_runtime_with_callback(self, mocker: MockerFixture):
        comm_info_mock = mocker.Mock(spec=CommunicationStructValue)
        provider_mock = mocker.Mock(spec=RecordsProvider)
        nodes_loaded_mock = mocker.Mock(spec=NodesLoaded)

        topic_name = '/topic'
        pub_node_name = '/pub_node'
        sub_node_name = '/sub_node'
        pub_cb_name = 'pub_cb'
        sub_cb_name = 'sub_cb'

        node_pub_mock = mocker.Mock(spec=Node)
        node_sub_mock = mocker.Mock(spec=Node)

        mocker.patch.object(comm_info_mock, 'topic_name', topic_name)
        mocker.patch.object(comm_info_mock, 'publish_node_name', pub_node_name)
        mocker.patch.object(comm_info_mock, 'subscribe_node_name', sub_node_name)

        mocker.patch.object(comm_info_mock, 'subscribe_callback_name', sub_cb_name)
        mocker.patch.object(comm_info_mock, 'publish_callback_names', [pub_cb_name])

        def find_node(node_name: str):
            if node_name == pub_node_name:
                return node_pub_mock
            return node_sub_mock

        cb_sub_mock = mocker.Mock(spec=CallbackBase)
        cb_pub_mock = mocker.Mock(spec=CallbackBase)

        def find_callback(callback_name: str):
            if callback_name == sub_cb_name:
                return cb_sub_mock
            return cb_pub_mock

        mocker.patch.object(nodes_loaded_mock, 'find_node', side_effect=find_node)
        mocker.patch.object(nodes_loaded_mock, 'find_callback', side_effect=find_callback)

        sub_mock = mocker.Mock(Subscription)
        pub_mock = mocker.Mock(Publisher)
        mocker.patch.object(node_sub_mock, 'get_subscription', return_value=sub_mock)
        mocker.patch.object(node_pub_mock, 'get_publisher', return_value=pub_mock)

        comm = CommunicationsLoaded._to_runtime(
            comm_info_mock, provider_mock, nodes_loaded_mock)

        assert comm.callback_publish == [cb_pub_mock]
        assert comm.callback_subscription == cb_sub_mock


class TestCallbacksLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        loaded = CallbacksLoaded(
            (), provider_mock, pub_loaded_mock, sub_loaded_mock, timer_loaded_mock)

        assert loaded.data == []

    def test_full(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        cb_info_mock = mocker.Mock(spec=TimerCallbackStructValue)
        cb_mock = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(CallbacksLoaded, '_to_runtime', return_value=cb_mock)

        loaded = CallbacksLoaded(
            (cb_info_mock,), provider_mock, pub_loaded_mock, sub_loaded_mock, timer_loaded_mock)

        assert loaded.data == [cb_mock]

    def test_to_runtime_timer_callback(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        node_name = '/node'
        period_ns = 3
        pub_topic_name = '/pub'
        cb_name = '/callback'
        symbol = 'symbol'

        cb_info = TimerCallbackStructValue(
            node_name, symbol, period_ns, (pub_topic_name,), cb_name
        )

        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_loaded_mock, 'get_publishers', return_value=[pub_mock])

        cb = CallbacksLoaded._to_runtime(
            cb_info, provider_mock, pub_loaded_mock, sub_loaded_mock, timer_loaded_mock)

        assert isinstance(cb, TimerCallback)
        assert cb.callback_name == cb_name
        assert cb.node_name == node_name
        assert cb.period_ns == period_ns
        assert cb.symbol == symbol
        assert cb.publishers == [pub_mock]
        assert cb.publish_topic_names == [pub_topic_name]

    def test_to_runtime_subscription_callback(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        node_name = '/node'
        pub_topic = '/pub_topic'
        sub_topic = '/sub_topic'
        cb_name = '/callback'
        symbol = 'symbol'

        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_loaded_mock, 'get_publishers', return_value=[pub_mock])

        sub_mock = mocker.Mock(spec=Subscription)
        mocker.patch.object(sub_loaded_mock, 'get_subscription', return_value=sub_mock)

        cb_info = SubscriptionCallbackStructValue(
            node_name, symbol, sub_topic, (pub_topic,), cb_name
        )

        cb = CallbacksLoaded._to_runtime(
            cb_info, provider_mock, pub_loaded_mock, sub_loaded_mock, timer_loaded_mock)

        assert isinstance(cb, SubscriptionCallback)
        assert cb.callback_name == cb_name
        assert cb.node_name == node_name
        assert cb.publishers == [pub_mock]
        assert cb.subscription == sub_mock
        assert cb.subscribe_topic_name == sub_topic
        assert cb.symbol == symbol
        assert cb.publish_topic_names == [pub_topic]


class TestCallbackGroupsLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spedc=RecordsProvider)
        publisher_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        cbgs = CallbackGroupsLoaded(
            (), provider_mock, publisher_loaded_mock, sub_loaded_mock, timer_loaded_mock).data
        assert cbgs == []

    def test_to_runtime(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spedc=RecordsProvider)
        cbg_info_mock = mocker.Mock(spec=CallbackGroupStructValue)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        timer_loaded_mock = mocker.Mock(spec=TimersLoaded)

        cbg_name = 'cbg'

        mocker.patch.object(cbg_info_mock, 'callback_group_name', cbg_name)

        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch('caret_analyze.runtime.runtime_loaded.CallbacksLoaded',
                     return_value=cb_loaded_mock)

        cb_mock = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(cb_loaded_mock, 'data', [cb_mock])

        cbg = CallbackGroupsLoaded._to_runtime(
            cbg_info_mock, provider_mock, pub_loaded_mock, sub_loaded_mock, timer_loaded_mock)

        assert cbg.callbacks == [cb_mock]
        assert cbg.callback_group_name == cbg_name


class TestNodePathsLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)
        loaded = NodePathsLoaded((), pub_loaded_mock, sub_loaded_mock, provider_mock, [])
        assert loaded.data == []

    def test_to_runtime(self, mocker: MockerFixture):
        node_path_info_mock = mocker.Mock(spec=NodePathStructValue)
        provider_mock = mocker.Mock(spec=RecordsProvider)
        pub_loaded_mock = mocker.Mock(spec=PublishersLoaded)
        sub_loaded_mock = mocker.Mock(spec=SubscriptionsLoaded)

        node_name = '/node'

        pub_mock = mocker.Mock(spec=Publisher)
        sub_mock = mocker.Mock(spec=Subscription)

        mocker.patch.object(pub_loaded_mock, 'get_publisher', return_value=pub_mock)
        mocker.patch.object(sub_loaded_mock, 'get_subscription', return_value=sub_mock)

        mocker.patch.object(node_path_info_mock, 'node_name', node_name)

        node_path = NodePathsLoaded._to_runtime(
            node_path_info_mock, provider_mock, pub_loaded_mock, sub_loaded_mock, []
        )

        assert node_path.node_name == node_name
        assert node_path.publisher == pub_mock
        assert node_path.subscription == sub_mock


class TestVariablePassingsLoaded:

    def test_empty(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=MockerFixture)
        loaded = VariablePassingsLoaded((), provider_mock)
        assert loaded.data == []

    def test_single_instance(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=MockerFixture)
        var_pass_info_mock = mocker.Mock(spec=VariablePassingStructValue)

        var_pass_mock = mocker.Mock(spec=VariablePassing)
        mocker.patch.object(VariablePassingsLoaded, '_to_runtime', return_value=var_pass_mock)

        loaded = VariablePassingsLoaded((var_pass_info_mock,), provider_mock)

        assert loaded.data == [var_pass_mock]

    def test_to_runtime(self, mocker: MockerFixture):
        provider_mock = mocker.Mock(spec=MockerFixture)
        var_pass_info_mock = mocker.Mock(spec=VariablePassingStructValue)

        node_name = '/node'
        mocker.patch.object(var_pass_info_mock, 'node_name', node_name)

        var_pass = VariablePassingsLoaded._to_runtime(var_pass_info_mock, provider_mock)

        assert isinstance(var_pass, VariablePassing)
        assert var_pass.node_name == node_name
