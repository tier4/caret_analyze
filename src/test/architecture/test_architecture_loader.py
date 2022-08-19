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

from caret_analyze.architecture.architecture_loaded import (ArchitectureLoaded,
                                                            CallbackGroupsLoaded,
                                                            CallbackPathSearched,
                                                            CallbacksLoaded,
                                                            CommValuesLoaded,
                                                            ExecutorValuesLoaded,
                                                            NodePathCreated,
                                                            NodeValuesLoaded,
                                                            PathValuesLoaded,
                                                            PublishersLoaded,
                                                            SubscriptionsLoaded,
                                                            TimersLoaded,
                                                            TopicIgnoredReader,
                                                            VariablePassingsLoaded)
from caret_analyze.architecture.graph_search import CallbackPathSearcher
from caret_analyze.architecture.reader_interface import ArchitectureReader
from caret_analyze.common import Util
from caret_analyze.exceptions import (InvalidReaderError, ItemNotFoundError,
                                      UnsupportedTypeError)
from caret_analyze.value_objects import (CallbackGroupValue, CallbackGroupType,
                                         CallbackValue, CallbackType,
                                         ExecutorValue,  ExecutorType,
                                         NodePathValue,
                                         NodeValue, PathValue,
                                         PublisherValue,
                                         SubscriptionCallbackValue,
                                         SubscriptionValue, TimerCallbackValue,
                                         VariablePassingValue)
from caret_analyze.architecture.struct import (NodeStruct,
                      CallbackGroupStruct,
                      CallbackStruct,
                      CommunicationStruct,
                      ExecutorStruct,
                      NodePathStruct,
                      PathStruct,
                      PublisherStruct,
                      SubscriptionStruct,
                      SubscriptionCallbackStruct,
                      TimerCallbackStruct,
                      TimerStruct,
                      VariablePassingStruct,
                      )

from caret_analyze.value_objects.message_context import MessageContextType
from caret_analyze.value_objects.node import NodeValueWithId

import pytest


class TestArchitectureLoaded:

    def test_empty_reader(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(
            reader_mock, 'get_paths', return_value=[])
        mocker.patch.object(reader_mock, 'get_executors', return_value=[])
        mocker.patch.object(reader_mock, 'get_nodes', return_value=[])

        node_loaded = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded = mocker.Mock(spec=CommValuesLoaded)
        exec_loaded = mocker.Mock(spec=ExecutorValuesLoaded)
        path_loaded = mocker.Mock(spec=PathValuesLoaded)

        mocker.patch('caret_analyze.architecture.architecture_loaded.NodeValuesLoaded',
                     return_value=node_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CommValuesLoaded',
                     return_value=comm_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ExecutorValuesLoaded',
                     return_value=exec_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.PathValuesLoaded',
                     return_value=path_loaded)

        mocker.patch.object(node_loaded, 'data', [])
        mocker.patch.object(comm_loaded, 'data', [])
        mocker.patch.object(exec_loaded, 'data', [])
        mocker.patch.object(path_loaded, 'data', [])

        arch = ArchitectureLoaded(reader_mock, [])
        assert len(arch.paths) == 0
        assert len(arch.executors) == 0
        assert len(arch.nodes) == 0
        assert len(arch.communications) == 0

    def test_get_data(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        path_mock = mocker.Mock(spec=PathStruct)
        executor_mock = mocker.Mock(spec=ExecutorValue)
        node_mock = mocker.Mock(spec=NodeStruct)
        comm_mock = mocker.Mock(spec=CommunicationStruct)
        pub_mock = mocker.Mock(spec=PublisherValue)
        sub_mock = mocker.Mock(spec=SubscriptionValue)
        sub_mock_ = mocker.Mock(spec=SubscriptionValue)

        mocker.patch.object(pub_mock, 'topic_name', '/chatter')
        mocker.patch.object(sub_mock, 'topic_name', '/chatter')
        mocker.patch.object(sub_mock_, 'topic_name', '/chatter2')
        mocker.patch.object(node_mock, 'publishers', [pub_mock])
        mocker.patch.object(node_mock, 'subscriptions', [sub_mock, sub_mock_])

        mocker.patch.object(reader_mock, 'get_paths', return_value=[path_mock])
        mocker.patch.object(reader_mock, 'get_executors', return_value=[executor_mock])
        mocker.patch.object(reader_mock, 'get_nodes', return_value=[NodeValue('node', 'node')])

        node_loaded = mocker.Mock(spec=NodeValuesLoaded)
        executor_loaded = mocker.Mock(spec=ExecutorValuesLoaded)
        path_loaded = mocker.Mock(spec=PathValuesLoaded)
        comm_loaded = mocker.Mock(spec=CommValuesLoaded)

        mocker.patch('caret_analyze.architecture.architecture_loaded.NodeValuesLoaded',
                     return_value=node_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CommValuesLoaded',
                     return_value=comm_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ExecutorValuesLoaded',
                     return_value=executor_loaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.PathValuesLoaded',
                     return_value=path_loaded)

        mocker.patch.object(node_loaded, 'data', (node_mock,))
        mocker.patch.object(comm_loaded, 'data', (comm_mock,))
        mocker.patch.object(executor_loaded, 'data', (executor_mock,))
        mocker.patch.object(path_loaded, 'data', (path_mock,))

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)
        arch = ArchitectureLoaded(reader_mock, [])
        assert len(arch.paths) == 1
        assert len(arch.executors) == 1
        assert len(arch.nodes) == 1
        assert len(arch.communications) == 1


class TestNodesInfoLoaded():

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)
        mocker.patch.object(
            reader_mock, 'get_subscriptions', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_nodes', return_value=[])

        loader = NodeValuesLoaded(reader_mock)
        nodes = loader.data
        assert len(nodes) == 0

    def test_single_node(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        node = NodeValue('node', 'node')
        mocker.patch.object(reader_mock, 'get_nodes',
                            return_value=[node])

        loader = NodeValuesLoaded(reader_mock)
        nodes = loader.data
        assert nodes == (node_mock,)

    def test_name_sort(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        node_a = NodeValue('a', 'a')
        node_b = NodeValue('b', 'b')
        node_c = NodeValue('c', 'c')

        mocker.patch.object(
            reader_mock, 'get_nodes', return_value=[node_b, node_c, node_a])

        def create_node(node, reader):
            node_mock = mocker.Mock(spec=NodeStruct)
            cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
            cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
            mocker.patch.object(node_mock, 'node_name', node.node_name)
            return node_mock, cb_loaded_mock, cbg_loaded_mock

        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            side_effect=create_node)

        loader = NodeValuesLoaded(reader_mock)
        nodes = loader.data
        assert nodes[0].node_name == 'a'
        assert nodes[1].node_name == 'b'
        assert nodes[2].node_name == 'c'

    def test_create_node_empty(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)
        mocker.patch.object(
            reader_mock, 'get_subscriptions', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timers', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_message_contexts', return_value=[])

        path_searched = mocker.Mock(spec=CallbackPathSearched)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearched',
                     return_value=path_searched)
        mocker.patch.object(path_searched, 'data', ())

        cbg_loaded = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackGroupsLoaded',
                     return_value=cbg_loaded)
        mocker.patch.object(cbg_loaded, 'data', ())

        var_pass_loaded = mocker.Mock(spec=VariablePassingsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.VariablePassingsLoaded',
                     return_value=var_pass_loaded)
        mocker.patch.object(var_pass_loaded, 'data', ())

        cbs_loaded = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbacksLoaded',
                     return_value=cbs_loaded)
        mocker.patch.object(cbs_loaded, 'data', ())

        node_value = NodeValue('node', 'node')
        node, _, _ = NodeValuesLoaded._create_node(node_value, reader_mock)

        assert node.node_name == 'node'
        assert node.publishers == ()
        assert node.subscriptions == ()
        assert node.callbacks == ()
        assert node.callback_groups == ()
        assert node.paths == ()
        assert node.variable_passings == ()

    def test_create_node_full(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)
        cbg = mocker.Mock(spec=CallbackGroupStruct)
        callback = mocker.Mock(spec=CallbackStruct)
        subscription = mocker.Mock(spec=SubscriptionStruct)
        timer = mocker.Mock(spec=TimerStruct)
        publisher = mocker.Mock(spec=PublisherStruct)
        var_pass = mocker.Mock(spec=VariablePassingStruct)
        path = mocker.Mock(spec=NodePathStruct)
        path_ = mocker.Mock(spec=NodePathStruct)
        context = {
            'context_type': MessageContextType.CALLBACK_CHAIN.type_name,
            'publisher_topic_name': 'UNDEFINED',
            'subscription_topic_name': 'UNDEFINED',
        }

        mocker.patch.object(cbg, 'callbacks', (callback,))

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)
        mocker.patch.object(
            reader_mock, 'get_subscriptions', return_value=[subscription])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[publisher])
        mocker.patch.object(
            reader_mock, 'get_timers', return_value=[timer])

        mocker.patch.object(
            reader_mock, 'get_message_contexts', return_value=[context])

        path_searched = mocker.Mock(spec=CallbackPathSearched)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearched',
                     return_value=path_searched)
        mocker.patch.object(path_searched, 'data', (path,))

        path_created = mocker.Mock(spec=NodePathCreated)
        mocker.patch('caret_analyze.architecture.architecture_loaded.NodePathCreated',
                     return_value=path_created)
        mocker.patch.object(path_created, 'data', (path_,))

        cbg_loaded = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackGroupsLoaded',
                     return_value=cbg_loaded)
        mocker.patch.object(cbg_loaded, 'data', (cbg,))

        var_pass_loaded = mocker.Mock(spec=VariablePassingsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.VariablePassingsLoaded',
                     return_value=var_pass_loaded)
        mocker.patch.object(var_pass_loaded, 'data', (var_pass,))

        cbs_loaded = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbacksLoaded',
                     return_value=cbs_loaded)
        mocker.patch.object(cbs_loaded, 'data', ())

        publishers_loaded = mocker.Mock(spec=PublishersLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.PublishersLoaded',
                     return_value=publishers_loaded)
        mocker.patch.object(publishers_loaded, 'data', (publisher,))

        subscriptions_loaded = mocker.Mock(spec=SubscriptionsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.SubscriptionsLoaded',
                     return_value=subscriptions_loaded)
        mocker.patch.object(subscriptions_loaded, 'data', (subscription,))
        timers_loaded = mocker.Mock(spec=TimersLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.TimersLoaded',
                     return_value=timers_loaded)
        mocker.patch.object(timers_loaded, 'data', (timer,))

        def assigned(node_paths, message_contexts):
            return node_paths

        mocker.patch.object(
            NodeValuesLoaded, '_message_context_assigned', side_effect=assigned)

        mocker.patch.object(
            NodeValuesLoaded, '_search_node_paths', return_value=[path, path_])

        node_value = NodeValue('node', 'node')
        node, _, _ = NodeValuesLoaded._create_node(node_value, reader_mock)

        assert node.node_name == 'node'
        assert node.publishers == (publisher,)
        assert node.timers == (timer,)
        assert node.subscriptions == (subscription,)
        assert node.callbacks == (callback,)
        assert node.callback_groups == (cbg,)
        assert node.paths == (path, path_)
        assert node.variable_passings == (var_pass,)

    def test_find_callback(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        cb_mock = mocker.Mock(spec=CallbackStruct)

        mocker.patch.object(cb_loaded_mock, 'find_callback', return_value=cb_mock)
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))
        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes',
                            return_value=[node])

        loaded = NodeValuesLoaded(reader_mock)
        assert loaded.find_callback('callback_id') == cb_mock

        mocker.patch.object(cb_loaded_mock, 'find_callback',
                            side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            loaded.find_callback('callback_id')

    def test_find_node(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node_value = NodeValue('node_name', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=(node_value,))
        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        nodes_loaded = NodeValuesLoaded(reader_mock)

        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(Util, 'find_one', return_value=node_mock)
        node = nodes_loaded.find_node('node')
        assert node == node_mock

        mocker.patch.object(
            Util, 'find_one', side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            nodes_loaded.find_node('node')

    def test_find_node_path(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=(node,))

        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        nodes_loaded = NodeValuesLoaded(reader_mock)

        mocker.patch.object(nodes_loaded, 'find_node',
                            return_value=node_mock)
        node_path_info_mock = mocker.Mock(spec=NodePathValue)

        node_path_mock = mocker.Mock(spec=NodePathStruct)

        mocker.patch.object(node_path_info_mock,
                            'publish_topic_name', 'pub_topic')
        mocker.patch.object(node_path_info_mock,
                            'subscribe_topic_name', 'sub_topic')
        mocker.patch.object(node_path_mock, 'publish_topic_name', 'pub_topic')
        mocker.patch.object(
            node_path_mock, 'subscribe_topic_name', 'sub_topic')

        mocker.patch.object(node_mock, 'paths', (node_path_mock,))
        node_path = nodes_loaded.find_node_path(node_path_info_mock)
        assert node_path == node_path_mock

        mocker.patch.object(
            Util, 'find_one', side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            nodes_loaded.find_node_path(node_path_info_mock)

    def test_find_callbacks(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        cb_mock = mocker.Mock(spec=CallbackStruct)

        mocker.patch.object(cb_loaded_mock, 'search_callbacks',
                            return_value=(cb_mock,))
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=(node,))

        loaded = NodeValuesLoaded(reader_mock)
        assert loaded.find_callbacks(('callback_id',)) == (cb_mock,)

        mocker.patch.object(cb_loaded_mock, 'search_callbacks', return_value=())
        with pytest.raises(ItemNotFoundError):
            loaded.find_callbacks(('callback_id',))

    def test_get_callbacks(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        mocker.patch.object(reader_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_publishers', return_value=[])
        mocker.patch.object(reader_mock, 'get_timers', return_value=[])
        mocker.patch.object(reader_mock, 'get_subscriptions', return_value=[])
        mocker.patch.object(reader_mock, 'get_callback_groups', return_value=[])
        mocker.patch.object(reader_mock, 'get_variable_passings', return_value=[])
        mocker.patch.object(reader_mock, 'get_message_contexts', return_value=[])

        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=[node])

        loaded = NodeValuesLoaded(reader_mock)

        cb_mock = mocker.Mock(spec=CallbackStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch.object(cb_loaded_mock, 'data', (cb_mock,))
        mocker.patch.object(Util, 'find_one', return_value=cb_loaded_mock)
        callbacks = loaded.get_callbacks('node')
        assert callbacks == (cb_mock,)

        mocker.patch.object(
            Util, 'find_one', side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            loaded.get_callbacks('node_not_exit')


class TestTopicIgnoreReader:

    def test_get_publishers(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)
        mocker.patch.object(
            TopicIgnoredReader, '_get_ignore_callback_ids', return_value={'ignore'})
        reader = TopicIgnoredReader(reader_mock, ['/ignore'])
        node = NodeValueWithId('node_name', 'node_id')

        pub = PublisherValue('/topic_name', node.node_name, node.node_id, None)
        pub_ignored = PublisherValue('/ignore', node.node_name, node.node_id, None)
        mocker.patch.object(reader_mock, 'get_publishers',
                            return_value=[pub, pub_ignored])

        pubs = reader.get_publishers(node)
        assert len(pubs) == 1
        assert pubs[0] == pub

    def test_get_subscriptions(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)
        mocker.patch.object(
            TopicIgnoredReader, '_get_ignore_callback_ids', return_value={'ignore'})

        reader = TopicIgnoredReader(reader_mock, ['/ignore'])
        node = NodeValue('node_name', 'node_id')

        sub = SubscriptionValue('/topic_name', node.node_name, node.node_id, None)
        sub_ignored = SubscriptionValue('/ignore', node.node_name, node.node_id, None)
        mocker.patch.object(reader_mock, 'get_subscriptions',
                            return_value=[sub, sub_ignored])

        subs = reader.get_subscriptions(node)
        assert len(subs) == 1
        assert subs[0] == sub

    def test_get_executor(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        exec_info = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            ('cbg_name',),
            executor_name='exec_name'
        )
        mocker.patch.object(reader_mock, 'get_executors',
                            return_value=(exec_info,))

        mocker.patch.object(
            TopicIgnoredReader, '_get_ignore_callback_ids', return_value=('cb_ignore'))

        reader = TopicIgnoredReader(reader_mock, ['/ignore'])
        execs_info = reader.get_executors()

        expected = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            ('cbg_name',),
            executor_name='exec_name'
        )
        assert execs_info == (expected,)

    def test_get_ignore_callback_ids(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValueWithId('node', 'node')
        mocker.patch.object(reader_mock, 'get_nodes',
                            return_value=[node])

        sub_info = SubscriptionCallbackValue(
            'cb', node.node_name, node.node_id, 'symbol', 'topic', None
        )
        sub_info_ignore = SubscriptionCallbackValue(
            'cb_ignore', node.node_name, node.node_id, 'symbol', 'topic_ignore', None
        )
        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks',
            return_value=(sub_info, sub_info_ignore,)
        )

        ignore_ids = TopicIgnoredReader._get_ignore_callback_ids(reader_mock, [
                                                                 'topic_ignore'])
        assert ignore_ids == {'cb_ignore'}

    def test_get_subscription_callbacks_info(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)
        mocker.patch.object(
            TopicIgnoredReader, '_get_ignore_callback_ids', return_value={'cb_id_ignore'})
        reader = TopicIgnoredReader(reader_mock, ['/ignore'])
        node = NodeValueWithId('node_name', 'node_name')

        sub_cb = SubscriptionCallbackValue(
            'cb_id', node.node_name, node.node_id, 'symbol', '/topic_name', None)
        sub_cb_ignored = SubscriptionCallbackValue(
            'cb_id_ignore', node.node_name, node.node_id, 'symbol', '/ignore', None)
        mocker.patch.object(reader_mock, 'get_subscription_callbacks',
                            return_value=[sub_cb, sub_cb_ignored])

        sub_cbs = reader.get_subscription_callbacks(node)
        assert len(sub_cbs) == 1
        assert sub_cbs[0] == sub_cb

    def test_get_callback_groups_info(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch.object(
            TopicIgnoredReader, '_get_ignore_callback_ids', return_value={'ignore'})

        reader = TopicIgnoredReader(reader_mock, ['/ignore'])
        callback_id = ['5', 'ignore']
        node = NodeValueWithId('node_name', 'node_id')

        sub_cb = SubscriptionCallbackValue(
            callback_id[0], node.node_name, node.node_id, 'symbol', '/topic_name', None)
        sub_cb_ignored = SubscriptionCallbackValue(
            callback_id[1], node.node_name, node.node_id, 'symbol', '/ignore', None)
        cbg = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name, node.node_id, (sub_cb.callback_id, sub_cb_ignored.callback_id),
            'callback_group_id'
        )

        mocker.patch.object(reader_mock, 'get_callback_groups',
                            return_value=[cbg])
        mocker.patch.object(reader_mock, 'get_subscription_callbacks',
                            return_value=[sub_cb, sub_cb_ignored])

        cbgs = reader.get_callback_groups(node)
        assert len(cbgs) == 1
        cbg = cbgs[0]
        assert len(cbg.callback_ids) == 1
        assert cbg.callback_ids[0] == callback_id[0]


class TestNodePathLoaded:

    def test_empty(self, mocker):
        searcher_mock = mocker.Mock(spec=CallbackPathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.CallbackPathSearcher',
                     return_value=searcher_mock)
        mocker.patch.object(searcher_mock, 'search', return_value=[])
        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'callbacks', [])
        searched = CallbackPathSearched(node_mock)
        assert len(searched.data) == 0

    def test_full(self, mocker):
        searcher_mock = mocker.Mock(spec=CallbackPathSearcher)
        mocker.patch('caret_analyze.architecture.graph_search.CallbackPathSearcher',
                     return_value=searcher_mock)

        callback_mock = mocker.Mock(spec=CallbackStruct)
        node_path_mock = mocker.Mock(NodePathStruct)
        mocker.patch.object(searcher_mock, 'search',
                            return_value=[node_path_mock])
        mocker.patch.object(node_path_mock, 'publish_topic_name', 'pub')
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', 'sub')

        callbacks = (callback_mock, callback_mock)
        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'callbacks', callbacks)
        searched = CallbackPathSearched(node_mock)

        import itertools
        product = list(itertools.product(callbacks, callbacks))
        assert len(searched.data) == len(product)


class TestPublishersLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch.object(callbacks_loaded_mock, 'data', [])
        node = NodeValue('node_name', None)
        loaded = PublishersLoaded(reader_mock, callbacks_loaded_mock, node)
        assert len(loaded.data) == 0

    def test_get_instance(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id = '5'
        publisher_info = PublisherValue(
            'topic_name', 'node_name', 'node_id', (callback_id,)
        )
        mocker.patch.object(reader_mock, 'get_publishers',
                            return_value=[publisher_info])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id)
        mocker.patch.object(callback_mock, 'publish_topic_names', ['topic_name'])
        mocker.patch.object(callback_mock, 'callback_name', 'cb0')

        callback_struct_mock = mocker.Mock(spec=CallbackStruct)
        mocker.patch.object(callback_struct_mock, 'publish_topic_names', ['topic_name'])
        mocker.patch.object(callback_struct_mock, 'callback_name', 'cb0')

        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_struct_mock])
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)
        node = NodeValue('node_name', 'ndoe_id')
        loaded = PublishersLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 1
        pub_struct_info = loaded.data[0]
        assert isinstance(pub_struct_info, PublisherStruct)
        assert pub_struct_info.callback_names is not None
        assert len(pub_struct_info.callback_names) == 1
        assert pub_struct_info.callbacks == (callback_struct_mock,)
        assert pub_struct_info.node_name == publisher_info.node_name
        assert pub_struct_info.topic_name == publisher_info.topic_name


class TestSubscriptionsLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(
            reader_mock, 'get_subscriptions', return_value=[])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        node = NodeValue('node_name', None)
        loaded = SubscriptionsLoaded(reader_mock, callbacks_loaded_mock, node)
        assert len(loaded.data) == 0

    def test_get_instance(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id = '5'

        subscription_info = SubscriptionValue(
            'topic_name', 'node_name', 'node_id',  callback_id
        )
        mocker.patch.object(reader_mock, 'get_subscriptions',
                            return_value=[subscription_info])

        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id)
        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_mock])
        callback_struct_mock = mocker.Mock(spec=SubscriptionCallbackStruct)
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)

        node = NodeValue('node_name', None)
        loaded = SubscriptionsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 1
        sub_struct_info = loaded.data[0]
        assert isinstance(sub_struct_info, SubscriptionStruct)
        assert sub_struct_info.callback == callback_struct_mock
        assert sub_struct_info.node_name == subscription_info.node_name
        assert sub_struct_info.topic_name == subscription_info.topic_name


class TestCallbacksLoaded:

    def test_empty_callback(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('node_name', 'node_name')

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])

        loaded = CallbacksLoaded(reader_mock, node)
        assert len(loaded.data) == 0

    def test_duplicated_callback_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('node_name', 'ndoe_name')

        callback_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(
            callback_mock, 'node_name', node.node_name
        )
        mocker.patch.object(
            callback_mock, 'callback_name', 'duplicated_name'
        )
        mocker.patch.object(
            callback_mock, 'callback_type', CallbackType.TIMER
        )

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[callback_mock, callback_mock])

        with pytest.raises(InvalidReaderError):
            CallbacksLoaded(reader_mock, node)

    def test_invalid_node_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('invalid_node_name', 'invalid_node_name')

        callback_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(
            callback_mock, 'node_name', node.node_name
        )
        mocker.patch.object(
            callback_mock, 'callback_name', 'callback_name'
        )
        mocker.patch.object(
            callback_mock, 'callback_type', CallbackType.TIMER
        )

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[callback_mock, callback_mock])

        with pytest.raises(InvalidReaderError):
            CallbacksLoaded(reader_mock, node)

    def test_find_callback(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_name = ['callback_name0', 'callback_name1']
        period_ns = 4
        topic_name = '/topic_name'
        symbol = ['symbol0', 'symbol1']
        callback_id = ['5', '6', '7']
        node = NodeValueWithId('/node_name', '/node_name')

        timer_cb = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns, (
                topic_name, ), callback_name=callback_name[0])

        sub_cb = SubscriptionCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1],
            topic_name, None, callback_name=callback_name[1])

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[timer_cb])

        loaded = CallbacksLoaded(reader_mock, node)

        with pytest.raises(ItemNotFoundError):
            loaded.find_callback(callback_id[2])

        cb = loaded.find_callback(callback_id[0])
        assert isinstance(cb, CallbackStruct)
        assert cb.node_name == timer_cb.node_id
        assert cb.callback_name == timer_cb.callback_name
        assert cb.callback_type == timer_cb.callback_type
        assert cb.publish_topic_names == (topic_name,)
        assert cb.subscribe_topic_name is None

        cb = loaded.find_callback(callback_id[1])
        assert isinstance(cb, CallbackStruct)
        assert cb.node_name == sub_cb.node_id
        assert cb.callback_name == sub_cb.callback_name
        assert cb.callback_type == sub_cb.callback_type
        assert cb.publish_topic_names is None
        assert cb.subscribe_topic_name == topic_name

    def test_not_implemented_callback_type(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        callback_mock = mocker.Mock(spec=CallbackValue)
        node = NodeValue('node_name', 'node_name')
        mocker.patch.object(callback_mock, 'node_name', node.node_name)
        mocker.patch.object(callback_mock, 'node_id', node.node_id)
        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[callback_mock])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[])

        with pytest.raises(UnsupportedTypeError):
            CallbacksLoaded(reader_mock, node).data

    def test_callback_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValueWithId('/node_name', '/node_name')
        period_ns = 4
        topic_name = '/topic_name'
        symbol = ['symbol0', 'symbol1', 'symbol2', 'symbol3']
        callback_id = ['5', '6', '7', '8']

        timer_cb_0 = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns, ())
        timer_cb_1 = TimerCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1], period_ns, ())
        sub_cb_0 = SubscriptionCallbackValue(
            callback_id[2], node.node_name, node.node_id, symbol[2], topic_name, None)
        sub_cb_1 = SubscriptionCallbackValue(
            callback_id[3], node.node_name, node.node_id, symbol[3], topic_name, None)

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb_0, sub_cb_1])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[timer_cb_0, timer_cb_1])

        loaded = CallbacksLoaded(reader_mock, node)

        cb = loaded.find_callback(callback_id[0])
        assert cb.callback_name == '/node_name/callback_0'

        cb = loaded.find_callback(callback_id[1])
        assert cb.callback_name == '/node_name/callback_1'

        cb = loaded.find_callback(callback_id[2])
        assert cb.callback_name == '/node_name/callback_2'

        cb = loaded.find_callback(callback_id[3])
        assert cb.callback_name == '/node_name/callback_3'


class TestVariablePassingsLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        mocker.patch.object(
            reader_mock, 'get_variable_passings', return_value=[])

        node = NodeValue('node0', None)

        loaded = VariablePassingsLoaded(
            reader_mock, callback_loaded_mock, node)
        assert len(loaded.data) == 0

    def test_get_instances(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        callback_id = ['5', '6']

        # mocker.patch.object(write_callback, 'callback_id', callback_id[0])
        # mocker.patch.object(read_callback, 'callback_id', callback_id[1])
        write_callback = mocker.Mock(spec=CallbackValue)
        read_callback = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(write_callback,
                            'callback_id', callback_id[0])
        mocker.patch.object(read_callback,
                            'callback_id', callback_id[1])

        write_callback_struct = mocker.Mock(spec=CallbackStruct)
        read_callback_struct = mocker.Mock(spec=CallbackStruct)

        def find_callback(cb_id: int):
            if cb_id == callback_id[0]:
                return write_callback_struct
            if cb_id == callback_id[1]:
                return read_callback_struct

        mocker.patch.object(callback_loaded_mock,
                            'find_callback', side_effect=find_callback)

        node = NodeValue('node0', None)
        var_pass = VariablePassingValue(
            node.node_name, callback_id[0], callback_id[1])
        mocker.patch.object(
            reader_mock, 'get_variable_passings', return_value=[var_pass])

        loaded = VariablePassingsLoaded(
            reader_mock, callback_loaded_mock, node)
        assert len(loaded.data) == 1
        datum = loaded.data[0]
        assert isinstance(datum, VariablePassingStruct)
        assert datum.callback_name_read == read_callback_struct.callback_name
        assert datum.callback_name_write == write_callback_struct.callback_name
        assert datum.node_name == node.node_name


class TestCallbackGroupsLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        mocker.patch.object(
            reader_mock, 'get_callback_groups', return_value=[])

        node = NodeValue('node', None)
        loaded = CallbackGroupsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 0

    def test_get_data(self, mocker):
        node = NodeValueWithId('node', 'ndoe')

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        callback_mock = mocker.Mock(spec=CallbackStruct)

        cbg = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            ('callback_ids',),
            'callback_group_id'
        )

        mocker.patch.object(
            reader_mock, 'get_callback_groups', return_value=[cbg])
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_mock)

        loaded = CallbackGroupsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 1
        datum = loaded.data[0]
        assert isinstance(datum, CallbackGroupStruct)
        assert datum.callback_group_type == CallbackGroupType.MUTUALLY_EXCLUSIVE
        assert datum.node_name == node.node_name
        assert len(datum.callbacks) == 1
        assert datum.callbacks[0] == callback_mock


class TestExecutorInfoLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        mocker.patch.object(reader_mock, 'get_executors', return_value=[])

        nodes_loaded = mocker.Mock(NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded)

        executors = loaded.data
        assert executors == ()

    def test_single_executor(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        executor_info_mock = mocker.Mock(ExecutorValue)

        struct_mock = mocker.Mock(spec=ExecutorStruct)
        mocker.patch.object(ExecutorValuesLoaded,
                            '_to_struct', return_value=struct_mock)
        mocker.patch.object(
            reader_mock, 'get_executors', return_value=[executor_info_mock])

        nodes_loaded = mocker.Mock(spec=NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded)

        executors = loaded.data

        assert executors == (struct_mock,)

    def test_to_struct_empty(self, mocker):
        executor = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, ())

        nodes_loaded = mocker.Mock(spec=NodeValuesLoaded)
        struct = ExecutorValuesLoaded._to_struct(
            'exec_name', executor, nodes_loaded)

        assert isinstance(struct, ExecutorStruct)
        assert len(struct.callback_groups) == 0
        assert len(struct.callbacks) == 0

    def test_to_struct_full(self, mocker):
        node_name = 'node'

        node_info_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_info_mock, 'node_name', node_name)

        cbg_info_mock = mocker.Mock(spec=CallbackGroupValue)
        executor_info = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            (cbg_info_mock,))
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(
            cbg_info_mock, 'callback_ids', ('callback',)
        )

        cbg_struct_info_mock = mocker.Mock(spec=CallbackStruct)
        mocker.patch.object(
            nodes_loaded_mock, 'find_callback_group', return_value=cbg_struct_info_mock)
        exec_name = 'single_threaded_executor_0'
        exec_info = ExecutorValuesLoaded._to_struct(
            exec_name, executor_info, nodes_loaded_mock)

        assert isinstance(exec_info, ExecutorStruct)
        assert len(exec_info.callback_groups) == 1
        assert exec_info.callback_groups == (cbg_struct_info_mock,)
        assert exec_info.executor_type == ExecutorType.SINGLE_THREADED_EXECUTOR
        assert exec_info.executor_name == 'single_threaded_executor_0'

    def test_executor_index(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        single_executor_info_mock = mocker.Mock(ExecutorValue)
        mocker.patch.object(single_executor_info_mock, 'executor_name', None)
        mocker.patch.object(
            single_executor_info_mock, 'executor_type', ExecutorType.SINGLE_THREADED_EXECUTOR)

        multi_executor_info_mock = mocker.Mock(ExecutorValue)
        mocker.patch.object(multi_executor_info_mock, 'executor_name', None)
        mocker.patch.object(
            multi_executor_info_mock, 'executor_type', ExecutorType.MULTI_THREADED_EXECUTOR)

        def to_struct(exec_name, executor_info, nodes_info):
            exec_mock = mocker.Mock(spec=ExecutorStruct)
            mocker.patch.object(exec_mock, 'executor_name', exec_name)
            return exec_mock

        mocker.patch.object(ExecutorValuesLoaded,
                            '_to_struct', side_effect=to_struct)
        mocker.patch.object(
            reader_mock, 'get_executors',
            return_value=[single_executor_info_mock, multi_executor_info_mock])

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded_mock)
        executors = loaded.data
        assert executors[0].executor_name == 'executor_0'
        assert executors[1].executor_name == 'executor_1'


class TestCommunicationInfoLoaded:

    def test_empty(self, mocker):

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', [])
        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert len(loaded.data) == 0

        node_info_mock = mocker.Mock(spec=NodeStruct)

        mocker.patch.object(node_info_mock, 'publishers', ())
        mocker.patch.object(node_info_mock, 'subscriptions', ())

        mocker.patch.object(nodes_loaded_mock, 'data', (node_info_mock,))
        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert len(loaded.data) == 0

        pub_info = PublisherValue('topic_a', 'pub_node', 'pub_node_id', None)
        sub_info = SubscriptionValue('topic_b', 'sub_node', 'sub_node_id', None)

        node_info_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_info_mock, 'publishers', [pub_info])
        mocker.patch.object(node_info_mock, 'subscriptions', [sub_info])

        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert len(loaded.data) == 0

    def test_get_data(self, mocker):
        node_info_mock = mocker.Mock(spec=NodeStruct)

        pub_info = PublisherValue('topic_a', 'pub_node', 'pub_node_id', None)
        sub_info = SubscriptionValue('topic_a', 'sub_node', 'sub_node_id', None)

        node_info_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_info_mock, 'publishers', [pub_info])
        mocker.patch.object(node_info_mock, 'subscriptions', [sub_info])

        comm_mock = mocker.Mock(seck=CommunicationStruct)
        mocker.patch.object(CommValuesLoaded,
                            '_to_struct', return_value=comm_mock)

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', (node_info_mock,))

        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert loaded.data == (comm_mock,)

    def test_to_struct_with_callback(self, mocker):
        topic_name = '/chatter'
        pub_node_name = 'talker'
        sub_node_name = 'listener'

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        pub_mock = mocker.Mock(spec=PublisherStruct)
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        node_pub_mock = mocker.Mock(spec=NodeStruct)
        node_sub_mock = mocker.Mock(spec=NodeStruct)

        mocker.patch.object(pub_mock, 'topic_name', topic_name)
        mocker.patch.object(sub_mock, 'topic_name', topic_name)

        mocker.patch.object(pub_mock, 'node_name', pub_node_name)
        mocker.patch.object(sub_mock, 'node_name', sub_node_name)

        mocker.patch.object(node_pub_mock, 'publishers', (pub_mock,))
        mocker.patch.object(node_sub_mock, 'subscriptions', (sub_mock,))

        pub_cb_mock = mocker.Mock(spec=CallbackStruct)
        sub_cb_mock = mocker.Mock(spec=CallbackStruct)

        mocker.patch.object(pub_cb_mock, 'publish_topic_names', (topic_name,))
        mocker.patch.object(pub_cb_mock, 'subscribe_topic_name', None)
        mocker.patch.object(pub_cb_mock, 'node_name', pub_node_name)

        mocker.patch.object(sub_cb_mock, 'publish_topic_names', ())
        mocker.patch.object(sub_cb_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(sub_cb_mock, 'node_name', sub_node_name)

        mocker.patch.object(
            nodes_loaded_mock, 'get_callbacks', return_value=(pub_cb_mock, sub_cb_mock))

        comm_info: CommunicationStruct = CommValuesLoaded._to_struct(
            nodes_loaded_mock, pub_mock, sub_mock, node_pub_mock, node_sub_mock
        )

        assert comm_info.publish_callbacks == (pub_cb_mock,)
        assert comm_info.subscribe_callback == sub_cb_mock
        assert comm_info.publish_node == node_pub_mock
        assert comm_info.subscribe_node == node_sub_mock
        assert comm_info.publisher == pub_mock
        assert comm_info.subscription == sub_mock

    def test_find_communication(self, mocker):
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', ())
        comm_loaded = CommValuesLoaded(nodes_loaded_mock)

        comm_mock = mocker.Mock(spec=CommunicationStruct)

        mocker.patch.object(Util, 'find_one', return_value=comm_mock)
        comm = comm_loaded.find_communication(
            'topic_name', 'pub_node_name', 'sub_node_name')
        assert comm == comm_mock

        mocker.patch.object(
            Util, 'find_one', side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            comm_loaded.find_communication(
                'topic_name', 'pub_node_name', 'sub_node_name')

    def test_to_struct(self, mocker):
        topic_name = '/chatter'
        pub_node_name = 'talker'
        sub_node_name = 'listener'

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        pub_mock = mocker.Mock(spec=PublisherStruct)
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        node_pub_mock = mocker.Mock(spec=NodeStruct)
        node_sub_mock = mocker.Mock(spec=NodeStruct)

        mocker.patch.object(pub_mock, 'topic_name', topic_name)
        mocker.patch.object(sub_mock, 'topic_name', topic_name)

        mocker.patch.object(pub_mock, 'node_name', pub_node_name)
        mocker.patch.object(sub_mock, 'node_name', sub_node_name)

        mocker.patch.object(node_pub_mock, 'publishers', (pub_mock,))
        mocker.patch.object(node_sub_mock, 'subscriptions', (sub_mock,))

        mocker.patch.object(
            nodes_loaded_mock, 'get_callbacks', side_effect=ItemNotFoundError(''))

        comm_info: CommunicationStruct = CommValuesLoaded._to_struct(
            nodes_loaded_mock, pub_mock, sub_mock, node_pub_mock, node_sub_mock
        )

        assert comm_info.publish_callbacks is None
        assert comm_info.subscribe_callback is None
        assert comm_info.publish_node == node_pub_mock
        assert comm_info.subscribe_node == node_sub_mock
        assert comm_info.publisher == pub_mock
        assert comm_info.subscription == sub_mock


class TestPathInfoLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded_mock = mocker.Mock(spec=CommValuesLoaded)

        mocker.patch.object(
            reader_mock, 'get_paths', return_value=())
        loaded = PathValuesLoaded(
            reader_mock, nodes_loaded_mock, comm_loaded_mock)
        paths_info = loaded.data
        assert paths_info == ()

    def test_single_path(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded_mock = mocker.Mock(spec=CommValuesLoaded)

        path_info_mock = mocker.Mock(spec=PathValue)
        mocker.patch.object(reader_mock, 'get_paths',
                            return_value=(path_info_mock,))
        path_mock = mocker.Mock(spec=PathStruct)
        mocker.patch.object(PathValuesLoaded, '_to_struct',
                            return_value=path_mock)

        loaded = PathValuesLoaded(
            reader_mock, nodes_loaded_mock, comm_loaded_mock)

        paths_info = loaded.data
        assert paths_info == (path_mock,)

    def test_to_struct(self, mocker):
        path_name = 'path'

        path_info = PathValue(path_name, ())

        node_path_mock = mocker.Mock(spec=NodePathStruct)
        mocker.patch.object(PathValuesLoaded, '_to_node_path_struct',
                            return_value=(node_path_mock, node_path_mock))

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded_mock = mocker.Mock(spec=CommValuesLoaded)

        comm_mock = mocker.Mock(spec=CommunicationStruct)
        mocker.patch.object(
            comm_loaded_mock, 'find_communication', return_value=comm_mock)

        path_mock = mocker.Mock(spec=PathStruct)
        mocker.patch('caret_analyze.architecture.struct.path.PathStruct',
                     return_value=path_mock)
        paths_info = PathValuesLoaded._to_struct(
            path_info, nodes_loaded_mock, comm_loaded_mock)
        assert paths_info.child == (
            node_path_mock, comm_mock, node_path_mock)
