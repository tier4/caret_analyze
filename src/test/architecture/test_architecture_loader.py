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

import itertools
from logging import getLogger

from caret_analyze.architecture.architecture import \
    DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
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
                                                            ServicesLoaded,
                                                            SubscriptionsLoaded,
                                                            TimersLoaded,
                                                            TopicIgnoredReader,
                                                            VariablePassingsLoaded)
from caret_analyze.architecture.graph_search import CallbackPathSearcher
from caret_analyze.architecture.reader_interface import ArchitectureReader
from caret_analyze.architecture.struct import (CallbackGroupStruct, CallbackStruct,
                                               CommunicationStruct,
                                               ExecutorStruct,
                                               NodePathStruct, NodeStruct, PathStruct,
                                               PublisherStruct,
                                               ServiceCallbackStruct, ServiceStruct,
                                               SubscriptionCallbackStruct, SubscriptionStruct,
                                               TimerCallbackStruct, TimerStruct,
                                               VariablePassingStruct)
from caret_analyze.common import Util
from caret_analyze.exceptions import (InvalidReaderError, ItemNotFoundError,
                                      UnsupportedTypeError)
from caret_analyze.value_objects import (CallbackGroupType, CallbackGroupValue,
                                         CallbackType, CallbackValue,
                                         ExecutorType, ExecutorValue,
                                         NodePathValue, NodeValue,
                                         PathValue, PublisherValue,
                                         ServiceCallbackValue, ServiceValue,
                                         SubscriptionCallbackValue, SubscriptionValue,
                                         TimerCallbackValue, TimerValue,
                                         VariablePassingValue)

from caret_analyze.value_objects.message_context import MessageContextType
from caret_analyze.value_objects.node import NodeValueWithId
from caret_analyze.value_objects.publish_topic_info import PublishTopicInfoValue

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

        arch = ArchitectureLoaded(reader_mock, [],
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        assert len(arch.paths) == 0
        assert len(arch.executors) == 0
        assert len(arch.nodes) == 0
        assert len(arch.communications) == 0

    def test_get_data(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        path_mock = mocker.Mock(spec=PathStruct)
        executor_mock = mocker.Mock(spec=ExecutorStruct)
        node_mock = mocker.Mock(spec=NodeStruct)
        comm_mock = mocker.Mock(spec=CommunicationStruct)
        pub_mock = mocker.Mock(spec=PublisherValue)
        sub_mock = mocker.Mock(spec=SubscriptionValue)
        sub_mock_ = mocker.Mock(spec=SubscriptionValue)
        cb_mock = mocker.Mock(spec=CallbackStruct)
        cbg_mock = mocker.Mock(spec=CallbackGroupStruct)
        cbg_mock_ = mocker.Mock(spec=CallbackGroupStruct)
        exec_mock = mocker.Mock(spec=ExecutorStruct)

        mocker.patch.object(pub_mock, 'topic_name', '/chatter')
        mocker.patch.object(sub_mock, 'topic_name', '/chatter')
        mocker.patch.object(sub_mock_, 'topic_name', '/chatter2')
        mocker.patch.object(node_mock, 'publishers', [pub_mock])
        mocker.patch.object(node_mock, 'subscriptions', [sub_mock, sub_mock_])
        mocker.patch.object(node_mock, 'node_name', 'node_name')
        mocker.patch.object(node_mock, 'callback_groups', [cbg_mock, cbg_mock_])
        mocker.patch.object(exec_mock, 'callback_groups', [cbg_mock, cbg_mock_])
        mocker.patch.object(cbg_mock, 'node_name', 'node_name')
        mocker.patch.object(cbg_mock, 'callbacks', return_value=[cb_mock, ])
        mocker.patch.object(cbg_mock, 'callback_group_name',
                            return_value='callback_group_0')
        mocker.patch.object(cbg_mock_, 'callback_group_name',
                            return_value='service_only_callback_group_0')
        mocker.patch.object(node_mock, 'callback_groups', (cbg_mock,))
        mocker.patch.object(exec_mock, 'callback_groups', (cbg_mock,))

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
        mocker.patch.object(executor_loaded, 'data', (exec_mock,))
        mocker.patch.object(path_loaded, 'data', (path_mock,))

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)
        arch = ArchitectureLoaded(reader_mock, [],
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
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

        loader = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
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

        loader = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        nodes = loader.data
        assert nodes == [node_mock]

    def test_duplicated_node_name_and_id(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        node_mock = mocker.Mock(spec=NodeStruct)
        cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        # duplicate check for node name
        node_a = NodeValue('nodeA', 'nodeAid')
        node_b = NodeValue('nodeA', 'nodeBid')
        mocker.patch.object(reader_mock, 'get_nodes',
                            return_value=[node_a, node_b])

        loader = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        nodes = loader.data
        assert len(nodes) == 1
        assert 'Duplicated node name.' in caplog.messages[0]
        caplog.clear()

        # duplicate check for node id
        node_a = NodeValue('nodeA', 'nodeAid')
        node_b = NodeValue('nodeB', 'nodeAid')
        mocker.patch.object(reader_mock, 'get_nodes',
                            return_value=[node_a, node_b])

        loader = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        nodes = loader.data
        assert len(nodes) == 1
        assert 'Duplicated node id.' in caplog.messages[0]

    def test_name_sort(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        node_a = NodeValue('a', 'a')
        node_b = NodeValue('b', 'b')
        node_c = NodeValue('c', 'c')

        mocker.patch.object(
            reader_mock, 'get_nodes', return_value=[node_b, node_c, node_a])

        def create_node(
            node,
            reader,
            max_callback_construction_order_on_path_searching: int
        ):
            node_mock = mocker.Mock(spec=NodeStruct)
            cb_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
            cbg_loaded_mock = mocker.Mock(spec=CallbackGroupsLoaded)
            mocker.patch.object(node_mock, 'node_name', node.node_name)
            return node_mock, cb_loaded_mock, cbg_loaded_mock

        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            side_effect=create_node)

        loader = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
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
            reader_mock, 'get_services', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timers', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_message_contexts', return_value=[])

        path_searched = mocker.Mock(spec=CallbackPathSearched)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearched',
                     return_value=path_searched)
        mocker.patch.object(path_searched, 'data', [])

        cbg_loaded = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackGroupsLoaded',
                     return_value=cbg_loaded)
        mocker.patch.object(cbg_loaded, 'data', [])

        var_pass_loaded = mocker.Mock(spec=VariablePassingsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.VariablePassingsLoaded',
                     return_value=var_pass_loaded)
        mocker.patch.object(var_pass_loaded, 'data', [])

        cbs_loaded = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbacksLoaded',
                     return_value=cbs_loaded)
        mocker.patch.object(cbs_loaded, 'data', [])

        node_value = NodeValue('node', 'node')
        node, _, _ = NodeValuesLoaded._create_node(
            node_value,
            reader_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        assert node.node_name == 'node'
        assert node.publishers == []
        assert node.subscriptions == []
        assert node.services == []
        assert node.callbacks == []
        assert node.callback_groups == []
        assert node.paths == []
        assert node.variable_passings == []

    def test_create_node_full(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)
        cbg = mocker.Mock(spec=CallbackGroupStruct)
        callback = mocker.Mock(spec=CallbackStruct)
        subscription = mocker.Mock(spec=SubscriptionStruct)
        service = mocker.Mock(spec=ServiceStruct)
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
            reader_mock, 'get_services', return_value=[service])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[publisher])
        mocker.patch.object(
            reader_mock, 'get_timers', return_value=[timer])

        mocker.patch.object(
            reader_mock, 'get_message_contexts', return_value=[context])

        path_searched = mocker.Mock(spec=CallbackPathSearched)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearched',
                     return_value=path_searched)
        mocker.patch.object(path_searched, 'data', [path])

        path_created = mocker.Mock(spec=NodePathCreated)
        mocker.patch('caret_analyze.architecture.architecture_loaded.NodePathCreated',
                     return_value=path_created)
        mocker.patch.object(path_created, 'data', [path_])

        cbg_loaded = mocker.Mock(spec=CallbackGroupsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackGroupsLoaded',
                     return_value=cbg_loaded)
        mocker.patch.object(cbg_loaded, 'data', [cbg])

        var_pass_loaded = mocker.Mock(spec=VariablePassingsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.VariablePassingsLoaded',
                     return_value=var_pass_loaded)
        mocker.patch.object(var_pass_loaded, 'data', [var_pass])

        cbs_loaded = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbacksLoaded',
                     return_value=cbs_loaded)
        mocker.patch.object(cbs_loaded, 'data', [])

        publishers_loaded = mocker.Mock(spec=PublishersLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.PublishersLoaded',
                     return_value=publishers_loaded)
        mocker.patch.object(publishers_loaded, 'data', [publisher])

        subscriptions_loaded = mocker.Mock(spec=SubscriptionsLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.SubscriptionsLoaded',
                     return_value=subscriptions_loaded)
        mocker.patch.object(subscriptions_loaded, 'data', [subscription])

        services_loaded = mocker.Mock(spec=ServicesLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.ServicesLoaded',
                     return_value=services_loaded)
        mocker.patch.object(services_loaded, 'data', [service])

        timers_loaded = mocker.Mock(spec=TimersLoaded)
        mocker.patch('caret_analyze.architecture.architecture_loaded.TimersLoaded',
                     return_value=timers_loaded)
        mocker.patch.object(timers_loaded, 'data', [timer])

        def assigned(node_paths, message_contexts):
            return node_paths

        mocker.patch.object(
            NodeValuesLoaded, '_message_context_assigned', side_effect=assigned)

        mocker.patch.object(
            NodeValuesLoaded, '_search_node_paths', return_value=[path, path_])

        node_value = NodeValue('node', 'node')
        node, _, _ = NodeValuesLoaded._create_node(
            node_value,
            reader_mock,
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING
        )

        assert node.node_name == 'node'
        assert node.publishers == [publisher]
        assert node.timers == [timer]
        assert node.subscriptions == [subscription]
        assert node.services == [service]
        assert node.callbacks == [callback]
        assert node.callback_groups == [cbg]
        assert node.paths == [path, path_]
        assert node.variable_passings == [var_pass]

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

        loaded = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
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

        nodes_loaded = NodeValuesLoaded(reader_mock,
                                        DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)

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

        nodes_loaded = NodeValuesLoaded(reader_mock,
                                        DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)

        mocker.patch.object(nodes_loaded, 'find_node',
                            return_value=node_mock)
        node_path_info_mock = mocker.Mock(spec=NodePathValue)
        node_path_err_mock = mocker.Mock(spec=NodePathValue)

        node_path_mock = mocker.Mock(spec=NodePathStruct)

        mocker.patch.object(node_path_info_mock,
                            'publish_topic_name', 'pub_topic')
        mocker.patch.object(node_path_info_mock,
                            'subscribe_topic_name', 'sub_topic')
        mocker.patch.object(
            node_path_info_mock, 'publisher_construction_order', 0)
        mocker.patch.object(
            node_path_info_mock, 'subscription_construction_order', 0)

        mocker.patch.object(node_path_err_mock,
                            'publish_topic_name', 'pub_topic')
        mocker.patch.object(node_path_err_mock,
                            'subscribe_topic_name', 'sub_topic')
        mocker.patch.object(
            node_path_err_mock, 'publisher_construction_order', 1)
        mocker.patch.object(
            node_path_err_mock, 'subscription_construction_order', 0)

        mocker.patch.object(
            node_path_mock, 'publish_topic_name', 'pub_topic')
        mocker.patch.object(
            node_path_mock, 'subscribe_topic_name', 'sub_topic')
        mocker.patch.object(
            node_path_mock, 'publisher_construction_order', 0)
        mocker.patch.object(
            node_path_mock, 'subscription_construction_order', 0)

        mocker.patch.object(node_mock, 'paths', (node_path_mock,))
        node_path = nodes_loaded.find_node_path(node_path_info_mock)
        assert node_path == node_path_mock

        with pytest.raises(ItemNotFoundError):
            nodes_loaded.find_node_path(node_path_err_mock)

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
                            return_value=[cb_mock])
        mocker.patch.object(NodeValuesLoaded, '_create_node',
                            return_value=(node_mock, cb_loaded_mock, cbg_loaded_mock))

        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=[node])

        loaded = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        assert loaded.find_callbacks(['callback_id']) == [cb_mock]

        mocker.patch.object(cb_loaded_mock, 'search_callbacks', return_value=[])
        with pytest.raises(ItemNotFoundError):
            loaded.find_callbacks(['callback_id'])

    def test_get_callbacks(self, mocker):
        reader_mock = mocker.Mock(spec=TopicIgnoredReader)

        mocker.patch('caret_analyze.architecture.architecture_loaded.TopicIgnoredReader',
                     return_value=reader_mock)

        mocker.patch.object(reader_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_service_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_publishers', return_value=[])
        mocker.patch.object(reader_mock, 'get_timers', return_value=[])
        mocker.patch.object(reader_mock, 'get_subscriptions', return_value=[])
        mocker.patch.object(reader_mock, 'get_services', return_value=[])
        mocker.patch.object(reader_mock, 'get_callback_groups', return_value=[])
        mocker.patch.object(reader_mock, 'get_variable_passings', return_value=[])
        mocker.patch.object(reader_mock, 'get_message_contexts', return_value=[])

        node = NodeValue('node', None)
        mocker.patch.object(reader_mock, 'get_nodes', return_value=[node])

        loaded = NodeValuesLoaded(reader_mock,
                                  DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)

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

        pub = PublisherValue('/topic_name', node.node_name, node.node_id, None, 0)
        pub_ignored = PublisherValue('/ignore', node.node_name, node.node_id, None, 0)
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

        sub = SubscriptionValue('/topic_name', node.node_name, node.node_id, None, 0)
        sub_ignored = SubscriptionValue('/ignore', node.node_name, node.node_id, None, 0)
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
            'cb', node.node_name, node.node_id, 'symbol', '/topic_name', None, 0
        )
        sub_info_ignore = SubscriptionCallbackValue(
            'cb_ignore', node.node_name, node.node_id, 'symbol', 'topic_ignore', None, 0
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
            'cb_id', node.node_name, node.node_id, 'symbol', '/topic_name', None, 0)
        sub_cb_ignored = SubscriptionCallbackValue(
            'cb_id_ignore', node.node_name, node.node_id, 'symbol', '/ignore', None, 0)
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
            callback_id[0], node.node_name, node.node_id, 'symbol', '/topic_name', None, 0)
        sub_cb_ignored = SubscriptionCallbackValue(
            callback_id[1], node.node_name, node.node_id, 'symbol', '/ignore', None, 0)
        cbg = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name, node.node_id, (sub_cb.callback_id, sub_cb_ignored.callback_id),
            'callback_group_id'
        )

        mocker.patch.object(reader_mock, 'get_callback_groups',
                            return_value=[cbg])
        mocker.patch.object(reader_mock, 'get_subscription_callbacks',
                            return_value=[sub_cb, sub_cb_ignored])

        callback_groups = reader.get_callback_groups(node)
        assert len(callback_groups) == 1
        cbg = callback_groups[0]
        assert len(cbg.callback_ids) == 1
        assert cbg.callback_ids[0] == callback_id[0]


class TestNodePathLoaded:

    def test_empty(self, mocker):
        searcher_mock = mocker.Mock(spec=CallbackPathSearcher)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearcher',
                     return_value=searcher_mock)
        mocker.patch.object(searcher_mock, 'search', return_value=[])
        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'callbacks', [])
        searched = CallbackPathSearched(node_mock,
                                        DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)
        assert len(searched.data) == 0

    def test_full(self, mocker):
        searcher_mock = mocker.Mock(spec=CallbackPathSearcher)
        mocker.patch('caret_analyze.architecture.architecture_loaded.CallbackPathSearcher',
                     return_value=searcher_mock)

        callback_mock = mocker.Mock(spec=CallbackStruct)
        callback_mock.construction_order = \
            DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING - 1
        node_path_mock = mocker.Mock(NodePathStruct)
        mocker.patch.object(searcher_mock, 'search',
                            return_value=[node_path_mock])
        mocker.patch.object(node_path_mock, 'publish_topic_name', 'pub')
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', 'sub')

        callbacks = (callback_mock, callback_mock)
        node_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_mock, 'callbacks', callbacks)
        searched = CallbackPathSearched(node_mock,
                                        DEFAULT_MAX_CALLBACK_CONSTRUCTION_ORDER_ON_PATH_SEARCHING)

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
            'topic_name', 'node_name', 'node_id', (callback_id,), 0
        )
        pub_info = PublishTopicInfoValue('topic_name', 0)
        mocker.patch.object(reader_mock, 'get_publishers',
                            return_value=[publisher_info])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id)
        mocker.patch.object(callback_mock, 'publish_topics', [pub_info])
        mocker.patch.object(callback_mock, 'callback_name', 'cb0')

        callback_struct_mock = mocker.Mock(spec=CallbackStruct)
        mocker.patch.object(callback_struct_mock, 'publish_topics', [pub_info])
        mocker.patch.object(callback_struct_mock, 'callback_name', 'cb0')

        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_struct_mock])
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)
        node = NodeValue('node_name', 'node_id')
        loaded = PublishersLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 1
        pub_struct_info = loaded.data[0]
        assert isinstance(pub_struct_info, PublisherStruct)
        assert pub_struct_info.callback_names is not None
        assert len(pub_struct_info.callback_names) == 1
        assert pub_struct_info.callbacks == [callback_struct_mock]
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
            'topic_name', 'node_name', 'node_id',  callback_id, 0
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

    def test_duplicated_subscription_name(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id_a = '5'
        callback_id_b = '6'

        subscript_info_a = SubscriptionValue(
            'topic_name', 'node_name', 'node_id',  callback_id_a, 0
        )
        subscript_info_b = SubscriptionValue(
            'topic_name', 'node_name', 'node_id',  callback_id_b, 0
        )
        subscript_info_c = SubscriptionValue(
            'topic_name', 'node_name', 'node_id',  callback_id_a, 0
        )
        mocker.patch.object(reader_mock, 'get_subscriptions',
                            return_value=[subscript_info_a, subscript_info_b, subscript_info_c])

        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id_a)
        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_mock])
        callback_struct_mock = mocker.Mock(spec=SubscriptionCallbackStruct)
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)

        node = NodeValue('node_name', None)
        loaded = SubscriptionsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 2
        assert 'Duplicated callback id.' in caplog.messages[0]


class TestServicesLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(
            reader_mock, 'get_services', return_value=[])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        node = NodeValue('node_name', None)
        loaded = ServicesLoaded(reader_mock, callbacks_loaded_mock, node)
        assert len(loaded.data) == 0

    def test_get_instance(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id = '5'

        service_info = ServiceValue(
            'service_name', 'node_name', 'node_id',  callback_id, 0
        )
        mocker.patch.object(reader_mock, 'get_services',
                            return_value=[service_info])

        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id)
        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_mock])
        callback_struct_mock = mocker.Mock(spec=ServiceCallbackStruct)
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)

        node = NodeValue('node_name', None)
        loaded = ServicesLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 1
        srv_struct_info = loaded.data[0]
        assert isinstance(srv_struct_info, ServiceStruct)
        assert srv_struct_info.callback == callback_struct_mock
        assert srv_struct_info.node_name == service_info.node_name
        assert srv_struct_info.service_name == service_info.service_name

    def test_duplicated_service_name(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id_a = '5'
        callback_id_b = '6'

        service_info_a = ServiceValue(
            'service_name', 'node_name', 'node_id',  callback_id_a, 0
        )
        service_info_b = ServiceValue(
            'service_name', 'node_name', 'node_id',  callback_id_b, 0
        )
        service_info_c = ServiceValue(
            'service_name', 'node_name', 'node_id',  callback_id_a, 0
        )
        mocker.patch.object(reader_mock, 'get_services',
                            return_value=[service_info_a, service_info_b, service_info_c])

        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id_a)
        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_mock])
        callback_struct_mock = mocker.Mock(spec=ServiceCallbackStruct)
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)

        node = NodeValue('node_name', None)
        loaded = ServicesLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 2
        assert 'Duplicated callback id.' in caplog.messages[0]


class TestCallbacksLoaded:

    def test_empty_callback(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('node_name', 'node_name')

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_publishers', return_value=[])

        loaded = CallbacksLoaded(reader_mock, node)
        assert len(loaded.data) == 0

    def test_duplicated_callback_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValue('node_name', 'node_name')

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
            reader_mock, 'get_service_callbacks', return_value=[])
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
            reader_mock, 'get_service_callbacks', return_value=[])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[callback_mock, callback_mock])

        with pytest.raises(InvalidReaderError):
            CallbacksLoaded(reader_mock, node)

    def test_find_callback(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_name = ['callback_name0', 'callback_name1', 'callback_name2']
        period_ns = 4
        topic_name = '/topic_name'
        pub_info = PublishTopicInfoValue(topic_name, 0)
        pub_infos = (pub_info,)
        service_name = '/service_name'
        symbol = ['symbol0', 'symbol1', 'symbol2']
        callback_id = ['5', '6', '7', '8']
        node = NodeValueWithId('/node_name', '/node_name')

        timer_cb = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns,
            pub_infos, callback_name=callback_name[0], construction_order=0)

        sub_cb = SubscriptionCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1],
            topic_name, None, callback_name=callback_name[1], construction_order=0)

        srv_cb = ServiceCallbackValue(
            callback_id[2], node.node_name, node.node_id, symbol[2],
            service_name, None, callback_name=callback_name[2], construction_order=0)

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[srv_cb])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[timer_cb])

        loaded = CallbacksLoaded(reader_mock, node)

        with pytest.raises(ItemNotFoundError):
            loaded.find_callback(callback_id[3])

        cb = loaded.find_callback(callback_id[0])
        assert isinstance(cb, CallbackStruct)
        assert cb.node_name == timer_cb.node_id
        assert cb.callback_name == timer_cb.callback_name
        assert cb.callback_type == timer_cb.callback_type
        assert cb.publish_topics == [pub_info]
        assert cb.subscribe_topic_name is None

        cb = loaded.find_callback(callback_id[1])
        assert isinstance(cb, CallbackStruct)
        assert cb.node_name == sub_cb.node_id
        assert cb.callback_name == sub_cb.callback_name
        assert cb.callback_type == sub_cb.callback_type
        assert cb.publish_topics is None
        assert cb.subscribe_topic_name == topic_name

        cb = loaded.find_callback(callback_id[2])
        assert isinstance(cb, CallbackStruct)
        assert cb.node_name == srv_cb.node_id
        assert cb.callback_name == srv_cb.callback_name
        assert cb.callback_type == srv_cb.callback_type
        assert cb.publish_topics is None
        assert cb.subscribe_topic_name is None
        assert cb.service_name == srv_cb.service_name

    def test_not_implemented_callback_type(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        callback_mock1 = mocker.Mock(spec=CallbackValue)
        callback_mock2 = mocker.Mock(spec=CallbackValue)
        node = NodeValue('node_name', 'node_name')
        mocker.patch.object(callback_mock1, 'node_name', node.node_name)
        mocker.patch.object(callback_mock1, 'node_id', node.node_id)
        mocker.patch.object(callback_mock2, 'node_name', node.node_name)
        mocker.patch.object(callback_mock2, 'node_id', node.node_id)
        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[callback_mock1])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[callback_mock2])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[])

        with pytest.raises(UnsupportedTypeError):
            CallbacksLoaded(reader_mock, node).data

    def test_callback_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValueWithId('/node_name', '/node_name')
        period_ns = 4
        topic_name = '/topic_name'
        service_name = '/service_name'
        symbol = ['symbol0', 'symbol1', 'symbol2', 'symbol3', 'symbol4', 'symbol5']
        callback_id = ['5', '6', '7', '8', '9', '10']

        timer_cb_0 = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns, (),
            construction_order=0)
        timer_cb_1 = TimerCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1], period_ns, (),
            construction_order=0)
        sub_cb_0 = SubscriptionCallbackValue(
            callback_id[2], node.node_name, node.node_id, symbol[2], topic_name, None,
            construction_order=0)
        sub_cb_1 = SubscriptionCallbackValue(
            callback_id[3], node.node_name, node.node_id, symbol[3], topic_name, None,
            construction_order=0)
        srv_cb_0 = ServiceCallbackValue(
            callback_id[4], node.node_name, node.node_id, symbol[4], service_name, None,
            construction_order=0)
        srv_cb_1 = ServiceCallbackValue(
            callback_id[5], node.node_name, node.node_id, symbol[5], service_name, None,
            construction_order=0)

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb_0, sub_cb_1])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[srv_cb_0, srv_cb_1])
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

    def test_duplicated_callback_id(self, mocker, caplog):
        logger = getLogger(__name__)

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        node = NodeValueWithId('/node_name', '/node_name')
        period_ns = 4
        topic_name = '/topic_name'
        service_name = '/service_name'
        symbol = ['symbol0', 'symbol1', 'symbol2', 'symbol3', 'symbol4', 'symbol5']
        callback_id = ['5', '6', '5', '8', '9', '10']

        timer_cb_0 = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns, (),
            construction_order=0)
        timer_cb_1 = TimerCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1], period_ns, (),
            construction_order=0)
        sub_cb_0 = SubscriptionCallbackValue(
            callback_id[2], node.node_name, node.node_id, symbol[2], topic_name, None,
            construction_order=0)
        sub_cb_1 = SubscriptionCallbackValue(
            callback_id[3], node.node_name, node.node_id, symbol[3], topic_name, None,
            construction_order=0)
        srv_cb_0 = ServiceCallbackValue(
            callback_id[4], node.node_name, node.node_id, symbol[4], service_name, None,
            construction_order=0)
        srv_cb_1 = ServiceCallbackValue(
            callback_id[5], node.node_name, node.node_id, symbol[5], service_name, None,
            construction_order=0)

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb_0, sub_cb_1])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[srv_cb_0, srv_cb_1])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[timer_cb_0, timer_cb_1])

        with pytest.raises(InvalidReaderError) as e:
            CallbacksLoaded(reader_mock, node)

        logger.warning(e)
        assert 'Duplicated callback id.' in caplog.messages[0]

    def test_duplicated_callback_name_cl(self, mocker, caplog):
        logger = getLogger(__name__)

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_name = ['callback_name0', 'callback_name1', 'callback_name2']
        period_ns = 4
        topic_name = '/topic_name'
        service_name = '/service_name'
        symbol = ['symbol0', 'symbol1', 'symbol2']
        callback_id = ['5', '6', '7', '8']
        node = NodeValueWithId('/node_name', '/node_name')

        timer_cb = TimerCallbackValue(
            callback_id[0], node.node_name, node.node_id, symbol[0], period_ns, (
                topic_name, ), construction_order=0, callback_name=callback_name[0])

        sub_cb = SubscriptionCallbackValue(
            callback_id[1], node.node_name, node.node_id, symbol[1],
            topic_name, None, construction_order=0, callback_name=callback_name[1])

        srv_cb = ServiceCallbackValue(
            callback_id[2], node.node_name, node.node_id, symbol[2],
            service_name, None, construction_order=0, callback_name=callback_name[0])

        mocker.patch.object(
            reader_mock, 'get_subscription_callbacks', return_value=[sub_cb])
        mocker.patch.object(
            reader_mock, 'get_service_callbacks', return_value=[srv_cb])
        mocker.patch.object(
            reader_mock, 'get_timer_callbacks', return_value=[timer_cb])

        with pytest.raises(InvalidReaderError) as e:
            CallbacksLoaded(reader_mock, node)

        logger.warning(e)
        assert 'Duplicated callback names.' in caplog.messages[0]


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
        node = NodeValueWithId('node', 'node')

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

    def test_name(self, mocker):
        node = NodeValueWithId('node', 'node')

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        cbg_0 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_0'
        )
        cbg_name = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_1',
            callback_group_name='callback_group_name'
        )
        cbg_1 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_2',
        )

        mocker.patch.object(
            reader_mock, 'get_callback_groups', return_value=[cbg_0, cbg_name, cbg_1])

        loaded = CallbackGroupsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        callback_group_names = [cbg.callback_group_name for cbg in loaded.data]
        expect = ['node/callback_group_0', 'callback_group_name', 'node/callback_group_1']
        assert callback_group_names == expect

    def test_duplicated_callback_group_id(self, mocker, caplog):
        node = NodeValueWithId('node', 'node')

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        cbg_0 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_0'
        )
        cbg_name = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_1',
            callback_group_name='callback_group_name'
        )
        cbg_1 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_0',
        )

        mocker.patch.object(
            reader_mock, 'get_callback_groups', return_value=[cbg_0, cbg_name, cbg_1])

        loaded = CallbackGroupsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 2
        assert 'Duplicated callback group id.' in caplog.messages[0]

    def test_duplicated_callback_group_name(self, mocker, caplog):
        node = NodeValueWithId('node', 'node')

        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)

        cbg_0 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_0'
        )
        cbg_name = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_1',
            callback_group_name='callback_group_name'
        )
        cbg_1 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node.node_name,
            node.node_id,
            (),
            'callback_group_id_2',
            callback_group_name='callback_group_name'
        )

        mocker.patch.object(
            reader_mock, 'get_callback_groups', return_value=[cbg_0, cbg_name, cbg_1])

        loaded = CallbackGroupsLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 2
        assert 'Duplicated callback group name.' in caplog.messages[0]


class TestExecutorInfoLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        mocker.patch.object(reader_mock, 'get_executors', return_value=[])

        nodes_loaded = mocker.Mock(NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded)

        executors = loaded.data
        assert executors == []

    def test_name(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        exec_0 = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, ())
        exec_named = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, (), executor_name='exec_name')
        exec_1 = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, ())
        mocker.patch.object(reader_mock, 'get_executors',
                            return_value=[exec_0, exec_named, exec_1])

        nodes_loaded = mocker.Mock(NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded)

        executors = loaded.data
        executor_names = [e.executor_name for e in executors]
        expect = ['executor_0', 'exec_name', 'executor_1']
        assert executor_names == expect

    def test_duplicated_executor_name(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        # duplicate check for executor name
        exec_0 = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, ())
        exec_named1 = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, (), executor_name='exec_name')
        exec_1 = ExecutorValue(
            ExecutorType.MULTI_THREADED_EXECUTOR.type_name, ())
        exec_named2 = ExecutorValue(
            ExecutorType.MULTI_THREADED_EXECUTOR.type_name, (), executor_name='exec_name')
        exec_2 = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name, ())
        mocker.patch.object(reader_mock, 'get_executors',
                            return_value=[exec_0, exec_named1, exec_1, exec_named2, exec_2])

        nodes_loaded = mocker.Mock(NodeValuesLoaded)
        loaded = ExecutorValuesLoaded(reader_mock, nodes_loaded)

        executors = loaded.data
        assert len(executors) == 4
        assert 'Duplicated executor name.' in caplog.messages[0]

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

        assert executors == [struct_mock]

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
            cbg_info_mock, 'callback_ids', ['callback']
        )

        cbg_struct_info_mock = mocker.Mock(spec=CallbackStruct)
        mocker.patch.object(
            nodes_loaded_mock, 'find_callback_group', return_value=cbg_struct_info_mock)
        exec_name = 'single_threaded_executor_0'
        exec_info = ExecutorValuesLoaded._to_struct(
            exec_name, executor_info, nodes_loaded_mock)

        assert isinstance(exec_info, ExecutorStruct)
        assert len(exec_info.callback_groups) == 1
        assert exec_info.callback_groups == [cbg_struct_info_mock]
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

        mocker.patch.object(node_info_mock, 'publishers', [])
        mocker.patch.object(node_info_mock, 'subscriptions', [])

        mocker.patch.object(nodes_loaded_mock, 'data', [node_info_mock])
        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert len(loaded.data) == 0

        pub_info = PublisherValue('topic_a', 'pub_node', 'pub_node_id', None, 0)
        sub_info = SubscriptionValue('topic_b', 'sub_node', 'sub_node_id', None, 0)

        node_info_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_info_mock, 'publishers', [pub_info])
        mocker.patch.object(node_info_mock, 'subscriptions', [sub_info])

        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert len(loaded.data) == 0

    def test_get_data(self, mocker):
        node_info_mock = mocker.Mock(spec=NodeStruct)

        pub_info = PublisherValue('topic_a', 'pub_node', 'pub_node_id', None, 0)
        sub_info = SubscriptionValue('topic_a', 'sub_node', 'sub_node_id', None, 0)

        node_info_mock = mocker.Mock(spec=NodeStruct)
        mocker.patch.object(node_info_mock, 'publishers', [pub_info])
        mocker.patch.object(node_info_mock, 'subscriptions', [sub_info])

        comm_mock = mocker.Mock(spec=CommunicationStruct)
        mocker.patch.object(CommValuesLoaded,
                            '_to_struct', return_value=comm_mock)

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', [node_info_mock])

        loaded = CommValuesLoaded(nodes_loaded_mock)
        assert loaded.data == [comm_mock]

    def test_to_struct_with_callback(self, mocker):
        topic_name = '/chatter'
        pub_node_name = 'talker'
        sub_node_name = 'listener'
        callback_name = 'callback_name'

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        pub_mock = mocker.Mock(spec=PublisherStruct)
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        node_pub_mock = mocker.Mock(spec=NodeStruct)
        node_sub_mock = mocker.Mock(spec=NodeStruct)
        sub_cb_mock = mocker.Mock(spec=SubscriptionCallbackStruct)

        mocker.patch.object(pub_mock, 'topic_name', topic_name)
        mocker.patch.object(sub_mock, 'topic_name', topic_name)
        mocker.patch.object(pub_mock, 'construction_order', 0)

        mocker.patch.object(pub_mock, 'node_name', pub_node_name)
        mocker.patch.object(sub_mock, 'node_name', sub_node_name)
        mocker.patch.object(sub_mock, 'callback_name', callback_name)
        mocker.patch.object(sub_mock, 'callback', sub_cb_mock)
        mocker.patch.object(sub_mock, 'construction_order', 1)
        mocker.patch.object(sub_cb_mock, 'construction_order', 1)

        mocker.patch.object(node_pub_mock, 'publishers', [pub_mock])
        mocker.patch.object(node_sub_mock, 'subscriptions', [sub_mock])

        pub_cb_mock = mocker.Mock(spec=CallbackStruct)
        sub_cb_mock = mocker.Mock(spec=CallbackStruct)

        pub_info = PublishTopicInfoValue(topic_name, 0)
        mocker.patch.object(pub_cb_mock, 'publish_topics',  [pub_info])
        mocker.patch.object(pub_cb_mock, 'subscribe_topic_name', None)
        mocker.patch.object(pub_cb_mock, 'node_name', pub_node_name)

        mocker.patch.object(sub_cb_mock, 'publish_topics', [])
        mocker.patch.object(sub_cb_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(sub_cb_mock, 'node_name', sub_node_name)
        mocker.patch.object(sub_cb_mock, 'callback_name', callback_name)
        mocker.patch.object(sub_cb_mock, 'construction_order', 0)

        sub_cb_mock2 = mocker.Mock(spec=CallbackStruct)
        mocker.patch.object(sub_cb_mock2, 'publish_topics', [])
        mocker.patch.object(sub_cb_mock2, 'subscribe_topic_name', topic_name)
        mocker.patch.object(sub_cb_mock2, 'node_name', sub_node_name)
        mocker.patch.object(sub_cb_mock2, 'callback_name', callback_name)
        mocker.patch.object(sub_cb_mock2, 'construction_order', 1)

        mocker.patch.object(
            nodes_loaded_mock, 'get_callbacks',
            return_value=(pub_cb_mock, sub_cb_mock, sub_cb_mock2))

        comm_info: CommunicationStruct = CommValuesLoaded._to_struct(
            nodes_loaded_mock, pub_mock, sub_mock, node_pub_mock, node_sub_mock
        )

        assert comm_info.publish_callbacks == [pub_cb_mock]
        assert comm_info.subscribe_callback == sub_cb_mock2
        assert comm_info.publish_node == node_pub_mock
        assert comm_info.subscribe_node == node_sub_mock
        assert comm_info.publisher == pub_mock
        assert comm_info.subscription == sub_mock

    def test_find_communication(self, mocker):
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', [])
        comm_loaded = CommValuesLoaded(nodes_loaded_mock)

        comm_mock = mocker.Mock(spec=CommunicationStruct)

        mocker.patch.object(Util, 'find_one', return_value=comm_mock)
        comm = comm_loaded.find_communication(
            'topic_name', 'pub_node_name', 0, 'sub_node_name', 0)
        assert comm == comm_mock

        mocker.patch.object(
            Util, 'find_one', side_effect=ItemNotFoundError(''))
        with pytest.raises(ItemNotFoundError):
            comm_loaded.find_communication(
                'topic_name', 'pub_node_name', 0, 'sub_node_name', 0)

    def test_find_communication_find_one(self, mocker):
        node_info_mock = mocker.Mock(spec=NodeStruct)

        pub_info = PublisherValue('topic_a', 'pub_node', 'pub_node_id', None, 0)
        sub_info = SubscriptionValue('topic_a', 'sub_node', 'sub_node_id', None, 0)
        mocker.patch.object(node_info_mock, 'publishers', [pub_info])
        mocker.patch.object(node_info_mock, 'subscriptions', [sub_info])

        comm_mock = mocker.Mock(spec=CommunicationStruct)
        mocker.patch.object(
            comm_mock, 'topic_name', 'topic_name')
        mocker.patch.object(
            comm_mock, 'publish_node_name', 'pub_node_name')
        mocker.patch.object(
            comm_mock, 'subscribe_node_name', 'sub_node_name')
        mocker.patch.object(
            comm_mock, 'publisher_construction_order', 0)
        mocker.patch.object(
            comm_mock, 'subscription_construction_order', 0)

        mocker.patch.object(CommValuesLoaded,
                            '_to_struct', return_value=comm_mock)

        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        mocker.patch.object(nodes_loaded_mock, 'data', [node_info_mock])

        comm_loaded = CommValuesLoaded(nodes_loaded_mock)

        comm = comm_loaded.find_communication(
            'topic_name', 'pub_node_name', 0, 'sub_node_name', 0)
        assert comm == comm_mock

        with pytest.raises(ItemNotFoundError):
            comm_loaded.find_communication(
                'topic_name', 'pub_node_name', 1, 'sub_node_name', 0)

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

        mocker.patch.object(node_pub_mock, 'publishers', [pub_mock])
        mocker.patch.object(node_sub_mock, 'subscriptions', [sub_mock])

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
            reader_mock, 'get_paths', return_value=[])
        loaded = PathValuesLoaded(
            reader_mock, nodes_loaded_mock, comm_loaded_mock)
        paths_info = loaded.data
        assert paths_info == []

    def test_single_path(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded_mock = mocker.Mock(spec=CommValuesLoaded)

        path_info_mock = mocker.Mock(spec=PathValue)
        mocker.patch.object(reader_mock, 'get_paths',
                            return_value=[path_info_mock])
        path_mock = mocker.Mock(spec=PathStruct)
        mocker.patch.object(PathValuesLoaded, '_to_struct',
                            return_value=path_mock)

        loaded = PathValuesLoaded(
            reader_mock, nodes_loaded_mock, comm_loaded_mock)

        paths_info = loaded.data
        assert paths_info == [path_mock]

    def test_duplicated_path_name(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        nodes_loaded_mock = mocker.Mock(spec=NodeValuesLoaded)
        comm_loaded_mock = mocker.Mock(spec=CommValuesLoaded)

        # duplicate check for path name
        path_name_a = 'pathA'
        path_name_b = 'pathB'
        path_name_c = 'pathA'
        path_info_a = PathValue(path_name_a, [])
        path_info_b = PathValue(path_name_b, [])
        path_info_c = PathValue(path_name_c, [])
        mocker.patch.object(reader_mock, 'get_paths',
                            return_value=[path_info_a, path_info_b, path_info_c])

        path_mock = mocker.Mock(spec=PathStruct)
        mocker.patch.object(PathValuesLoaded, '_to_struct',
                            return_value=path_mock)

        loaded = PathValuesLoaded(
            reader_mock, nodes_loaded_mock, comm_loaded_mock)
        nodes = loaded.data
        assert len(nodes) == 2
        assert 'Duplicated path name.' in caplog.messages[0]

    def test_to_struct(self, mocker):
        path_name = 'path'

        path_info = PathValue(path_name, [])

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
        assert paths_info.child == [
            node_path_mock, comm_mock, node_path_mock]


class TestTimersLoaded:

    def test_empty(self, mocker):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        mocker.patch.object(
            reader_mock, 'get_timers', return_value=[])
        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        mocker.patch.object(callbacks_loaded_mock, 'data', [])
        node = NodeValue('node_name', None)
        loaded = TimersLoaded(reader_mock, callbacks_loaded_mock, node)
        assert len(loaded.data) == 0

    def test_duplicated_timer_callback_id(self, mocker, caplog):
        reader_mock = mocker.Mock(spec=ArchitectureReader)
        callback_id_a = '5'
        callback_id_b = '6'
        period_ns = 4

        timer_info_a = TimerValue(
            period_ns, 'node_name', 'node_id',  callback_id_a, 0
        )
        timer_info_b = TimerValue(
            period_ns, 'node_name', 'node_id',  callback_id_b, 0
        )
        timer_info_c = TimerValue(
            period_ns, 'node_name', 'node_id',  callback_id_a, 0
        )
        mocker.patch.object(reader_mock, 'get_timers',
                            return_value=[timer_info_a, timer_info_b, timer_info_c])

        callbacks_loaded_mock = mocker.Mock(spec=CallbacksLoaded)
        callback_mock = mocker.Mock(spec=CallbackValue)
        mocker.patch.object(callback_mock, 'callback_id', callback_id_a)
        mocker.patch.object(callbacks_loaded_mock, 'data', [callback_mock])
        callback_struct_mock = mocker.Mock(spec=TimerCallbackStruct)
        mocker.patch.object(callbacks_loaded_mock,
                            'find_callback', return_value=callback_struct_mock)

        node = NodeValue('node_name', None)
        loaded = TimersLoaded(
            reader_mock, callbacks_loaded_mock, node)

        assert len(loaded.data) == 2
        assert 'Duplicated callback id.' in caplog.messages[0]
