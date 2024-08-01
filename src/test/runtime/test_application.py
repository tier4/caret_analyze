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


from caret_analyze.architecture.architecture import Architecture
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.runtime.application import Application
from caret_analyze.runtime.callback import CallbackBase
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.executor import Executor
from caret_analyze.runtime.node import Node
from caret_analyze.runtime.node_path import NodePath
from caret_analyze.runtime.path import Path
from caret_analyze.runtime.runtime_loaded import RuntimeLoaded

import pytest


class TestApplication:

    def test_empty_architecture(self, mocker):
        arch_mock = mocker.Mock(spec=Architecture)

        assigned_mock = mocker.Mock(spec=RuntimeLoaded)
        mocker.patch.object(assigned_mock, 'nodes', [])
        mocker.patch.object(assigned_mock, 'executors', [])
        mocker.patch.object(assigned_mock, 'paths', [])
        mocker.patch.object(assigned_mock, 'communications', [])
        mocker.patch(
            'caret_analyze.runtime.application.RuntimeLoaded', return_value=assigned_mock)
        records_provider_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        records_provider_mock.data = data_model_mock
        app = Application(arch_mock, records_provider_mock)

        assert len(app.paths) == 0
        assert len(app.nodes) == 0
        assert len(app.executors) == 0
        assert len(app.communications) == 0
        assert len(app.callbacks) == 0

        with pytest.raises(ItemNotFoundError):
            app.get_path('')

        with pytest.raises(ItemNotFoundError):
            app.get_node('')

        with pytest.raises(ItemNotFoundError):
            app.get_callback('')

        with pytest.raises(ItemNotFoundError):
            app.get_communication('', '', '')

    def test_full_architecture(self, mocker):
        # define mocks
        arch_mock = mocker.Mock(spec=Architecture)
        records_provider_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        records_provider_mock.data = data_model_mock

        node_mock = mocker.Mock(spec=Node)
        executor_mock = mocker.Mock(spec=Executor)
        path_mock = mocker.Mock(spec=Path)
        comm_mock = mocker.Mock(spec=Communication)
        callback_mock = mocker.Mock(spec=CallbackBase)
        records_assigned_mock = mocker.Mock(spec=RuntimeLoaded)
        node_path_mock = mocker.Mock(spec=NodePath)

        # patch mocks
        mocker.patch.object(node_path_mock, 'node_name', 'node_name_')
        mocker.patch.object(node_path_mock, 'publish_topic_name', 'publish_topic_name_')
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', 'subscribe_topic_name_')
        mocker.patch.object(node_path_mock, 'publisher_construction_order', 0)
        mocker.patch.object(node_path_mock, 'subscription_construction_order', 0)
        mocker.patch.object(
            callback_mock, 'callback_name', 'callback_name_')
        mocker.patch.object(node_mock, 'node_name', 'node_name_')
        mocker.patch.object(node_mock, 'callbacks', [callback_mock])
        mocker.patch.object(node_mock, 'paths', [node_path_mock])
        mocker.patch.object(node_mock, 'publishers', 'pub_name_')

        mocker.patch.object(path_mock, 'path_name', 'path_name_')
        mocker.patch.object(comm_mock, 'publish_node_name',
                            'publish_node_name_')
        mocker.patch.object(
            comm_mock, 'subscribe_node_name', 'subscribe_node_name_')
        mocker.patch.object(
            comm_mock, 'topic_name', 'topic_name')
        mocker.patch.object(
            comm_mock, 'publisher_construction_order', 0)
        mocker.patch.object(
            comm_mock, 'subscription_construction_order', 0)

        mocker.patch.object(arch_mock, 'nodes', [node_mock])
        mocker.patch.object(arch_mock,
                            'executors', [executor_mock])
        mocker.patch.object(arch_mock, 'paths', [path_mock])
        mocker.patch.object(arch_mock,
                            'communications', [comm_mock])

        mocker.patch('caret_analyze.runtime.application.RuntimeLoaded',
                     return_value=records_assigned_mock)

        mocker.patch.object(records_assigned_mock, 'nodes', [node_mock])
        mocker.patch.object(records_assigned_mock,
                            'executors', [executor_mock])
        mocker.patch.object(records_assigned_mock, 'paths', [path_mock])
        mocker.patch.object(records_assigned_mock,
                            'communications', [comm_mock])

        # test scenario
        app = Application(arch_mock, records_provider_mock)

        assert len(app.paths) == 1
        assert len(app.nodes) == 1
        assert len(app.executors) == 1
        assert len(app.communications) == 1
        assert len(app.callbacks) == 1

        assert app.get_path(path_mock.path_name) == path_mock
        assert app.get_node(node_mock.node_name) == node_mock
        assert app.get_callback(
            callback_mock.callback_name) == callback_mock

        assert app.get_communication(
            comm_mock.publish_node_name,
            comm_mock.subscribe_node_name,
            comm_mock.topic_name,
            publisher_construction_order=comm_mock.publisher_construction_order,
            subscription_construction_order=comm_mock.subscription_construction_order) == \
            comm_mock

        with pytest.raises(ItemNotFoundError) as e:
            app.get_communication(
                comm_mock.publish_node_name,
                comm_mock.subscribe_node_name,
                comm_mock.topic_name,
                publisher_construction_order=1,
                subscription_construction_order=0)
        assert 'publisher_construction_order' in str(e.value)

        assert app.get_node_path(
            node_path_mock.node_name,
            node_path_mock.subscribe_topic_name,
            node_path_mock.publish_topic_name,
            subscription_construction_order=node_path_mock.subscription_construction_order,
            publisher_construction_order=node_path_mock.publisher_construction_order) == \
            node_path_mock

        with pytest.raises(ItemNotFoundError) as e:
            app.get_node_path(
                node_path_mock.node_name,
                node_path_mock.subscribe_topic_name,
                node_path_mock.publish_topic_name,
                subscription_construction_order=0,
                publisher_construction_order=1)
        assert 'publisher_construction_order' in str(e.value)

    def test_get_callbacks(self, mocker):
        # define mocks
        arch_mock = mocker.Mock(spec=Architecture)
        records_provider_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        records_provider_mock.data = data_model_mock

        # node_mock = mocker.Mock(spec=Node)
        node_mock = mocker.Mock(spec=Node)
        executor_mock = mocker.Mock(spec=Executor)
        path_mock = mocker.Mock(spec=Path)
        comm_mock = mocker.Mock(spec=Communication)
        callback_mock0 = mocker.Mock(spec=CallbackBase)
        callback_mock1 = mocker.Mock(spec=CallbackBase)
        records_assigned_mock = mocker.Mock(spec=RuntimeLoaded)

        # patch mocks
        mocker.patch.object(callback_mock0, 'callback_name', 'cb_abcdefg')
        mocker.patch.object(callback_mock1, 'callback_name', 'cb_b')
        mocker.patch.object(node_mock, 'node_name', 'node_name_')
        mocker.patch.object(node_mock, 'callbacks', [callback_mock0, callback_mock1])
        mocker.patch.object(path_mock, 'path_name', 'path_name_')
        mocker.patch.object(comm_mock, 'publish_node_name',
                            'publish_node_name_')
        mocker.patch.object(
            comm_mock, 'subscribe_node_name', 'subscribe_node_name_')
        mocker.patch.object(
            comm_mock, 'topic_name', 'topic_name')

        mocker.patch('caret_analyze.runtime.application.RuntimeLoaded',
                     return_value=records_assigned_mock)

        mocker.patch.object(records_assigned_mock, 'nodes', [node_mock])
        mocker.patch.object(records_assigned_mock,
                            'executors', [executor_mock])
        mocker.patch.object(records_assigned_mock, 'paths', [path_mock])
        mocker.patch.object(records_assigned_mock,
                            'communications', [comm_mock])

        app = Application(arch_mock, records_provider_mock)

        assert app.get_callbacks('cb*') == [callback_mock0, callback_mock1]
        assert app.get_callbacks('*') == [callback_mock0, callback_mock1]
        assert app.get_callbacks('cbb*') == []
        assert app.get_callbacks('cb_?') == [callback_mock1]
