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


from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.architecture_reader_lttng import \
    ArchitectureReaderLttng
from caret_analyze.value_objects import (CallbackGroupValue, ExecutorValue,
                                         NodeValueWithId, PublisherValue,
                                         ServiceCallbackValue, ServiceStructValue,
                                         SubscriptionCallbackValue, SubscriptionValue,
                                         TimerCallbackValue)


class TestArchitectureReaderLttng:

    def test_get_nodes(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_nodes', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_nodes() == []

        node = NodeValueWithId('node_name', 'node_id')
        mocker.patch.object(lttng_mock, 'get_nodes', return_value=[node])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_nodes() == [node]

    def test_get_publishers(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_publishers(node) == []

        pub_mock = mocker.Mock(spec=PublisherValue)
        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[pub_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_publishers(node) == [pub_mock]

    def test_get_timer_callbacks(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')

        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_timer_callbacks(node) == []

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[timer_cb_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_timer_callbacks(node) == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_subscription_callbacks(node) == []

        subscription_cb_mock = mocker.Mock(spec=SubscriptionCallbackValue)
        mocker.patch.object(
            lttng_mock, 'get_subscription_callbacks', return_value=[subscription_cb_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_subscription_callbacks(node) == [subscription_cb_mock]

    def test_get_service_callbacks(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_service_callbacks', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_service_callbacks(node) == []

        service_cb_mock = mocker.Mock(spec=ServiceCallbackValue)
        mocker.patch.object(
            lttng_mock, 'get_service_callbacks', return_value=[service_cb_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_service_callbacks(node) == [service_cb_mock]

    def test_get_executors(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_executors', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == []

        exec_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_mock, 'get_executors', return_value=[exec_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == [exec_mock]

    def test_get_variable_passings(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_variable_passings(node) == []

    def test_get_named_paths(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_paths() == []

    def test_get_subscriptions(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_subscriptions', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_subscriptions(node) == []

        sub_mock = mocker.Mock(spec=SubscriptionValue)
        mocker.patch.object(lttng_mock, 'get_subscriptions', return_value=[sub_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_subscriptions(node) == [sub_mock]

    def test_get_callback_groups(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        mocker.patch.object(lttng_mock, 'get_callback_groups', return_value=[])
        node_ = NodeValueWithId('node_name', 'node_id')
        assert reader.get_callback_groups(node_) == []

        cbg = mocker.Mock(spec=CallbackGroupValue)

        mocker.patch.object(
            lttng_mock,
            'get_callback_groups',
            return_value=[cbg])

        callback_groups = reader.get_callback_groups(node_)
        assert callback_groups == [cbg]

    def test_get_services(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_services', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.architecture_reader_lttng.Lttng',
                     return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_services(node) == []

        srv_mock = mocker.Mock(spec=ServiceStructValue)
        mocker.patch.object(lttng_mock, 'get_services', return_value=[srv_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_services(node) == [srv_mock]
