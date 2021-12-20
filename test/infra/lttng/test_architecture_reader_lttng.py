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


from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.architecture_reader_lttng import \
    ArchitectureReaderLttng
from caret_analyze.value_objects import (CallbackGroupValue, ExecutorValue,
                                         NodeValueWithId, PublisherValue,
                                         SubscriptionCallbackValue,
                                         TimerCallbackValue)

from pytest_mock import MockerFixture


class TestArchitectureReaderLttng:

    def test_get_nodes(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_nodes', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_nodes() == []

        node = NodeValueWithId('node_name', 'node_id')
        mocker.patch.object(lttng_mock, 'get_nodes', return_value=[node])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_nodes() == [node]

    def test_get_publishers(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_publishers(node) == []

        pub_mock = mocker.Mock(spec=PublisherValue)
        mocker.patch.object(lttng_mock, 'get_publishers', return_value=[pub_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_publishers(node) == [pub_mock]

    def test_get_timer_callbacks(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')

        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_timer_callbacks(node) == []

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(lttng_mock, 'get_timer_callbacks', return_value=[timer_cb_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_timer_callbacks(node) == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_subscription_callbacks(node) == []

        subscription_cb_mock = mocker.Mock(spec=SubscriptionCallbackValue)
        mocker.patch.object(
            lttng_mock, 'get_subscription_callbacks', return_value=[subscription_cb_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_subscription_callbacks(node) == [subscription_cb_mock]

    def test_get_executors(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_executors', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == []

        exec_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_mock, 'get_executors', return_value=[exec_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == [exec_mock]

    def test_get_variable_passings(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        node = NodeValueWithId('node_name', 'node_id')
        assert reader.get_variable_passings(node) == []

    def test_get_named_paths(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_paths() == []

    def test_get_subscriptions(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')

        mocker.patch.object(
            lttng_mock,
            'get_subscription_callbacks',
            return_value=[])
        node_ = NodeValueWithId('node_name', 'node_id')
        assert reader.get_subscriptions(node_) == []

        node = ['node0', 'node1']
        node_id = ['node0_id', 'node1_id']
        topic = ['topic0', 'topic1']
        symbol = ['symbol0', 'symbol1']
        callback_id = ['callback0', 'callback1']

        sub_cb_0 = SubscriptionCallbackValue(
            callback_id[0], node[0], node_id[0], symbol[0], topic[0], None)
        sub_cb_1 = SubscriptionCallbackValue(
            callback_id[1], node[1], node_id[1], symbol[1], topic[1], None)

        mocker.patch.object(
            lttng_mock,
            'get_subscription_callbacks',
            return_value=[sub_cb_0, sub_cb_1])

        subs = reader.get_subscriptions(node_)
        for i, sub in enumerate(subs):
            assert sub.node_name == node[i]
            assert sub.node_id == node_id[i]
            assert sub.topic_name == topic[i]
            assert sub.callback_id == callback_id[i]

    def test_get_callback_groups(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        mocker.patch.object(lttng_mock, 'get_callback_groups', return_value=[])
        node_ = NodeValueWithId('node_name', 'node_id')
        assert reader.get_callback_groups(node_) == []

        cbg = mocker.Mock(spec=CallbackGroupValue)

        mocker.patch.object(
            lttng_mock,
            'get_callback_groups',
            return_value=[cbg])

        cbgs = reader.get_callback_groups(node_)
        assert cbgs == [cbg]
