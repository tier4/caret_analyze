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


from pytest_mock import MockerFixture

from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.architecture_reader_lttng import \
    ArchitectureReaderLttng
from caret_analyze.value_objects import (CallbackGroupValue,
                                         CallbackGroupType,
                                         SubscriptionCallbackValue,
                                         TimerCallbackValue,
                                         ExecutorValue,
                                         ExecutorType,
                                         PublisherValue)

class TestArchitectureReaderLttng:


    def test_get_node_names(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_node_names', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_nodes() == []

        mocker.patch.object(lttng_mock, 'get_node_names', return_value=['node'])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_nodes() == ['node']

    def test_get_publishers_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_publishers_info', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_publishers('node') == []

        pub_mock = mocker.Mock(spec=PublisherValue)
        mocker.patch.object(lttng_mock, 'get_publishers_info', return_value=[pub_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_publishers('node') == [pub_mock]

    def test_get_timer_callbacks_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_timer_callbacks_info', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_timer_callbacks('node') == []

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(lttng_mock, 'get_timer_callbacks_info', return_value=[timer_cb_mock])
        reader = ArchitectureReaderLttng(lttng_mock)
        assert reader.get_timer_callbacks('node') == [timer_cb_mock]

    def test_get_subscription_callbacks_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_subscription_callbacks_info', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_subscription_callbacks('node') == []

        subscription_cb_mock = mocker.Mock(spec=SubscriptionCallbackValue)
        mocker.patch.object(lttng_mock, 'get_subscription_callbacks_info', return_value=[subscription_cb_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_subscription_callbacks('node') == [subscription_cb_mock]

    def test_get_executors_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)

        mocker.patch.object(lttng_mock, 'get_executors_info', return_value=[])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == []

        exec_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_mock, 'get_executors_info', return_value=[exec_mock])
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_executors() == [exec_mock]

    def test_get_variable_passings_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_variable_passings('node') == []

    def test_get_named_paths_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        assert reader.get_named_paths() == []

    def test_get_subscriptions_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')

        mocker.patch.object(
            lttng_mock,
            'get_subscription_callbacks_info',
            return_value=[])
        assert reader.get_subscriptions('') == []

        node = ['node0', 'node1']
        topic = ['topic0', 'topic1']
        symbol = ['symbol0', 'symbol1']
        callback_id = ['callback0', 'callback1']

        sub_cb_0 = SubscriptionCallbackValue(callback_id[0], node[0], symbol[0], topic[0], None)
        sub_cb_1 = SubscriptionCallbackValue(callback_id[1], node[1], symbol[1], topic[1], None)

        mocker.patch.object(
            lttng_mock,
            'get_subscription_callbacks_info',
            return_value=[sub_cb_0, sub_cb_1])

        subs = reader.get_subscriptions('')
        for i, sub in enumerate(subs):
            assert sub.node_name == node[i]
            assert sub.topic_name == topic[i]
            assert sub.callback_id == callback_id[i]

    def test_get_callback_groups_info(self, mocker: MockerFixture):
        lttng_mock = mocker.Mock(spec=Lttng)
        mocker.patch('caret_analyze.infra.lttng.lttng.Lttng', return_value=lttng_mock)
        reader = ArchitectureReaderLttng('trace_dir')
        mocker.patch.object(lttng_mock, 'get_executors_info', return_value=[])
        assert reader.get_callback_groups('') == []

        node = ['node0', 'node1']
        callback = ['callback0', 'callback1']

        cbg_0 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node[0],
            (callback[0],)
        )
        cbg_1 = CallbackGroupValue(
            CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
            node[1],
            (callback[1],)
        )

        executor = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            (cbg_0, cbg_1)
        )

        mocker.patch.object(
            lttng_mock,
            'get_executors_info',
            return_value=[executor])

        cbgs = reader.get_callback_groups('node0')
        assert len(cbgs) == 1
        cbg = cbgs[0]
        assert cbg.node_name == node[0]
        assert cbg.callback_ids == (callback[0],)
        assert cbg.callback_group_type == CallbackGroupType.MUTUALLY_EXCLUSIVE
