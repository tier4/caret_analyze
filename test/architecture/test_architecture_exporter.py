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

from typing import Dict, List

from caret_analyze.architecture.architecture_exporter import (ArchitectureExporter,
                                                              CallbackDicts,
                                                              ExecutorsDicts,
                                                              NamedPathsDicts,
                                                              NodesDicts,
                                                              PubDicts,
                                                              SubDicts,
                                                              VarPassDicts)
from caret_analyze.architecture.reader_interface import UNDEFINED_STR
from caret_analyze.exceptions import UnsupportedTypeError
from caret_analyze.value_objects import (CallbackGroupStructValue,
                                         CallbackStructValue,
                                         ExecutorStructValue,
                                         NodePathStructValue, NodeStructValue,
                                         PathStructValue, PublisherStructValue,
                                         SubscriptionCallbackStructValue,
                                         SubscriptionStructValue,
                                         TimerCallbackStructValue,
                                         VariablePassingStructValue)

import pytest
from pytest_mock import MockerFixture


class TestArchitectureExporter:

    def test_empty(self, mocker: MockerFixture):
        exporter = ArchitectureExporter((), (), ())

        named_path_mock = mocker.Mock(spec=NamedPathsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NamedPathsDicts',
                     return_value=named_path_mock)
        mocker.patch.object(named_path_mock, 'data', [])

        exec_mock = mocker.Mock(spec=ExecutorsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.ExecutorsDicts',
                     return_value=exec_mock)
        mocker.patch.object(exec_mock, 'data', [])

        node_mock = mocker.Mock(spec=NodesDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NodesDicts',
                     return_value=node_mock)
        mocker.patch.object(node_mock, 'data', [])

        expected: Dict[str, List] = {
            'named_paths': [],
            'executors': [],
            'nodes': []
        }
        assert exporter.to_dict() == expected

    def test_full(self, mocker: MockerFixture):
        exporter = ArchitectureExporter((), (), ())

        named_path_mock = mocker.Mock(spec=NamedPathsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NamedPathsDicts',
                     return_value=named_path_mock)
        mocker.patch.object(named_path_mock, 'data', ['path_dict'])

        exec_mock = mocker.Mock(spec=ExecutorsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.ExecutorsDicts',
                     return_value=exec_mock)
        mocker.patch.object(exec_mock, 'data', ['exec_dict'])

        node_mock = mocker.Mock(spec=NodesDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NodesDicts',
                     return_value=node_mock)
        mocker.patch.object(node_mock, 'data', ['node_dict'])

        expected = {
            'named_paths': ['path_dict'],
            'executors': ['exec_dict'],
            'nodes': ['node_dict']
        }
        assert exporter.to_dict() == expected

    def test_str(self, mocker: MockerFixture):
        exporter = ArchitectureExporter((), (), ())

        named_path_mock = mocker.Mock(spec=NamedPathsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NamedPathsDicts',
                     return_value=named_path_mock)
        mocker.patch.object(named_path_mock, 'data', [])

        exec_mock = mocker.Mock(spec=ExecutorsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.ExecutorsDicts',
                     return_value=exec_mock)
        mocker.patch.object(exec_mock, 'data', [])

        node_mock = mocker.Mock(spec=NodesDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NodesDicts',
                     return_value=node_mock)
        mocker.patch.object(node_mock, 'data', [])

        expected = \
            """named_paths: []
executors: []
nodes: []
"""
        assert str(exporter) == expected

    def test_force_option(self, mocker: MockerFixture, tmpdir):
        exporter = ArchitectureExporter((), (), ())
        exporter_force = ArchitectureExporter((), (), (), force=True)

        named_path_mock = mocker.Mock(spec=NamedPathsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NamedPathsDicts',
                     return_value=named_path_mock)
        mocker.patch.object(named_path_mock, 'data', [])

        exec_mock = mocker.Mock(spec=ExecutorsDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.ExecutorsDicts',
                     return_value=exec_mock)
        mocker.patch.object(exec_mock, 'data', [])

        node_mock = mocker.Mock(spec=NodesDicts)
        mocker.patch('caret_analyze.architecture.architecture_exporter.NodesDicts',
                     return_value=node_mock)
        mocker.patch.object(node_mock, 'data', [])

        f = tmpdir.mkdir('sub').join('arch.yaml')
        exporter.execute(f.strpath)

        with pytest.raises(FileExistsError):
            exporter.execute(f)

        exporter_force.execute(f)


class TestNamedPathsDicts:

    def test_empty(self):
        path_dict = NamedPathsDicts([])
        expect = []
        assert path_dict.data == expect

    def test_full(self, mocker: MockerFixture):

        path_info_mock = mocker.Mock(spec=PathStructValue)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        mocker.patch.object(path_info_mock, 'path_name', 'target_path')
        mocker.patch.object(
            path_info_mock, 'node_paths', [node_path_mock])

        mocker.patch.object(node_path_mock, 'node_name', 'node')
        mocker.patch.object(node_path_mock, 'publish_topic_name', 'pub_topic')
        mocker.patch.object(
            node_path_mock, 'subscribe_topic_name', 'sub_topic')

        path_dict = NamedPathsDicts([path_info_mock])
        expect = [
            {
                'path_name': 'target_path',
                'node_chain': [
                    {
                        'node_name': 'node',
                        'publish_topic_name': 'pub_topic',
                        'subscribe_topic_name': 'sub_topic'
                    }
                ]
            }
        ]
        assert path_dict.data == expect

    def test_undefined(self, mocker: MockerFixture):
        path_info_mock = mocker.Mock(spec=PathStructValue)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        mocker.patch.object(path_info_mock, 'path_name', 'target_path')
        mocker.patch.object(
            path_info_mock, 'node_paths', [node_path_mock])

        mocker.patch.object(node_path_mock, 'node_name', 'node')
        mocker.patch.object(node_path_mock, 'publish_topic_name', None)
        mocker.patch.object(node_path_mock, 'subscribe_topic_name', None)

        path_dict = NamedPathsDicts([path_info_mock])
        expect = [
            {
                'path_name': 'target_path',
                'node_chain': [
                    {
                        'node_name': 'node',
                        'publish_topic_name': UNDEFINED_STR,
                        'subscribe_topic_name': UNDEFINED_STR
                    }
                ]
            }
        ]
        assert path_dict.data == expect


class TestExecutorDicts:

    def test_empty(self):
        exec_dicts = ExecutorsDicts([])
        expect = []
        assert exec_dicts.data == expect

    def test_full(self, mocker: MockerFixture):

        single_threaded_exec_mock = mocker.Mock(spec=ExecutorStructValue)
        multi_threaded_exec_mock = mocker.Mock(spec=ExecutorStructValue)

        mocker.patch.object(single_threaded_exec_mock, 'executor_type_name',
                            'single_threaded_executor')

        exclusive_cbg_mock = mocker.Mock(spec=CallbackGroupStructValue)
        mocker.patch.object(
            exclusive_cbg_mock, 'node_name', '/talker')
        mocker.patch.object(
            exclusive_cbg_mock, 'callback_group_type_name', 'mutually_exclusive')
        mocker.patch.object(
            exclusive_cbg_mock, 'callback_group_name', 'callback_group_1')
        mocker.patch.object(
            exclusive_cbg_mock, 'callback_names', ['/talker/timer_callback_0'])
        mocker.patch.object(
            single_threaded_exec_mock, 'callback_groups',
            [exclusive_cbg_mock])
        mocker.patch.object(
            single_threaded_exec_mock, 'executor_name', 'single_threaded_executor_0')

        reentrant_cbg_mock = mocker.Mock(spec=CallbackGroupStructValue)
        mocker.patch.object(
            reentrant_cbg_mock, 'node_name', '/listener')
        mocker.patch.object(
            reentrant_cbg_mock, 'callback_group_type_name', 'reentrant')
        mocker.patch.object(
            reentrant_cbg_mock, 'callback_group_name', 'callback_group_0')
        mocker.patch.object(
            reentrant_cbg_mock, 'callback_names', ['/listener/subscription_callback_0'])
        mocker.patch.object(
            multi_threaded_exec_mock, 'executor_type_name', 'multi_threaded_executor')
        mocker.patch.object(
            multi_threaded_exec_mock, 'callback_groups', [reentrant_cbg_mock])
        mocker.patch.object(
            multi_threaded_exec_mock, 'executor_name', 'multi_threaded_executor_0')

        exec_dicts = ExecutorsDicts(
            [single_threaded_exec_mock, multi_threaded_exec_mock]
        )

        expect = [
            {
                'executor_type': 'multi_threaded_executor',
                'executor_name': 'multi_threaded_executor_0',
                'callback_group_names': [
                    'callback_group_0',
                ]
            },
            {
                'executor_type': 'single_threaded_executor',
                'executor_name': 'single_threaded_executor_0',
                'callback_group_names': [
                    'callback_group_1'
                ]
            },
        ]

        assert exec_dicts.data == expect

    def test_name_sort(self, mocker: MockerFixture):
        exec_0 = mocker.Mock(spec=ExecutorStructValue)
        exec_1 = mocker.Mock(spec=ExecutorStructValue)
        exec_2 = mocker.Mock(spec=ExecutorStructValue)
        exec_3 = mocker.Mock(spec=ExecutorStructValue)

        mocker.patch.object(exec_0, 'executor_name', 'multi_threaded_executor_0')
        mocker.patch.object(exec_1, 'executor_name', 'multi_threaded_executor_1')
        mocker.patch.object(exec_2, 'executor_name', 'single_threaded_executor_0')
        mocker.patch.object(exec_3, 'executor_name', 'single_threaded_executor_1')

        mocker.patch.object(exec_0, 'executor_type_name', 'multi_threaded_executor')
        mocker.patch.object(exec_1, 'executor_type_name', 'multi_threaded_executor')
        mocker.patch.object(exec_2, 'executor_type_name', 'single_threaded_executor')
        mocker.patch.object(exec_3, 'executor_type_name', 'single_threaded_executor')

        mocker.patch.object(exec_0, 'callback_groups', [])
        mocker.patch.object(exec_1, 'callback_groups', [])
        mocker.patch.object(exec_2, 'callback_groups', [])
        mocker.patch.object(exec_3, 'callback_groups', [])

        exec_dicts = ExecutorsDicts(
            [exec_3, exec_0, exec_1, exec_2]
        )

        expect = [
            {
                'executor_name': 'multi_threaded_executor_0',
                'executor_type': 'multi_threaded_executor',
                'callback_group_names': []
            },
            {
                'executor_name': 'multi_threaded_executor_1',
                'executor_type': 'multi_threaded_executor',
                'callback_group_names': []
            },
            {
                'executor_name': 'single_threaded_executor_0',
                'executor_type': 'single_threaded_executor',
                'callback_group_names': []
            },
            {
                'executor_name': 'single_threaded_executor_1',
                'executor_type': 'single_threaded_executor',
                'callback_group_names': []
            },
        ]

        assert exec_dicts.data == expect


class TestNodeDicts:

    def test_empty(self):
        node_dict = NodesDicts([])
        assert node_dict.data == []

    def test_callbacks(self, mocker: MockerFixture):
        node_info = mocker.Mock(spec=NodeStructValue)

        callback_dict_mock = mocker.Mock(spec=CallbackDicts)
        mocker.patch.object(callback_dict_mock, 'data', ['callback_dict'])
        mocker.patch('caret_analyze.architecture.architecture_exporter.CallbackDicts',
                     return_value=callback_dict_mock)

        mocker.patch.object(node_info, 'callbacks', [callback_dict_mock])
        mocker.patch.object(node_info, 'variable_passings', [])
        mocker.patch.object(node_info, 'publishers', [])
        mocker.patch.object(node_info, 'subscriptions', [])
        mocker.patch.object(node_info, 'callback_groups', [])
        mocker.patch.object(node_info, 'node_name', 'node')

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_name': 'node',
                'callbacks': ['callback_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_variable_passings(self, mocker: MockerFixture):
        node_info = mocker.Mock(spec=NodeStructValue)

        callback_dict_mock = mocker.Mock(spec=CallbackDicts)
        mocker.patch.object(callback_dict_mock, 'data', ['callback_dict', 'callback_dict'])
        mocker.patch('caret_analyze.architecture.architecture_exporter.CallbackDicts',
                     return_value=callback_dict_mock)

        var_pass_dicts_mock = mocker.Mock(spec=VarPassDicts)
        mocker.patch.object(var_pass_dicts_mock, 'data', ['var_pass_dict'])
        mocker.patch('caret_analyze.architecture.architecture_exporter.VarPassDicts',
                     return_value=var_pass_dicts_mock)

        mocker.patch.object(node_info, 'callbacks', [callback_dict_mock, callback_dict_mock])
        mocker.patch.object(node_info, 'callback_groups', [])
        mocker.patch.object(node_info, 'variable_passings', [var_pass_dicts_mock])
        mocker.patch.object(node_info, 'publishers', [])
        mocker.patch.object(node_info, 'subscriptions', [])
        mocker.patch.object(node_info, 'node_name', 'node')

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_name': 'node',
                'callbacks': ['callback_dict', 'callback_dict'],
                'callback_groups': [],
                'variable_passings': ['var_pass_dict'],
            }
        ]
        assert node_dict.data == expect

    def test_publish(self, mocker: MockerFixture):
        node_info = mocker.Mock(spec=NodeStructValue)

        pub_dict_mock = mocker.Mock(spec=PubDicts)
        mocker.patch.object(pub_dict_mock, 'data', ['pub_dict'])
        mocker.patch('caret_analyze.architecture.architecture_exporter.PubDicts',
                     return_value=pub_dict_mock)

        mocker.patch.object(node_info, 'callbacks', [])
        mocker.patch.object(node_info, 'variable_passings', [])
        mocker.patch.object(node_info, 'publishers', [pub_dict_mock])
        mocker.patch.object(node_info, 'callback_groups', [])
        mocker.patch.object(node_info, 'subscriptions', [])
        mocker.patch.object(node_info, 'node_name', 'node')

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_name': 'node',
                'publishes': ['pub_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_subscription(self, mocker: MockerFixture):
        node_info = mocker.Mock(spec=NodeStructValue)

        sub_dict_mock = mocker.Mock(spec=SubDicts)
        mocker.patch.object(sub_dict_mock, 'data', ['sub_dict'])
        mocker.patch('caret_analyze.architecture.architecture_exporter.SubDicts',
                     return_value=sub_dict_mock)

        mocker.patch.object(node_info, 'callbacks', [])
        mocker.patch.object(node_info, 'variable_passings', [])
        mocker.patch.object(node_info, 'publishers', [])
        mocker.patch.object(node_info, 'subscriptions', [sub_dict_mock])
        mocker.patch.object(node_info, 'node_name', 'node')
        mocker.patch.object(node_info, 'callback_groups', [])

        node_dict = NodesDicts([node_info])

        expect = [
            {
                'node_name': 'node',
                'subscribes': ['sub_dict'],
                'callback_groups': []
            }
        ]
        assert node_dict.data == expect

    def test_name_sort(self, mocker: MockerFixture):
        node_0 = mocker.Mock(spec=NodeStructValue)
        node_1 = mocker.Mock(spec=NodeStructValue)
        node_2 = mocker.Mock(spec=NodeStructValue)
        node_3 = mocker.Mock(spec=NodeStructValue)

        mocker.patch.object(node_0, 'node_name', 'node_0')
        mocker.patch.object(node_1, 'node_name', 'node_1')
        mocker.patch.object(node_2, 'node_name', 'node_2')
        mocker.patch.object(node_3, 'node_name', 'node_3')

        mocker.patch.object(node_0, 'callbacks', [])
        mocker.patch.object(node_1, 'callbacks', [])
        mocker.patch.object(node_2, 'callbacks', [])
        mocker.patch.object(node_3, 'callbacks', [])

        mocker.patch.object(node_0, 'callback_groups', [])
        mocker.patch.object(node_1, 'callback_groups', [])
        mocker.patch.object(node_2, 'callback_groups', [])
        mocker.patch.object(node_3, 'callback_groups', [])

        mocker.patch.object(node_0, 'publishers', [])
        mocker.patch.object(node_1, 'publishers', [])
        mocker.patch.object(node_2, 'publishers', [])
        mocker.patch.object(node_3, 'publishers', [])

        mocker.patch.object(node_0, 'subscriptions', [])
        mocker.patch.object(node_1, 'subscriptions', [])
        mocker.patch.object(node_2, 'subscriptions', [])
        mocker.patch.object(node_3, 'subscriptions', [])

        node_dicts = NodesDicts([node_2, node_3, node_1, node_0])

        expect = [
            {'node_name': 'node_0', 'callback_groups': []},
            {'node_name': 'node_1', 'callback_groups': []},
            {'node_name': 'node_2', 'callback_groups': []},
            {'node_name': 'node_3', 'callback_groups': []},
        ]

        assert node_dicts.data == expect


class TestSubDicts:

    def test_content(self, mocker: MockerFixture):
        sub_info = mocker.Mock(spec=SubscriptionStructValue)
        mocker.patch.object(sub_info, 'topic_name', 'topic')
        mocker.patch.object(sub_info, 'callback_name', 'callback')
        sub_dict = SubDicts((sub_info,))

        expect = [{
            'topic_name': 'topic',
            'callback_name': 'callback'
        }]

        assert sub_dict.data == expect

    def test_callback_None(self, mocker: MockerFixture):
        sub_info = mocker.Mock(spec=SubscriptionStructValue)
        mocker.patch.object(sub_info, 'topic_name', 'topic')
        mocker.patch.object(sub_info, 'callback_name', None)

        sub_dict = SubDicts((sub_info,))
        expect = [{
            'topic_name': 'topic',
            'callback_name': UNDEFINED_STR
        }]

        assert sub_dict.data == expect

    def test_name_sort(self, mocker: MockerFixture):
        sub_mock_0 = mocker.Mock(spec=SubscriptionStructValue)
        sub_mock_1 = mocker.Mock(spec=SubscriptionStructValue)
        sub_mock_2 = mocker.Mock(spec=SubscriptionStructValue)

        mocker.patch.object(sub_mock_0, 'topic_name', 'A')
        mocker.patch.object(sub_mock_1, 'topic_name', 'B')
        mocker.patch.object(sub_mock_2, 'topic_name', 'C')

        mocker.patch.object(sub_mock_0, 'callback_name', None)
        mocker.patch.object(sub_mock_1, 'callback_name', None)
        mocker.patch.object(sub_mock_2, 'callback_name', None)

        sub_dict = SubDicts((sub_mock_1, sub_mock_2, sub_mock_0,))

        expect = [
            {'topic_name': 'A', 'callback_name': UNDEFINED_STR},
            {'topic_name': 'B', 'callback_name': UNDEFINED_STR},
            {'topic_name': 'C', 'callback_name': UNDEFINED_STR},
        ]

        assert sub_dict.data == expect


class TestPubDicts:

    def test_content(self, mocker: MockerFixture):
        pub_info = mocker.Mock(spec=PublisherStructValue)
        mocker.patch.object(pub_info, 'topic_name', 'topic')
        mocker.patch.object(pub_info, 'callback_names', ('callback',))
        pub_dict = PubDicts((pub_info,))

        expect = [{
            'topic_name': 'topic',
            'callback_names': ['callback'],
        }]

        assert pub_dict.data == expect

    def test_callback_none(self, mocker: MockerFixture):
        pub_info = mocker.Mock(spec=PublisherStructValue)
        mocker.patch.object(pub_info, 'topic_name', 'topic')
        mocker.patch.object(pub_info, 'callback_names', None)
        pub_dict = PubDicts((pub_info,))

        expect = [{
            'topic_name': 'topic',
            'callback_names': [UNDEFINED_STR],
        }]

        assert pub_dict.data == expect

    def test_name_sort(self, mocker: MockerFixture):
        pub_mock_0 = mocker.Mock(spec=PublisherStructValue)
        pub_mock_1 = mocker.Mock(spec=PublisherStructValue)
        pub_mock_2 = mocker.Mock(spec=PublisherStructValue)

        mocker.patch.object(pub_mock_0, 'topic_name', 'A')
        mocker.patch.object(pub_mock_1, 'topic_name', 'B')
        mocker.patch.object(pub_mock_2, 'topic_name', 'C')

        mocker.patch.object(pub_mock_0, 'callback_names', None)
        mocker.patch.object(pub_mock_1, 'callback_names', None)
        mocker.patch.object(pub_mock_2, 'callback_names', None)

        pub_dict = PubDicts((pub_mock_1, pub_mock_2, pub_mock_0))

        expect = [
            {'topic_name': 'A', 'callback_names': [UNDEFINED_STR]},
            {'topic_name': 'B', 'callback_names': [UNDEFINED_STR]},
            {'topic_name': 'C', 'callback_names': [UNDEFINED_STR]},
        ]

        assert pub_dict.data == expect


class TestCallbackDicts:

    def test_timer_callback(self, mocker: MockerFixture):
        callback_mock = mocker.Mock(spec=TimerCallbackStructValue)

        period_ns = 3
        symbol = 'symbol'
        callback_name = 'callback'

        mocker.patch.object(callback_mock, 'callback_name', callback_name)
        mocker.patch.object(callback_mock, 'period_ns', period_ns)
        mocker.patch.object(callback_mock, 'symbol', symbol)
        callback_dict = CallbackDicts((callback_mock,))

        expect = [{
            'callback_name': callback_name,
            'callback_type': 'timer_callback',
            'period_ns': period_ns,
            'symbol': symbol
        }]

        assert callback_dict.data == expect

    def test_subscription_callback(self, mocker: MockerFixture):
        callback_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)

        topic_name = 'topic'
        symbol = 'symbol'
        callback_name = 'callback'

        mocker.patch.object(callback_mock, 'callback_name', callback_name)
        mocker.patch.object(callback_mock, 'subscribe_topic_name', topic_name)
        mocker.patch.object(callback_mock, 'symbol', symbol)
        callback_dict = CallbackDicts((callback_mock,))

        expect = [{
            'callback_name': callback_name,
            'callback_type': 'subscription_callback',
            'topic_name': topic_name,
            'symbol': symbol
        }]

        assert callback_dict.data == expect

    def test_unsupported_type_error(self, mocker: MockerFixture):
        callback_mock = mocker.Mock(spec=CallbackStructValue)
        with pytest.raises(UnsupportedTypeError):
            CallbackDicts((callback_mock,))

    def test_name_sort(self, mocker: MockerFixture):

        callback_mock_0 = mocker.Mock(spec=SubscriptionCallbackStructValue)
        callback_mock_1 = mocker.Mock(spec=SubscriptionCallbackStructValue)
        callback_mock_2 = mocker.Mock(spec=SubscriptionCallbackStructValue)

        mocker.patch.object(callback_mock_0, 'callback_name', 'callback_0')
        mocker.patch.object(callback_mock_1, 'callback_name', 'callback_1')
        mocker.patch.object(callback_mock_2, 'callback_name', 'callback_2')

        mocker.patch.object(callback_mock_0, 'subscribe_topic_name', None)
        mocker.patch.object(callback_mock_1, 'subscribe_topic_name', None)
        mocker.patch.object(callback_mock_2, 'subscribe_topic_name', None)

        mocker.patch.object(callback_mock_0, 'symbol', '')
        mocker.patch.object(callback_mock_1, 'symbol', '')
        mocker.patch.object(callback_mock_2, 'symbol', '')

        callback_dict = CallbackDicts((callback_mock_2, callback_mock_1, callback_mock_0))

        expect = [
            {
                'callback_name': 'callback_0',
                'callback_type': 'subscription_callback',
                'topic_name': None,
                'symbol': ''
            },
            {
                'callback_name': 'callback_1',
                'callback_type': 'subscription_callback',
                'topic_name': None,
                'symbol': ''
            },
            {
                'callback_name': 'callback_2',
                'callback_type': 'subscription_callback',
                'topic_name': None,
                'symbol': ''
            }
        ]

        assert callback_dict.data == expect


class TestVarPassDicts:

    def test_empty(self):
        var_pass_dicts = VarPassDicts([])
        expect = [{
            'callback_name_write': UNDEFINED_STR,
            'callback_name_read': UNDEFINED_STR,
        }]

        assert var_pass_dicts.data == expect

        var_pass_dicts = VarPassDicts(None)
        expect = [{
            'callback_name_write': UNDEFINED_STR,
            'callback_name_read': UNDEFINED_STR,
        }]

        assert var_pass_dicts.data == expect

    def test_full(self, mocker: MockerFixture):
        var_pass_mock = mocker.Mock(spec=VariablePassingStructValue)
        callback_write_name = 'callback0'
        callback_read_name = 'callback1'
        mocker.patch.object(var_pass_mock, 'callback_name_write', callback_write_name)
        mocker.patch.object(var_pass_mock, 'callback_name_read', callback_read_name)
        var_pass_dicts = VarPassDicts((var_pass_mock,))

        expect = [{
            'callback_name_write': callback_write_name,
            'callback_name_read': callback_read_name,
        }]

        assert var_pass_dicts.data == expect
