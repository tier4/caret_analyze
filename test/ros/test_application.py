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

import pytest
from pytest_mock import MockerFixture

from caret_analyze.architecture.architecture import Architecture
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.infra.interface import RecordsProvider
from caret_analyze.runtime.application import Application
from caret_analyze.runtime.callback import CallbackBase
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.executor import Executor
from caret_analyze.runtime.node import Node
from caret_analyze.runtime.path import Path
from caret_analyze.runtime.runtime_loaded import RuntimeLoaded
from caret_analyze.infra.lttng import Lttng


class TestApplication:

    def test_empty_architecture(self, mocker: MockerFixture):
        arch_mock = mocker.Mock(spec=Architecture)

        assigned_mock = mocker.Mock(spec=RuntimeLoaded)
        mocker.patch.object(assigned_mock, 'nodes', [])
        mocker.patch.object(assigned_mock, 'executors', [])
        mocker.patch.object(assigned_mock, 'named_paths', [])
        mocker.patch.object(assigned_mock, 'communications', [])
        mocker.patch(
            'caret_analyze.runtime.runtime_loaded.RuntimeLoaded', return_value=assigned_mock)
        records_provider_mock = mocker.Mock(spec=Lttng)
        app = Application(arch_mock, records_provider_mock)

        assert len(app.named_paths) == 0
        assert len(app.nodes) == 0
        assert len(app.executors) == 0
        assert len(app.communications) == 0
        assert len(app.callbacks) == 0

        with pytest.raises(ItemNotFoundError):
            app.get_named_path('')

        with pytest.raises(ItemNotFoundError):
            app.get_node('')

        with pytest.raises(ItemNotFoundError):
            app.get_callback('')

        with pytest.raises(ItemNotFoundError):
            app.get_communication('', '', '')

    def test_full_architecture(self, mocker: MockerFixture):
        # define mocks
        arch_mock = mocker.Mock(spec=Architecture)
        records_provider_mock = mocker.Mock(spec=Lttng)

        node_mock = mocker.Mock(spec=Node)
        executor_mock = mocker.Mock(spec=Executor)
        path_mock = mocker.Mock(spec=Path)
        comm_mock = mocker.Mock(spec=Communication)
        callback_mock = mocker.Mock(spec=CallbackBase)
        records_assigned_mock = mocker.Mock(spec=RuntimeLoaded)

        # patch mocks
        mocker.patch.object(
            callback_mock, 'callback_name', 'callback_name_')
        mocker.patch.object(node_mock, 'node_name', 'node_name_')
        mocker.patch.object(node_mock, 'callbacks', [callback_mock])

        mocker.patch.object(path_mock, 'path_name', 'path_name_')
        mocker.patch.object(comm_mock, 'publish_node_name',
                            'publish_node_name_')
        mocker.patch.object(
            comm_mock, 'subscribe_node_name', 'subscribe_node_name_')

        mocker.patch('caret_analyze.runtime.runtime_loaded.RuntimeLoaded',
                     return_value=records_assigned_mock)

        mocker.patch.object(records_assigned_mock, 'nodes', [node_mock])
        mocker.patch.object(records_assigned_mock,
                            'executors', [executor_mock])
        mocker.patch.object(records_assigned_mock, 'named_paths', [path_mock])
        mocker.patch.object(records_assigned_mock,
                            'communications', [comm_mock])

        # test senario
        app = Application(arch_mock, records_provider_mock)

        assert len(app.named_paths) == 1
        assert len(app.nodes) == 1
        assert len(app.executors) == 1
        assert len(app.communications) == 1
        assert len(app.callbacks) == 1

        assert app.get_named_path(path_mock.path_name) == path_mock
        assert app.get_node(node_mock.node_name) == node_mock
        assert app.get_callback(
            callback_mock.callback_name) == callback_mock
        assert app.get_communication(
            comm_mock.publish_node_name,
            comm_mock.subscribe_node_name,
            comm_mock.topic_name) == comm_mock

    # @pytest.mark.parametrize(
    #     'trace_path, yaml_path, start_cb_name, end_cb_name, paths_len',
    #     [
    #         (
    #             'sample/lttng_samples/talker_listener/',
    #             'sample/lttng_samples/talker_listener/architecture.yaml',
    #             '/talker/timer_callback_0',
    #             '/listener/subscription_callback_0',
    #             1,
    #         ),
    #         (  # cyclic case
    #             'sample/lttng_samples/cyclic_pipeline_intra_process',
    #             'sample/lttng_samples/cyclic_pipeline_intra_process/architecture.yaml',
    #             '/pipe1/subscription_callback_0',
    #             '/pipe1/subscription_callback_0',
    #             2,  # [pipe1 -> pipe2] -> pipe1 and [pipe1]
    #         ),
    #         (
    #             'sample/lttng_samples/end_to_end_sample/fastrtps',
    #             'sample/lttng_samples/end_to_end_sample/architecture.yaml',
    #             '/sensor_dummy_node/timer_callback_0',
    #             '/actuator_dummy_node/subscription_callback_0',
    #             0,
    #         ),
    #         (  # publisher callback_name and callback depencency added
    #             'sample/lttng_samples/end_to_end_sample/fastrtps',
    #             'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml',
    #             '/sensor_dummy_node/timer_callback_0',
    #             '/actuator_dummy_node/subscription_callback_0',
    #             1,
    #         ),
    #         (
    #             'sample/lttng_samples/end_to_end_sample/cyclonedds',
    #             'sample/lttng_samples/end_to_end_sample/architecture.yaml',
    #             '/sensor_dummy_node/timer_callback_0',
    #             '/actuator_dummy_node/subscription_callback_0',
    #             0,
    #         ),
    #         (  # publisher callback_name and callback depencency added
    #             'sample/lttng_samples/end_to_end_sample/cyclonedds',
    #             'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml',
    #             '/sensor_dummy_node/timer_callback_0',
    #             '/actuator_dummy_node/subscription_callback_0',
    #             1,
    #         ),
    #     ],
    # )
    # def test_search_paths(self, trace_path, yaml_path, start_cb_name, end_cb_name, paths_len):
    #     lttng = Lttng(trace_path)
    #     app = Application(yaml_path, 'yaml', lttng)

    #     paths = app.search_paths(start_cb_name, end_cb_name)
    #     assert len(paths) == paths_len

    #     for path in paths:
    #         assert path[0].callback_name == start_cb_name
    #         assert path[-1].callback_name == end_cb_name

    # @pytest.mark.parametrize(
    #     'trace_path, yaml_path, start_cb_name, end_cb_name, paths_len',
    #     [
    #         (
    #             'sample/lttng_samples/talker_listener/',
    #             'sample/lttng_samples/cyclic_pipeline_intra_process/architecture.yaml',
    #             '/talker/timer_callback_0',
    #             '/listener/subscription_callback_0',
    #             0,
    #         ),
    #         (
    #             'sample/lttng_samples/end_to_end_sample/cyclonedds',
    #             'sample/lttng_samples/end_to_end_sample/architecture.yaml',
    #             '/sensor_dummy_node/timer_callback_0',
    #             '/actuator_dummy_node/subscription_callback_0',
    #             0,
    #         ),
    #     ],
    # )
    # def test_architecture_miss_match(
    #     self,
    #     trace_path,
    #     yaml_path,
    #     start_cb_name,
    #     end_cb_name,
    #     paths_len
    # ):
    #     """Test if the architecture file and measurement results do not match."""
    #     trace_path = 'sample/lttng_samples/talker_listener/'
    #     yaml_path = 'sample/lttng_samples/end_to_end_sample/architecture.yaml'
    #     start_cb_name = '/talker/timer_callback_0'
    #     end_cb_name = '/listener/subscription_callback_0'
    #     lttng = Lttng(trace_path)
    #     app = Application(yaml_path, 'yaml', lttng)

    #     paths = app.search_paths(start_cb_name, end_cb_name)
    #     assert len(paths) == paths_len

    # def test_multi_node(self, mocker: MockerFixture):
    #     # define mocks
    #     path_manager_mock = mocker.Mock(spec=PathManager)
    #     arch_mock = mocker.Mock(spec=Architecture)
    #     records_provider_mock = mocker.Mock(spec=RecordsProvider)
    #     path_manager_mock = mocker.Mock(spec=PathManager)

    #     # replace factory method
    #     mocker.patch.object(Architecture, 'create_instance',
    #                         return_value=arch_mock)
    #     mocker.patch.object(PathManager, 'create_instance',
    #                         return_value=path_manager_mock)

    #     # set parameters
    #     topic_name = '/chatter'
    #     node_name = ['node1', '/node2']
    #     symbol = ['symbol1', 'symbol2']
    #     period_ns = 5
    #     topic_name = '/topic_name'
    #     callback_name = ['timer_callback_0', 'subscription_callback_0']

    #     # define objects
    #     publisher = PublisherInfo(node_name[0], topic_name, callback_name[0])
    #     pub_callback = TimerCallback(node_name[0], callback_name[0], symbol[0],
    #                                  period_ns, records_provider_mock,
    #                                  publishers_info=[publisher])
    #     sub_callback = SubscriptionCallback(
    #         records_provider_mock, node_name[1], callback_name[1], symbol[1], topic_name
    #     )
    #     callback_groups = [
    #         CallbackGroup(
    #             CallbackGroupType.MUTUALLY_EXCLUSIVE,
    #             node_name[0],
    #             [pub_callback]
    #         ),
    #         CallbackGroup(
    #             CallbackGroupType.MUTUALLY_EXCLUSIVE,
    #             node_name[1],
    #             [sub_callback]
    #         ),
    #     ]
    #     pub_node = Node(node_name[0], [pub_callback],
    #                     [], [], records_provider_mock)
    #     sub_node = Node(node_name[1], [sub_callback],
    #                     [], [], records_provider_mock)
    #     communication = Communication(
    #         pub_callback, sub_callback, records_provider_mock)
    #     executor = Executor(
    #         ExecutorType.SINGLE_THREADED_EXECUTOR, callback_groups)
    #     path_alias = PathAlias(
    #         'target_path',
    #         [
    #             NodePathInfoCallbackChain(
    #                 pub_callback.node_name, [pub_callback.info]),
    #             NodePathInfoCallbackChain(
    #                 sub_callback.node_name, [sub_callback.info]),
    #         ])

    #     # patch mocks
    #     mocker.patch.object(path_manager_mock,
    #                         'get_named_paths', return_value=[])
    #     mocker.patch.object(path_manager_mock, 'get_named_path',
    #                         side_effect=ItemNotFoundError(''))
    #     mocker.patch.object(PathManager, 'create_instance',
    #                         return_value=path_manager_mock)

    #     mocker.patch.object(arch_mock, 'nodes', [pub_node, sub_node])
    #     mocker.patch.object(arch_mock, 'executors', [executor])
    #     mocker.patch.object(arch_mock, 'path_aliases', [path_alias])
    #     mocker.patch.object(arch_mock, 'communications', [communication])

    #     # mocker.patch.object(
    #     #     records_provider_mock,
    #     #     'compose_intra_process_communication_records', return_value=Records())
    #     # mocker.patch.object(arch_mock, 'path_aliases', [path_alias])
    #     # comm = Communication(pub_callback, sub_callback, records_composer_mock)
    #     # mocker.patch.object(arch_mock, 'communications', [comm])

    #     # test senario
    #     app = Application('filepath', 'yaml', records_provider_mock)

    #     assert len(app.named_paths) == 0
    #     assert len(app.nodes) == 2

    #     assert len(app.executors) == 1
    #     assert app.executors[0] == executor
    #     assert len(app.communications) == 0
    #     assert len(app.callbacks) == 2
    #     assert app.callbacks[0] == pub_callback
    #     assert app.callbacks[1] == sub_callback

    # def test_future(self, mocker: MockerFixture):
    #     # lttng = Lttng('lttng_result_path')
    #     # records_container = RecordsLttng(lttng)
    #     arch_reader = mocker.Mock(spec=ArchitectureReader)
    #     mocker.patch.object(
    #         arch_reader,
    #         'get_node_names',
    #         return_value=['/talker', '/listener'])
    #     # mocker.patch.object(
    #     #     records_container_mock,
    #     #     'compose_intra_process_communication_records',
    #     #     return_value=Records()
    #     # )

    #     def get_timer_callbacks_info(node_name: str):
    #         if node_name == '/talker':
    #             return [TimerCallbackStructInfo('/talker', 'timer_callback_0', 'symbol', 1)]
    #         return []

    #     mocker.patch.object(
    #         arch_reader,
    #         'get_timer_callbacks_info',
    #         return_value=get_timer_callbacks_info
    #     )

    #     def get_subscription_callbacks_info(node_name: str):
    #         if node_name == '/listener':
    #             return [SubscriptionCallbackStructInfo('/listener', 'subscription_callback_0',
    #                     'symbol2', '/chatter')]
    #         return []
    #     mocker.patch.object(
    #         arch_reader,
    #         'get_subscription_callbacks_info',
    #         side_effect=get_subscription_callbacks_info
    #     )

    #     def get_publishers_info(node_name: str):
    #         if node_name == '/talker':
    #             return [PublisherInfo('/talker', '/chatter', 'timer_callback_0')]
    #         return []
    #     mocker.patch.object(
    #         arch_reader,
    #         'get_publishers_info',
    #         side_effect=get_publishers_info
    #     )
    #     mocker.patch.object(
    #         arch_reader, 'get_variable_passings_info', return_value=[])
    #     mocker.patch.object(arch_reader, 'get_path_aliases', return_value=[])

    #     mocker.patch.object(
    #         arch_reader,
    #         'get_executors_info',
    #         return_value=[ExecutorInfo(
    #             ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
    #             [CallbackGroupInfo(
    #                 CallbackGroupType.MUTUALLY_EXCLUSIVE.type_name,
    #                 '/talker',
    #                 ['timer_callback_0']
    #             )]
    #         )]
    #     )

    # def test_architecture_reader(self):
    #     reader = ArchitectureReaderFactory.YamlFile('')
    #     reader.get_node_names()
    #     reader.get_timer_callbacks()
    #     reader.get_subscription_callbacks()
    #     reader.get_publishers()

    # def test_architecture_file_create_senario(self):
    #     lttng = Lttng('')
    #     trace_result = TraceResult(lttng)  # 今後トレースのペアは増える見込みがあるので、ここを抽象化する。
    #     architecture__reader = ArchitectureReaderFactory.lttng(lttng)
    #     architecture = Architecture(architecture_reader)
    #     architecture.export('')

    # def test_set_named_path(self):
    #     lttng = Lttng('')
    #     trace_result = TraceResult(lttng)  # 今後トレースのペアは増える見込みがあるので、ここを抽象化する。
    #     architecture_reader = ArchitectureReaderFactory.YamlFile('')
    #     architecture = Architecture(architecture_file)
    #     architecture = Architecture('', type='')
    #     app = Application(architecture, trace_result)
    #     paths = app.search_paths('', '')
    #     app.named_path['aa'] == paths[0]
    #     app.architecture.export('')

    # def test_analyze_executor_behavior(self):

    #     architecture_reader = ArchitectureReaderFactory.YamlFile('')
    #     architecture = Architecture(architecture_file)
    #     app = Application(architecture)

    #     synario_config = {
    #         'callback_name': 10,
    #         'executor_priority': 2,
    #         'executor_core': 0
    #     }

    #     senario_analyzer = WorstCaseAnalyzer(app)
    #     trace_result = senario_analyzer.analyze(synario_config)
    #     wc_app = Application(architecture, trace_result)
    #     path = wc_app._named_path['aaa']
    #     path
