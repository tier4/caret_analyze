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

from caret_analyze.infra.lttng import Lttng, lttng_info
from caret_analyze.infra.lttng.architecture_reader_lttng import \
    ArchitectureReaderLttng
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.infra.lttng.records_source import RecordsSource
from caret_analyze.infra.lttng.ros2_tracing.data_model import DataModel
from caret_analyze.infra.lttng.value_objects import (SubscriptionCallbackValueLttng,
                                                                         TimerCallbackValueLttng, PublisherValueLttng)
from caret_analyze.record import Record, Records
from caret_analyze.record.interface import RecordsInterface
from caret_analyze.value_objects import (SubscriptionCallbackStructValue,
                                         TimerCallbackStructValue,
                                         CallbackType,
                                         ExecutorValue,
                                         ExecutorType)

# listener_callback = SubscriptionCallbackStructInfo(
#     '/listener',
#     CallbackType.SUBSCRIPTION.to_indexed_callback_name(0),
#     'demo_nodes_cpp::Listener::Listener(rclcpp::NodeOptionsconst&)::{lambda(std::shared_ptr<std_msgs::msg::String>)#1}',  # noqa: 501
#     '/chatter',
# )

# pipe2_callback = SubscriptionCallbackStructInfo(
#     '/pipe2',
#     CallbackType.SUBSCRIPTION.to_indexed_callback_name(0),
#     'IncrementerPipe::IncrementerPipe(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>const&)::{lambda(std::unique_ptr<std_msgs::msg::Int32>)#1}',  # noqa: 501
#     '/topic2',
# )

class TestLttng:

    def test_get_node_names(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        mocker.patch.object(lttng_info_mock, 'get_node_names', return_value=['/node'])
        node_names = lttng.get_nodes()
        assert len(node_names) == 1
        assert node_names[0] == '/node'

    def test_get_rmw_implementation(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        mocker.patch.object(lttng_info_mock, 'get_rmw_implementation', return_value='rmw')
        a = lttng.get_rmw_impl()
        assert lttng.get_rmw_impl() == 'rmw'

    def test_get_publishers(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        pub_mock = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_publishers_info', return_value=[pub_mock])
        assert lttng.get_publishers('node_name') == [pub_mock]

    def test_get_timer_callbacks(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_timer_callbacks_info',
                            return_value=[timer_cb_mock])
        assert lttng.get_timers('node_name') == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        sub_cb_mock = mocker.Mock(spec=SubscriptionCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_subscription_callbacks_info',
                            return_value=[sub_cb_mock])
        assert lttng.get_subscription_callbacks('node_name') == [sub_cb_mock]

    def test_get_executors(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        exec_info_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_info_mock, 'get_executors_info',
                            return_value=[exec_info_mock])
        assert lttng.get_executors() == [exec_info_mock]

    def test_compose_inter_proc_comm_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_source_mock, 'compose_inter_proc_comm_records',
                            return_value=records_mock)
        assert lttng.compose_inter_proc_comm_records() == records_mock

    def test_compose_intra_proc_comm_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_source_mock, 'compose_intra_proc_comm_records',
                            return_value=records_mock)
        assert lttng.compose_intra_proc_comm_records() == records_mock

    def test_compose_callback_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=data_mock)

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_source_mock, 'compose_callback_records',
                            return_value=records_mock)
        assert lttng.compose_callback_records() == records_mock

    # @pytest.mark.parametrize(
    #     'path, node_name, topic_name, attrs_len',
    #     [
    #         ('sample/lttng_samples/talker_listener', None, None, 3),
    #         ('sample/lttng_samples/talker_listener', '/listener', None, 2),
    #         ('sample/lttng_samples/talker_listener', '/listener', '/chatter', 1),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process', None, None, 4),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process', '/pipe1', None, 2),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process',
    #          '/pipe1', '/topic1', 1),
    #     ],
    # )
    # def test_get_subscription_callback_attrs_with_empty_publish(
    #     self, path, node_name, topic_name, attrs_len
    # ):
    #     lttng = Lttng(path)
    #     attrs = lttng.get_subscription_callbacks(node_name, topic_name)
    #     assert len(attrs) == attrs_len

    # @pytest.mark.parametrize(
    #     'path, node_name, period_ns, cbs_len',
    #     [
    #         ('sample/lttng_samples/talker_listener', None, None, 1),
    #         ('sample/lttng_samples/talker_listener', '/talker', None, 1),
    #         ('sample/lttng_samples/talker_listener', '/talker', 1000000000, 1),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process', None, None, 0),
    #         ('sample/lttng_samples/multi_talker_listener', None, None, 2),
    #         ('sample/lttng_samples/end_to_end_sample/fastrtps', None, None, 3),
    #         ('sample/lttng_samples/end_to_end_sample/cyclonedds', None, None, 3),
    #     ],
    # )
    # def test_get_timer_callback_attrs_with_empty_publish(
    #     self, path, node_name, period_ns, cbs_len
    # ):
    #     lttng = Lttng(path)
    #     callbacks = lttng.get_timer_callbacks(node_name, period_ns)
    #     assert len(callbacks) == cbs_len

    # @pytest.mark.parametrize(
    #     'path, attr, records_len',
    #     [
    #         ('sample/lttng_samples/talker_listener', listener_callback, 5),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process', pipe2_callback, 5),
    #     ],
    # )
    # def test_compose_callback_records(self, mocker: MockerFixture):
    #     info_mock = mocker.Mock(spec=LttngInfo)
    #     source_mock = mocker.Mock(spec=LttngRecordsSourse)

    #     # mocker.patch.object(lttng_info, 'get_node_names', return_value=['/node'])
    #     lttng = Lttng('', info=info_mock, records_source=source_mock)
    #     callback_info = TimerCallbackStructInfo(
    #         '/node', 'timer_callback_0', 'symbol', 0, 1
    #     )
    #     records = lttng.compose_callback_records(callback_info)
    #     assert len(records.data) == records_len

    # @pytest.mark.parametrize(
    #     'path, sub_node_name, pub_node_name, topic_name',
    #     [
    #         ('sample/lttng_samples/talker_listener',
    #          '/listener', '/talker', '/chatter'),
    #         (
    #             'sample/lttng_samples/end_to_end_sample/fastrtps',
    #             '/filter_node',
    #             '/sensor_dummy_node',
    #             '/topic1',
    #         ),
    #         (
    #             'sample/lttng_samples/end_to_end_sample/cyclonedds',
    #             '/filter_node',
    #             '/sensor_dummy_node',
    #             '/topic1',
    #         ),
    #     ],
    # )
    # def test_compose_inter_process_communication_records(
    #     self,
    #     path: str,
    #     sub_node_name: str,
    #     pub_node_name: str,
    #     topic_name: str,
    # ):
    #     lttng = Lttng(path)

    #     sub_cb = lttng.get_subscription_callbacks_info(node_name=sub_node_name)[0]

    #     pub_cb = lttng.get_timer_callbacks_info(node_name=pub_node_name)[0]
    #     records = lttng.compose_inter_process_communication_records(sub_cb, pub_cb)

    #     lttng._records._data_util.data.rclcpp_publish_instances
    #     publish_instances = lttng._records._data_util.data.rclcpp_publish_instances

    #     publish_handlers = lttng._info.get_publisher_handles(
    #         sub_cb.topic_name, pub_cb.node_name)
    #     inter_publish_handle = publish_handlers[0]

    #     def is_target_instance(instance: Record):
    #         return instance.get('publisher_handle') == inter_publish_handle
    #     target_publish_instances = list(filter(
    #         is_target_instance, publish_instances.data))

    #     assert len(records.data) == len(target_publish_instances)

    # @pytest.mark.parametrize(
    #     'path, pub_node_name, sub_node_name, records_len',
    #     [
    #         ('sample/lttng_samples/talker_listener', '/talker', '/listener', 0),
    #         ('sample/lttng_samples/cyclic_pipeline_intra_process', '/pipe1', '/pipe2', 5),
    #     ],
    # )
    # def test_compose_intra_process_communication_records(
    #     self, path, pub_node_name, sub_node_name, records_len
    # ):
    #     def get_cb(node_name: str):
    #         cb = lttng.get_timer_callbacks(node_name)
    #         cb += lttng.get_subscription_callbacks(node_name)
    #         return cb[0]

    #     lttng = Lttng(path)
    #     pub = get_cb(pub_node_name)
    #     sub = get_cb(sub_node_name)
    #     records = lttng.compose_intra_process_communication_records(sub, pub)
    #     assert len(records.data) == records_len

    # @pytest.mark.parametrize(
    #     'path',
    #     [
    #         ('sample/lttng_samples/end_to_end_sample/fastrtps'),
    #         ('sample/lttng_samples/end_to_end_sample/cyclonedds'),
    #     ],
    # )
    # def test_compose_variable_passing_records(self, path):
    #     lttng = Lttng(path)

    #     callback_write = SubscriptionCallbackStructInfo(
    #         '/message_driven_node',
    #         SubscriptionCallbackStructInfo.to_indexed_callback_name(0),
    #         'SubDependencyNode::SubDependencyNode(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>)::{lambda(std::unique_ptr<sensor_msgs::msg::Image>)#1}',  # noqa: 501
    #         '/topic2',
    #     )

    #     callback_read = SubscriptionCallbackStructInfo(
    #         '/message_driven_node',
    #         SubscriptionCallbackStructInfo.to_indexed_callback_name(1),
    #         'SubDependencyNode::SubDependencyNode(std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>,std::__cxx11::basic_string<char,std::char_traits<char>,std::allocator<char>>)::{lambda(std::unique_ptr<sensor_msgs::msg::Image>)#2}',  # noqa: 501
    #         '/drive',
    #     )

    #     records = lttng.compose_variable_passing_records(
    #         callback_write, callback_read)
    #     callback_instances = lttng._records._data_util.data.callback_end_instances
    #     callback_object = lttng._to_local_callback(callback_write).inter_callback_object

    #     def is_target_callback(record: Record):
    #         return record.get('callback_object') == callback_object

    #     target_callbacks = list(filter(is_target_callback, callback_instances.data))

    #     assert len(records.data) == len(target_callbacks)

    # @pytest.mark.parametrize(
    #     'path, expect',
    #     [
    #         ('sample/lttng_samples/end_to_end_sample/fastrtps', 'rmw_fastrtps_cpp'),
    #         ('sample/lttng_samples/end_to_end_sample/cyclonedds', 'rmw_cyclonedds_cpp'),
    #     ],
    # )
    # def test_rmw_implementation(self, path, expect):
    #     lttng = Lttng(path)

    #     assert lttng.get_rmw_implementation() == expect
