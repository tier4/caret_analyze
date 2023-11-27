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


from datetime import datetime

from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.event_counter import EventCounter
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.infra.lttng.records_source import RecordsSource
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (PublisherValueLttng,
                                                     ServiceCallbackValueLttng,
                                                     SubscriptionCallbackValueLttng,
                                                     TimerCallbackValueLttng)
from caret_analyze.record.interface import RecordsInterface
from caret_analyze.value_objects import ExecutorValue
from caret_analyze.value_objects.node import NodeValue

import pytest


class TestLttng:

    def test_file_not_found_error(self, mocker):
        with mocker.patch('os.path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                Lttng('', force_conversion=True)

    def test_get_nodes(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        node = NodeValue('/node', None)
        mocker.patch.object(lttng_info_mock, 'get_nodes', return_value=[node])
        nodes = lttng.get_nodes()
        assert nodes == [node]

    def test_get_rmw_implementation(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        mocker.patch.object(lttng_info_mock, 'get_rmw_impl', return_value='rmw')
        assert lttng.get_rmw_impl() == 'rmw'

    def test_get_publishers(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        pub_mock = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_publishers', return_value=[pub_mock])
        assert lttng.get_publishers(NodeValue('node', None)) == [pub_mock]

    def test_get_timer_callbacks(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)
        lttng = Lttng('trace_dir')

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_timer_callbacks',
                            return_value=[timer_cb_mock])
        assert lttng.get_timer_callbacks(NodeValue('node', None)) == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        sub_cb_mock = mocker.Mock(spec=SubscriptionCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_subscription_callbacks',
                            return_value=[sub_cb_mock])
        assert lttng.get_subscription_callbacks(NodeValue('node', None)) == [sub_cb_mock]

    def test_get_service_callbacks(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        srv_cb_mock = mocker.Mock(spec=ServiceCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_service_callbacks',
                            return_value=[srv_cb_mock])
        assert lttng.get_service_callbacks(NodeValue('node', None)) == [srv_cb_mock]

    def test_get_executors(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        exec_info_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_info_mock, 'get_executors',
                            return_value=[exec_info_mock])
        assert lttng.get_executors() == [exec_info_mock]

    def test_compose_intra_proc_comm_records(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'intra_proc_comm_records',
                            records_mock)
        assert lttng.compose_intra_proc_comm_records() == records_mock

    def test_compose_callback_records(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'callback_records',
                            records_mock)
        assert lttng.compose_callback_records() == records_mock

    def test_singleton_disabled(self, mocker):
        data = Ros2DataModel()
        data.finalize()
        events = {'b': None}
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data, events, 0, 0))
        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        lttng = Lttng('', validate=False, store_events=True)

        data_ = Ros2DataModel()
        data_.finalize()
        events_ = {'a': None}
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_, events_, 0, 1*10**9))
        lttng_ = Lttng('', validate=False, store_events=True)
        begin, end = lttng_.get_trace_range()
        assert begin == datetime.fromtimestamp(0)
        assert end == datetime.fromtimestamp(1)
        creation = lttng_.get_trace_creation_datetime()
        assert creation == datetime.fromtimestamp(0)
        assert lttng.events == events
        assert lttng_.events == events_
        assert lttng.events != lttng_.events

    def test_compare_init_event(self):
        import functools
        init_events = []
        event_collection = {}
        # for testing sorting with _timestamp
        TIME_ORDER = 0
        event_collection[TIME_ORDER+0] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 503, }
        event_collection[TIME_ORDER+1] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 500, }
        event_collection[TIME_ORDER+2] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 502, }
        event_collection[TIME_ORDER+3] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 501, }
        # below, _timestamp is the same
        # the order of _prioritized_init_events is defined in reverse order
        # (lowest priority first)
        SORT_ORDER_ADD_IRON = 4
        event_collection[SORT_ORDER_ADD_IRON+0] = {
            '_name': 'ros2:rclcpp_ipb_to_subscription', '_timestamp': 600, }
        event_collection[SORT_ORDER_ADD_IRON+1] = {
            '_name': 'ros2_caret:rclcpp_ipb_to_subscription', '_timestamp': 600, }
        event_collection[SORT_ORDER_ADD_IRON+2] = {
            '_name': 'ros2:rclcpp_buffer_to_ipb', '_timestamp': 600, }
        event_collection[SORT_ORDER_ADD_IRON+3] = {
            '_name': 'ros2_caret:rclcpp_buffer_to_ipb', '_timestamp': 600, }
        event_collection[SORT_ORDER_ADD_IRON+4] = {
            '_name': 'ros2:rclcpp_construct_ring_buffer', '_timestamp': 600, }
        event_collection[SORT_ORDER_ADD_IRON+5] = {
            '_name': 'ros2_caret:rclcpp_construct_ring_buffer', '_timestamp': 600, }
        SORT_ORDER = 10
        event_collection[SORT_ORDER+0] = {
            '_name': 'ros2_caret:callback_group_add_client', '_timestamp': 600, }
        event_collection[SORT_ORDER+1] = {
            '_name': 'ros2_caret:callback_group_add_service', '_timestamp': 600, }
        event_collection[SORT_ORDER+2] = {
            '_name': 'ros2_caret:callback_group_add_subscription', '_timestamp': 600, }
        event_collection[SORT_ORDER+3] = {
            '_name': 'ros2_caret:callback_group_add_timer', '_timestamp': 600, }
        event_collection[SORT_ORDER+4] = {
            '_name': 'ros2_caret:add_callback_group_static_executor', '_timestamp': 600, }
        event_collection[SORT_ORDER+5] = {
            '_name': 'ros2_caret:add_callback_group', '_timestamp': 600, }
        event_collection[SORT_ORDER+6] = {
            '_name': 'ros2_caret:construct_static_executor', '_timestamp': 600, }
        event_collection[SORT_ORDER+7] = {
            '_name': 'ros2_caret:construct_executor', '_timestamp': 600, }
        event_collection[SORT_ORDER+8] = {
            '_name': 'ros2_caret:rmw_implementation', '_timestamp': 600, }
        event_collection[SORT_ORDER+9] = {
            '_name': 'ros2_caret:caret_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+10] = {
            '_name': 'ros2:rcl_lifecycle_state_machine_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+11] = {
            '_name': 'ros2_caret:rcl_lifecycle_state_machine_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+12] = {
            '_name': 'ros2:rclcpp_callback_register', '_timestamp': 600, }
        event_collection[SORT_ORDER+13] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 600, }
        event_collection[SORT_ORDER+14] = {
            '_name': 'ros2:rclcpp_timer_link_node', '_timestamp': 600, }
        event_collection[SORT_ORDER+15] = {
            '_name': 'ros2_caret:rclcpp_timer_link_node', '_timestamp': 600, }
        event_collection[SORT_ORDER+16] = {
            '_name': 'ros2:rclcpp_timer_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+17] = {
            '_name': 'ros2_caret:rclcpp_timer_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+18] = {
            '_name': 'ros2:rcl_timer_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+19] = {
            '_name': 'ros2_caret:rcl_timer_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+20] = {
            '_name': 'ros2:rcl_client_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+21] = {
            '_name': 'ros2_caret:rcl_client_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+22] = {
            '_name': 'ros2:rclcpp_service_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+23] = {
            '_name': 'ros2_caret:rclcpp_service_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+24] = {
            '_name': 'ros2:rcl_service_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+25] = {
            '_name': 'ros2_caret:rcl_service_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+26] = {
            '_name': 'ros2:rclcpp_subscription_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+27] = {
            '_name': 'ros2_caret:rclcpp_subscription_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER+28] = {
            '_name': 'ros2:rclcpp_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+29] = {
            '_name': 'ros2_caret:rclcpp_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+30] = {
            '_name': 'ros2:rcl_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+31] = {
            '_name': 'ros2_caret:rcl_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+32] = {
            '_name': 'ros2:rcl_publisher_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+33] = {
            '_name': 'ros2_caret:rcl_publisher_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+34] = {
            '_name': 'ros2:rcl_node_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+35] = {
            '_name': 'ros2_caret:rcl_node_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+36] = {
            '_name': 'ros2:rcl_init', '_timestamp': 600, }
        event_collection[SORT_ORDER+37] = {
            '_name': 'ros2_caret:rcl_init', '_timestamp': 600, }
        # also check when _timestamp is small
        SMALL_ORDER = 48
        event_collection[SMALL_ORDER+0] = {
            '_name': 'ros2_caret:callback_group_add_client', '_timestamp': 400, }
        event_collection[SMALL_ORDER+1] = {
            '_name': 'ros2_caret:callback_group_add_service', '_timestamp': 400, }
        event_collection[SMALL_ORDER+2] = {
            '_name': 'ros2_caret:callback_group_add_subscription', '_timestamp': 400, }

        for i in range(len(event_collection)):
            init_events.append(event_collection[i])

        init_events.sort(key=functools.cmp_to_key(Lttng._compare_init_event))

        # 0-2 : tests where _timestamp is younger than _timestamp alone and is sorted by _name
        assert init_events[0]['_timestamp'] == 400 and \
            init_events[0]['_name'] == 'ros2_caret:callback_group_add_subscription'
        assert init_events[1]['_timestamp'] == 400 and \
            init_events[1]['_name'] == 'ros2_caret:callback_group_add_service'
        assert init_events[2]['_timestamp'] == 400 and \
            init_events[2]['_name'] == 'ros2_caret:callback_group_add_client'
        # 3-6 : exams sorted by _timestamp
        assert init_events[3]['_timestamp'] == 500
        assert init_events[4]['_timestamp'] == 501
        assert init_events[5]['_timestamp'] == 502
        assert init_events[6]['_timestamp'] == 503
        # 7-44 : tests sorted by _prioritized_init_events order
        assert init_events[7]['_timestamp'] == 600 and \
            init_events[7]['_name'] == 'ros2_caret:rcl_init'
        assert init_events[8]['_timestamp'] == 600 and \
            init_events[8]['_name'] == 'ros2:rcl_init'
        assert init_events[9]['_timestamp'] == 600 and \
            init_events[9]['_name'] == 'ros2_caret:rcl_node_init'
        assert init_events[10]['_timestamp'] == 600 and \
            init_events[10]['_name'] == 'ros2:rcl_node_init'
        assert init_events[11]['_timestamp'] == 600 and \
            init_events[11]['_name'] == 'ros2_caret:rcl_publisher_init'
        assert init_events[12]['_timestamp'] == 600 and \
            init_events[12]['_name'] == 'ros2:rcl_publisher_init'
        assert init_events[13]['_timestamp'] == 600 and \
            init_events[13]['_name'] == 'ros2_caret:rcl_subscription_init'
        assert init_events[14]['_timestamp'] == 600 and \
            init_events[14]['_name'] == 'ros2:rcl_subscription_init'
        assert init_events[15]['_timestamp'] == 600 and \
            init_events[15]['_name'] == 'ros2_caret:rclcpp_subscription_init'
        assert init_events[16]['_timestamp'] == 600 and \
            init_events[16]['_name'] == 'ros2:rclcpp_subscription_init'
        assert init_events[17]['_timestamp'] == 600 and \
            init_events[17]['_name'] == 'ros2_caret:rclcpp_subscription_callback_added'
        assert init_events[18]['_timestamp'] == 600 and \
            init_events[18]['_name'] == 'ros2:rclcpp_subscription_callback_added'
        assert init_events[19]['_timestamp'] == 600 and \
            init_events[19]['_name'] == 'ros2_caret:rcl_service_init'
        assert init_events[20]['_timestamp'] == 600 and \
            init_events[20]['_name'] == 'ros2:rcl_service_init'
        assert init_events[21]['_timestamp'] == 600 and \
            init_events[21]['_name'] == 'ros2_caret:rclcpp_service_callback_added'
        assert init_events[22]['_timestamp'] == 600 and \
            init_events[22]['_name'] == 'ros2:rclcpp_service_callback_added'
        assert init_events[23]['_timestamp'] == 600 and \
            init_events[23]['_name'] == 'ros2_caret:rcl_client_init'
        assert init_events[24]['_timestamp'] == 600 and \
            init_events[24]['_name'] == 'ros2:rcl_client_init'
        assert init_events[25]['_timestamp'] == 600 and \
            init_events[25]['_name'] == 'ros2_caret:rcl_timer_init'
        assert init_events[26]['_timestamp'] == 600 and \
            init_events[26]['_name'] == 'ros2:rcl_timer_init'
        assert init_events[27]['_timestamp'] == 600 and \
            init_events[27]['_name'] == 'ros2_caret:rclcpp_timer_callback_added'
        assert init_events[28]['_timestamp'] == 600 and \
            init_events[28]['_name'] == 'ros2:rclcpp_timer_callback_added'
        assert init_events[29]['_timestamp'] == 600 and \
            init_events[29]['_name'] == 'ros2_caret:rclcpp_timer_link_node'
        assert init_events[30]['_timestamp'] == 600 and \
            init_events[30]['_name'] == 'ros2:rclcpp_timer_link_node'
        assert init_events[31]['_timestamp'] == 600 and \
            init_events[31]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[32]['_timestamp'] == 600 and \
            init_events[32]['_name'] == 'ros2:rclcpp_callback_register'
        assert init_events[33]['_timestamp'] == 600 and \
            init_events[33]['_name'] == 'ros2_caret:rcl_lifecycle_state_machine_init'
        assert init_events[34]['_timestamp'] == 600 and \
            init_events[34]['_name'] == 'ros2:rcl_lifecycle_state_machine_init'
        assert init_events[35]['_timestamp'] == 600 and \
            init_events[35]['_name'] == 'ros2_caret:caret_init'
        assert init_events[36]['_timestamp'] == 600 and \
            init_events[36]['_name'] == 'ros2_caret:rmw_implementation'
        assert init_events[37]['_timestamp'] == 600 and \
            init_events[37]['_name'] == 'ros2_caret:construct_executor'
        assert init_events[38]['_timestamp'] == 600 and \
            init_events[38]['_name'] == 'ros2_caret:construct_static_executor'
        assert init_events[39]['_timestamp'] == 600 and \
            init_events[39]['_name'] == 'ros2_caret:add_callback_group'
        assert init_events[40]['_timestamp'] == 600 and \
            init_events[40]['_name'] == 'ros2_caret:add_callback_group_static_executor'
        assert init_events[41]['_timestamp'] == 600 and \
            init_events[41]['_name'] == 'ros2_caret:callback_group_add_timer'
        assert init_events[42]['_timestamp'] == 600 and \
            init_events[42]['_name'] == 'ros2_caret:callback_group_add_subscription'
        assert init_events[43]['_timestamp'] == 600 and \
            init_events[43]['_name'] == 'ros2_caret:callback_group_add_service'
        assert init_events[44]['_timestamp'] == 600 and \
            init_events[44]['_name'] == 'ros2_caret:callback_group_add_client'
        assert init_events[45]['_timestamp'] == 600 and \
            init_events[45]['_name'] == 'ros2_caret:rclcpp_construct_ring_buffer'
        assert init_events[46]['_timestamp'] == 600 and \
            init_events[46]['_name'] == 'ros2:rclcpp_construct_ring_buffer'
        assert init_events[47]['_timestamp'] == 600 and \
            init_events[47]['_name'] == 'ros2_caret:rclcpp_buffer_to_ipb'
        assert init_events[48]['_timestamp'] == 600 and \
            init_events[48]['_name'] == 'ros2:rclcpp_buffer_to_ipb'
        assert init_events[49]['_timestamp'] == 600 and \
            init_events[49]['_name'] == 'ros2_caret:rclcpp_ipb_to_subscription'
        assert init_events[50]['_timestamp'] == 600 and \
            init_events[50]['_name'] == 'ros2:rclcpp_ipb_to_subscription'

    def test_duplicated_events(self, mocker):
        HDL_CONTEXT = 1000101
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_PUBLISHER = 1000301
        HDL_RMW_PUBLISHER = 1000311
        HDL_SUBSCRIPTION = 1000401
        HDL_RMW_SUBSCRIPTION = 1000411
        SUBSCRIPTION = 1000451
        SUBSCRIPTION_CALLBACK = 1000461
        HDL_SERVICE = 1000501
        HDL_RMW_SERVICE = 1000511
        SERVICE_CALLBACK = 1000561
        TIMER_CALLBACK = 1000661
        HDL_CLIENT = 1000601
        HDL_RMW_CLIENT = 1000611
        HDL_TIMER = 1000701
        STATE_MACHINE = 1000801
        HDL_EXECUTOR = 1001101
        HDL_EXECUTOR_STATIC = 1001201
        HDL_ENTITIES = 1001211
        EXECUTOR_CALLBACK = 1001261
        EXECUTOR_STA_CALLBACK = 1001281
        RING_BUFFER = 1001701
        BUFFER_JPB = 1001801
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
            {
                '_name': 'ros2:rcl_init',
                'context_handle': HDL_CONTEXT,
                '_timestamp': 100100101,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_init',
                'context_handle': HDL_CONTEXT,
                '_timestamp': 100100102,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_node_init',
                'node_handle': HDL_NODE,
                'rmw_handle': HDL_RMW,
                'node_name': 'my_node_name',
                'namespace': '/',
                '_timestamp': 100100201,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_node_init',
                'node_handle': HDL_NODE,
                'rmw_handle': HDL_RMW,
                'node_name': 'my_node_name',
                'namespace': '/',
                '_timestamp': 100100203,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_publisher_init',
                'publisher_handle': HDL_PUBLISHER,
                'node_handle': HDL_NODE,
                'rmw_publisher_handle': HDL_RMW_PUBLISHER,
                '_timestamp': 100100202,
                'topic_name': 'my_topic_name',
                'queue_depth': 1,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_publisher_init',
                'publisher_handle': HDL_PUBLISHER,
                'node_handle': HDL_NODE,
                'rmw_publisher_handle': HDL_RMW_PUBLISHER,
                '_timestamp': 100100303,
                'topic_name': 'my_topic_name',
                'queue_depth': 1,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_subscription_init',
                'subscription_handle': HDL_SUBSCRIPTION,
                'node_handle': HDL_NODE,
                'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                '_timestamp': 100100202,
                'topic_name': 'my_topic_name',
                'queue_depth': 1,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_subscription_init',
                'subscription_handle': HDL_SUBSCRIPTION,
                'node_handle': HDL_NODE,
                'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                '_timestamp': 100100402,
                'topic_name': 'my_topic_name',
                'queue_depth': 1,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_subscription_init',
                'subscription_handle': HDL_SUBSCRIPTION,
                'node_handle': HDL_NODE,
                'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                '_timestamp': 100100404,
                'topic_name': 'my_topic_name',
                'queue_depth': 1,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_subscription_init',
                'subscription': SUBSCRIPTION,
                'subscription_handle': HDL_SUBSCRIPTION,
                '_timestamp': 100100403,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_subscription_init',
                'subscription': SUBSCRIPTION,
                'subscription_handle': HDL_SUBSCRIPTION,
                '_timestamp': 100100452,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_subscription_callback_added',
                'subscription': SUBSCRIPTION,
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100100450,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_subscription_callback_added',
                'subscription': SUBSCRIPTION,
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100100462,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_service_init',
                'service_handle': HDL_SERVICE,
                'node_handle': HDL_NODE,
                'rmw_service_handle': HDL_RMW_SERVICE,
                '_timestamp': 100100202,
                'service_name': 'my_service_name',
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_service_init',
                'service_handle': HDL_SERVICE,
                'node_handle': HDL_NODE,
                'rmw_service_handle': HDL_RMW_SERVICE,
                '_timestamp': 100100502,
                'service_name': 'my_service_name',
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_service_callback_added',
                'service_handle': HDL_SERVICE,
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100100500,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_service_callback_added',
                'service_handle': HDL_SERVICE,
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100100551,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_client_init',
                'client_handle': HDL_CLIENT,
                'node_handle': HDL_NODE,
                'rmw_client_handle': HDL_RMW_CLIENT,
                '_timestamp': 100100202,
                'service_name': 'my_service_name',
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_client_init',
                'client_handle': HDL_CLIENT,
                'node_handle': HDL_NODE,
                'rmw_client_handle': HDL_RMW_CLIENT,
                '_timestamp': 100100602,
                'service_name': 'my_service_name',
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_timer_init',
                'timer_handle': HDL_TIMER,
                'period': 100,
                '_timestamp': 100100701,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_timer_init',
                'timer_handle': HDL_TIMER,
                'period': 101,
                '_timestamp': 100100703,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_timer_callback_added',
                'timer_handle': HDL_TIMER,
                'callback': TIMER_CALLBACK,
                '_timestamp': 100100702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_timer_callback_added',
                'timer_handle': HDL_TIMER,
                'callback': TIMER_CALLBACK,
                '_timestamp': 100100751,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_timer_link_node',
                'timer_handle': HDL_TIMER,
                'node_handle': HDL_NODE,
                '_timestamp': 100100702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_timer_link_node',
                'timer_handle': HDL_TIMER,
                'node_handle': HDL_NODE,
                '_timestamp': 100100772,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_callback_register',
                'callback': TIMER_CALLBACK,
                'symbol': 100,
                '_timestamp': 100100750,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_callback_register',
                'callback': TIMER_CALLBACK,
                'symbol': 101,
                '_timestamp': 100100782,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_lifecycle_state_machine_init',
                'node_handle': HDL_NODE,
                'state_machine': STATE_MACHINE,
                '_timestamp': 100100202,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_lifecycle_state_machine_init',
                'node_handle': HDL_NODE,
                'state_machine': STATE_MACHINE,
                '_timestamp': 100100802,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:caret_init',
                'clock_offset': 10,
                'distribution': 20,
                '_timestamp': 100100901,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:rmw_implementation',
                'rmw_impl': 10,
                '_timestamp': 100101001,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:construct_executor',
                'executor_addr': HDL_EXECUTOR,
                'executor_type_name': 'my_executor_name',
                '_timestamp': 100101101,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:construct_executor',
                'executor_addr': HDL_EXECUTOR,
                'executor_type_name': 'my_executor_name',
                '_timestamp': 100101103,
                '_vtid': VTID1,
                '_vpid': VPID1
            },

            {
                '_name': 'ros2_caret:construct_static_executor',
                'executor_addr': HDL_EXECUTOR_STATIC,
                'entities_collector_addr': HDL_ENTITIES,
                'executor_type_name': 'my_executor_name',
                '_timestamp': 100101102,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:construct_static_executor',
                'executor_addr': HDL_EXECUTOR_STATIC,
                'entities_collector_addr': HDL_ENTITIES,
                'executor_type_name': 'my_executor_name',
                '_timestamp': 100101203,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:construct_static_executor',
                'executor_addr': HDL_EXECUTOR_STATIC,
                'entities_collector_addr': HDL_ENTITIES,
                'executor_type_name': 'my_executor_name',
                '_timestamp': 100101205,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:add_callback_group',
                'executor_addr': HDL_EXECUTOR_STATIC,
                'callback_group_addr': EXECUTOR_CALLBACK,
                'group_type_name': 'my_group_name',
                '_timestamp': 100101102,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:add_callback_group',
                'executor_addr': HDL_EXECUTOR_STATIC,
                'callback_group_addr': EXECUTOR_CALLBACK,
                'group_type_name': 'my_group_name',
                '_timestamp': 100101261,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:add_callback_group_static_executor',
                'entities_collector_addr': HDL_ENTITIES,
                'callback_group_addr': EXECUTOR_STA_CALLBACK,
                'group_type_name': 'my_group_name',
                '_timestamp': 100101204,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:add_callback_group_static_executor',
                'entities_collector_addr': HDL_ENTITIES,
                'callback_group_addr': EXECUTOR_STA_CALLBACK,
                'group_type_name': 'my_group_name',
                '_timestamp': 100101281,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_timer',
                'timer_handle': HDL_TIMER,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100100702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_timer',
                'timer_handle': HDL_TIMER,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100101302,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_subscription',
                'subscription_handle': HDL_SUBSCRIPTION,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100100401,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_subscription',
                'subscription_handle': HDL_SUBSCRIPTION,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100101402,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_service',
                'service_handle': HDL_SERVICE,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100100501,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_service',
                'service_handle': HDL_SERVICE,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100101502,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_client',
                'client_handle': HDL_CLIENT,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100100601,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_add_client',
                'client_handle': HDL_CLIENT,
                'callback_group_addr': EXECUTOR_CALLBACK,
                '_timestamp': 100101602,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_construct_ring_buffer',
                'buffer': RING_BUFFER,
                'capacity': 100,
                '_timestamp': 100101701,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_construct_ring_buffer',
                'buffer': RING_BUFFER,
                'capacity': 101,
                '_timestamp': 100101703,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_buffer_to_ipb',
                'ipb': BUFFER_JPB,
                'buffer': RING_BUFFER,
                '_timestamp': 100101702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_buffer_to_ipb',
                'ipb': BUFFER_JPB,
                'buffer': RING_BUFFER,
                '_timestamp': 100101803,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ipb_to_subscription',
                'ipb': BUFFER_JPB,
                'subscription': SUBSCRIPTION,
                '_timestamp': 100101802,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ipb_to_subscription',
                'ipb': BUFFER_JPB,
                'subscription': SUBSCRIPTION,
                '_timestamp': 100101902,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            # Runtime trace events
            {
                '_name': 'ros2:callback_start',
                'callback': TIMER_CALLBACK,
                'is_intra_process': 100,
                '_timestamp': 100100750,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:callback_start',
                'callback': TIMER_CALLBACK,
                'is_intra_process': 101,
                '_timestamp': 100102002,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:callback_end',
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100100501,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:callback_end',
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100102102,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:message_construct',
                'original_message': 10,
                'constructed_message': 20,
                '_timestamp': 100102201,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_intra_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 100,
                '_timestamp': 100100302,
                'message_timestamp': 100100305,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_intra_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 101,
                '_timestamp': 100102303,
                'message_timestamp': 100102304,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:dispatch_subscription_callback',
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100100450,
                'message': 100,
                'source_stamp': 100100453,
                'message_timestamp': 100100454,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:dispatch_subscription_callback',
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100102402,
                'message': 101,
                'source_stamp': 100102404,
                'message_timestamp': 100102405,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rmw_take',
                'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                '_timestamp': 100100401,
                'message': 100,
                'source_timestamp': 100100453,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rmw_take',
                'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                '_timestamp': 100102503,
                'message': 101,
                'source_timestamp': 100102504,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:dispatch_intra_process_subscription_callback',
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100100450,
                'message': 100,
                'message_timestamp': 100100454,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:dispatch_intra_process_subscription_callback',
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100102602,
                'message': 101,
                'message_timestamp': 100102603,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 100,
                '_timestamp': 100100302,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 101,
                '_timestamp': 100102703,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 100,
                'message_timestamp': 100100306,
                '_timestamp': 100100302,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_publish',
                'publisher_handle': HDL_PUBLISHER,
                'message': 101,
                'message_timestamp': 100102804,
                '_timestamp': 100102803,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_publish',
                # 'publisher_handle': 0,
                'message': 102,
                # 'message_timestamp': 0,
                '_timestamp': 100102804,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:dds_write',
                'message': 100,
                '_timestamp': 100102901,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:dds_bind_addr_to_stamp',
                'addr': 100,
                'source_stamp': 101,
                '_timestamp': 100103001,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:dds_bind_addr_to_addr',
                'addr_from': 100,
                'addr_to': 101,
                '_timestamp': 100103101,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_lifecycle_transition',
                'state_machine': STATE_MACHINE,
                'start_label': 100,
                'goal_label': 101,
                '_timestamp': 100100801,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rcl_lifecycle_transition',
                'state_machine': STATE_MACHINE,
                'start_label': 200,
                'goal_label': 201,
                '_timestamp': 100103201,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:on_data_available',
                'source_stamp': 100,
                '_timestamp': 100103301,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:sim_time',
                'stamp': 100,
                '_timestamp': 100103401,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ring_buffer_enqueue',
                'buffer': RING_BUFFER,
                'index': 100,
                'size': 101,
                'overwritten': 102,
                '_timestamp': 100101702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ring_buffer_enqueue',
                'buffer': RING_BUFFER,
                'index': 200,
                'size': 201,
                'overwritten': 202,
                '_timestamp': 100103501,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ring_buffer_dequeue',
                'buffer': RING_BUFFER,
                'index': 100,
                'size': 101,
                '_timestamp': 100101702,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_ring_buffer_dequeue',
                'buffer': RING_BUFFER,
                'index': 200,
                'size': 201,
                '_timestamp': 100103601,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # contexts
        # ['timestamp', 'pid']
        assert lttng.data.contexts.df.index[0] == HDL_CONTEXT and \
            lttng.data.contexts.df.iloc[0]['timestamp'] == 100100101
        # HDL_CONTEXT is duplicated and "1" is newly assigned
        assert lttng.data.contexts.df.index[1] == 1 and \
            lttng.data.contexts.df.iloc[1]['timestamp'] == 100100102

        # nodes
        # ['timestamp', 'tid', 'rmw_handle', 'namespace', 'name']
        assert lttng.data.nodes.df.index[0] == HDL_NODE and \
            lttng.data.nodes.df.iloc[0]['timestamp'] == 100100201 and \
            lttng.data.nodes.df.iloc[0]['rmw_handle'] == HDL_RMW
        # HDL_NODE,HDL_RMW is duplicated and "1" is newly assigned
        # This pattern is omitted below.
        assert lttng.data.nodes.df.index[1] == 1 and \
            lttng.data.nodes.df.iloc[1]['timestamp'] == 100100203 and \
            lttng.data.nodes.df.iloc[1]['rmw_handle'] == 1

        # publishers
        # ['timestamp', 'node_handle', 'rmw_handle', 'topic_name', 'depth']
        # Note: 'node_handle' is HDL_NODE the first time and 1 the second time.
        #   'timestamp' puts the first publisher_init between the first and second node_init
        #   Duplicate check triangle pattern
        #   The rest omitted.
        assert lttng.data.publishers.df.index[0] == HDL_PUBLISHER and \
            lttng.data.publishers.df.iloc[0]['timestamp'] == 100100202 and \
            lttng.data.publishers.df.iloc[0]['node_handle'] == HDL_NODE and \
            lttng.data.publishers.df.iloc[0]['rmw_handle'] == HDL_RMW_PUBLISHER

        assert lttng.data.publishers.df.index[1] == 1 and \
            lttng.data.publishers.df.iloc[1]['timestamp'] == 100100303 and \
            lttng.data.publishers.df.iloc[1]['node_handle'] == 1 and \
            lttng.data.publishers.df.iloc[1]['rmw_handle'] == 1

        # subscriptions
        # ['timestamp', 'node_handle', 'rmw_handle', 'topic_name', 'depth']
        assert lttng.data.subscriptions.df.index[0] == HDL_SUBSCRIPTION and \
            lttng.data.subscriptions.df.iloc[0]['timestamp'] == 100100202 and \
            lttng.data.subscriptions.df.iloc[0]['node_handle'] == HDL_NODE and \
            lttng.data.subscriptions.df.iloc[0]['rmw_handle'] == HDL_RMW_SUBSCRIPTION

        assert lttng.data.subscriptions.df.index[1] == 1 and \
            lttng.data.subscriptions.df.iloc[1]['timestamp'] == 100100402 and \
            lttng.data.subscriptions.df.iloc[1]['node_handle'] == 1 and \
            lttng.data.subscriptions.df.iloc[1]['rmw_handle'] == 1

        assert lttng.data.subscriptions.df.index[2] == 2 and \
            lttng.data.subscriptions.df.iloc[2]['timestamp'] == 100100404 and \
            lttng.data.subscriptions.df.iloc[2]['node_handle'] == 1 and \
            lttng.data.subscriptions.df.iloc[2]['rmw_handle'] == 2

        # subscription_objects
        # ['timestamp', 'subscription_handle']
        assert lttng.data.subscription_objects.df.index[0] == SUBSCRIPTION and \
            lttng.data.subscription_objects.df.iloc[0]['timestamp'] == 100100403 and \
            lttng.data.subscription_objects.df.iloc[0]['subscription_handle'] == 1

        assert lttng.data.subscription_objects.df.index[1] == 1 and \
            lttng.data.subscription_objects.df.iloc[1]['timestamp'] == 100100452 and \
            lttng.data.subscription_objects.df.iloc[1]['subscription_handle'] == 2

        # callback_objects
        # ['timestamp', 'callback_object']
        assert lttng.data.callback_objects.df.index[0] == SUBSCRIPTION and \
            lttng.data.callback_objects.df.iloc[0]['timestamp'] == 100100450 and \
            lttng.data.callback_objects.df.iloc[0]['callback_object'] == SUBSCRIPTION_CALLBACK

        assert lttng.data.callback_objects.df.index[1] == 1 and \
            lttng.data.callback_objects.df.iloc[1]['timestamp'] == 100100462 and \
            lttng.data.callback_objects.df.iloc[1]['callback_object'] == 1

        # services
        # ['timestamp', 'node_handle', 'rmw_handle', 'service_name']
        assert lttng.data.services.df.index[0] == HDL_SERVICE and \
            lttng.data.services.df.iloc[0]['timestamp'] == 100100202 and \
            lttng.data.services.df.iloc[0]['node_handle'] == HDL_NODE and \
            lttng.data.services.df.iloc[0]['rmw_handle'] == HDL_RMW_SERVICE

        assert lttng.data.services.df.index[1] == 1 and \
            lttng.data.services.df.iloc[1]['timestamp'] == 100100502 and \
            lttng.data.services.df.iloc[1]['node_handle'] == 1 and \
            lttng.data.services.df.iloc[1]['rmw_handle'] == 1

        # callback_objects
        # ['timestamp', 'callback_object']
        assert lttng.data.callback_objects.df.index[2] == HDL_SERVICE and \
            lttng.data.callback_objects.df.iloc[2]['timestamp'] == 100100500 and \
            lttng.data.callback_objects.df.iloc[2]['callback_object'] == SERVICE_CALLBACK

        # Note: 1 is registered in callback_object of SUBSCRIPTION, so it becomes 2
        assert lttng.data.callback_objects.df.index[3] == 1 and \
            lttng.data.callback_objects.df.iloc[3]['timestamp'] == 100100551 and \
            lttng.data.callback_objects.df.iloc[3]['callback_object'] == 2

        # clients
        # ['timestamp', 'node_handle', 'rmw_handle', 'service_name']
        assert lttng.data.clients.df.index[0] == HDL_CLIENT and \
            lttng.data.clients.df.iloc[0]['timestamp'] == 100100202 and \
            lttng.data.clients.df.iloc[0]['node_handle'] == HDL_NODE and \
            lttng.data.clients.df.iloc[0]['rmw_handle'] == HDL_RMW_CLIENT

        assert lttng.data.clients.df.index[1] == 1 and \
            lttng.data.clients.df.iloc[1]['timestamp'] == 100100602 and \
            lttng.data.clients.df.iloc[1]['node_handle'] == 1 and \
            lttng.data.clients.df.iloc[1]['rmw_handle'] == 1

        # timers
        # ['timestamp', 'period', 'tid']
        assert lttng.data.timers.df.index[0] == HDL_TIMER and \
            lttng.data.timers.df.iloc[0]['timestamp'] == 100100701 and \
            lttng.data.timers.df.iloc[0]['period'] == 100

        assert lttng.data.timers.df.index[1] == 1 and \
            lttng.data.timers.df.iloc[1]['timestamp'] == 100100703 and \
            lttng.data.timers.df.iloc[1]['period'] == 101

        # callback_objects
        # ['timestamp', 'callback_object']
        assert lttng.data.callback_objects.df.index[4] == HDL_TIMER and \
            lttng.data.callback_objects.df.iloc[4]['timestamp'] == 100100702 and \
            lttng.data.callback_objects.df.iloc[4]['callback_object'] == TIMER_CALLBACK

        # Note: 1 and 2 are registered in callback_object of SUBSCRIPTION,HDL_SERVICE,
        #   so it becomes 3.
        assert lttng.data.callback_objects.df.index[5] == 1 and \
            lttng.data.callback_objects.df.iloc[5]['timestamp'] == 100100751 and \
            lttng.data.callback_objects.df.iloc[5]['callback_object'] == 3

        # timer_node_links
        # ['timestamp', 'node_handle']
        assert lttng.data.timer_node_links.df.index[0] == HDL_TIMER and \
            lttng.data.timer_node_links.df.iloc[0]['timestamp'] == 100100702 and \
            lttng.data.timer_node_links.df.iloc[0]['node_handle'] == 1

        assert lttng.data.timer_node_links.df.index[1] == 1 and \
            lttng.data.timer_node_links.df.iloc[1]['timestamp'] == 100100772 and \
            lttng.data.timer_node_links.df.iloc[1]['node_handle'] == 1

        # callback_symbols
        # ['timestamp', 'symbol']
        assert lttng.data.callback_symbols.df.index[0] == TIMER_CALLBACK and \
            lttng.data.callback_symbols.df.iloc[0]['timestamp'] == 100100750 and \
            lttng.data.callback_symbols.df.iloc[0]['symbol'] == 100

        assert lttng.data.callback_symbols.df.index[1] == 3 and \
            lttng.data.callback_symbols.df.iloc[1]['timestamp'] == 100100782 and \
            lttng.data.callback_symbols.df.iloc[1]['symbol'] == 101

        # lifecycle_state_machines
        # ['node_handle']
        assert lttng.data.lifecycle_state_machines.df.index[0] == STATE_MACHINE and \
            lttng.data.lifecycle_state_machines.df.iloc[0]['node_handle'] == HDL_NODE

        assert lttng.data.lifecycle_state_machines.df.index[1] == 1 and \
            lttng.data.lifecycle_state_machines.df.iloc[1]['node_handle'] == 1

        # caret_init
        # ['timestamp', 'clock_offset', 'distribution']
        assert lttng.data.caret_init.df.iloc[0]['timestamp'] == 100100901

        # rmw_implementation
        # ['rmw_impl']
        assert lttng.data.rmw_impl.df.iloc[0]['rmw_impl'] == 10

        # executors
        # ['timestamp', 'executor_type_name']
        assert lttng.data.executors.df.index[0] == HDL_EXECUTOR and \
            lttng.data.executors.df.iloc[0]['timestamp'] == 100101101

        assert lttng.data.executors.df.index[1] == 1 and \
            lttng.data.executors.df.iloc[1]['timestamp'] == 100101103

        # executors_static
        # ['timestamp', 'entities_collector_addr', 'executor_type_name']
        assert lttng.data.executors_static.df.index[0] == HDL_EXECUTOR_STATIC and \
            lttng.data.executors_static.df.iloc[0]['timestamp'] == 100101102 and \
            lttng.data.executors_static.df.iloc[0]['entities_collector_addr'] == HDL_ENTITIES

        assert lttng.data.executors_static.df.index[1] == 2 and \
            lttng.data.executors_static.df.iloc[1]['timestamp'] == 100101203 and \
            lttng.data.executors_static.df.iloc[1]['entities_collector_addr'] == 1

        assert lttng.data.executors_static.df.index[2] == 3 and \
            lttng.data.executors_static.df.iloc[2]['timestamp'] == 100101205 and \
            lttng.data.executors_static.df.iloc[2]['entities_collector_addr'] == 2

        # callback_groups
        # ['timestamp', 'executor_addr', 'group_type_name']
        assert lttng.data.callback_groups.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_groups.df.iloc[0]['timestamp'] == 100101102 and \
            lttng.data.callback_groups.df.iloc[0]['executor_addr'] == HDL_EXECUTOR_STATIC

        assert lttng.data.callback_groups.df.index[1] == 1 and \
            lttng.data.callback_groups.df.iloc[1]['timestamp'] == 100101261 and \
            lttng.data.callback_groups.df.iloc[1]['executor_addr'] == 3

        # callback_groups_static
        # ['timestamp', 'entities_collector_addr', 'group_type_name']
        assert lttng.data.callback_groups_static.df.index[0] == EXECUTOR_STA_CALLBACK and \
            lttng.data.callback_groups_static.df.iloc[0]['timestamp'] == 100101204 and \
            lttng.data.callback_groups_static.df.iloc[0]['entities_collector_addr'] == 1

        assert lttng.data.callback_groups_static.df.index[1] == 2 and \
            lttng.data.callback_groups_static.df.iloc[1]['timestamp'] == 100101281 and \
            lttng.data.callback_groups_static.df.iloc[1]['entities_collector_addr'] == 2

        # callback_group_timer
        # ['timestamp', 'timer_handle']
        assert lttng.data.callback_group_timer.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_timer.df.iloc[0]['timestamp'] == 100100702 and \
            lttng.data.callback_group_timer.df.iloc[0]['timer_handle'] == HDL_TIMER

        assert lttng.data.callback_group_timer.df.index[1] == 1 and \
            lttng.data.callback_group_timer.df.iloc[1]['timestamp'] == 100101302 and \
            lttng.data.callback_group_timer.df.iloc[1]['timer_handle'] == 1

        # callback_group_subscription
        # ['timestamp', 'subscription_handle']
        assert lttng.data.callback_group_subscription.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_subscription.df.iloc[0]['timestamp'] == 100100401 and \
            lttng.data.callback_group_subscription.df.iloc[0]['subscription_handle'] \
            == HDL_SUBSCRIPTION

        assert lttng.data.callback_group_subscription.df.index[1] == 1 and \
            lttng.data.callback_group_subscription.df.iloc[1]['timestamp'] == 100101402 and \
            lttng.data.callback_group_subscription.df.iloc[1]['subscription_handle'] == 2

        # callback_group_service
        # ['timestamp', 'service_handle']
        assert lttng.data.callback_group_service.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_service.df.iloc[0]['timestamp'] == 100100501 and \
            lttng.data.callback_group_service.df.iloc[0]['service_handle'] == HDL_SERVICE

        assert lttng.data.callback_group_service.df.index[1] == 1 and \
            lttng.data.callback_group_service.df.iloc[1]['timestamp'] == 100101502 and \
            lttng.data.callback_group_service.df.iloc[1]['service_handle'] == 1

        # callback_group_client
        # ['timestamp', 'client_handle']
        assert lttng.data.callback_group_client.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_client.df.iloc[0]['timestamp'] == 100100601 and \
            lttng.data.callback_group_client.df.iloc[0]['client_handle'] == HDL_CLIENT

        assert lttng.data.callback_group_client.df.index[1] == 1 and \
            lttng.data.callback_group_client.df.iloc[1]['timestamp'] == 100101602 and \
            lttng.data.callback_group_client.df.iloc[1]['client_handle'] == 1

        # ring_buffers
        # ['timestamp', 'capacity']
        assert lttng.data.ring_buffers.df.index[0] == RING_BUFFER and \
            lttng.data.ring_buffers.df.iloc[0]['timestamp'] == 100101701 and \
            lttng.data.ring_buffers.df.iloc[0]['capacity'] == 100

        assert lttng.data.ring_buffers.df.index[1] == 1 and \
            lttng.data.ring_buffers.df.iloc[1]['timestamp'] == 100101703 and \
            lttng.data.ring_buffers.df.iloc[1]['capacity'] == 101

        # buffer_to_ipbs
        # ['timestamp', 'buffer']
        assert lttng.data.buffer_to_ipbs.df.index[0] == RING_BUFFER and \
            lttng.data.buffer_to_ipbs.df.iloc[0]['timestamp'] == 100101702 and \
            lttng.data.buffer_to_ipbs.df.iloc[0]['ipb'] == BUFFER_JPB

        assert lttng.data.buffer_to_ipbs.df.index[1] == 1 and \
            lttng.data.buffer_to_ipbs.df.iloc[1]['timestamp'] == 100101803 and \
            lttng.data.buffer_to_ipbs.df.iloc[1]['ipb'] == 1

        # ipb_to_subscriptions
        # ['tid', 'callback_start_timestamp', 'callback_object', 'is_intra_process']
        assert lttng.data.ipb_to_subscriptions.df.index[0] == BUFFER_JPB and \
            lttng.data.ipb_to_subscriptions.df.iloc[0]['timestamp'] == 100101802 and \
            lttng.data.ipb_to_subscriptions.df.iloc[0]['subscription'] == 1

        assert lttng.data.ipb_to_subscriptions.df.index[1] == 1 and \
            lttng.data.ipb_to_subscriptions.df.iloc[1]['timestamp'] == 100101902 and \
            lttng.data.ipb_to_subscriptions.df.iloc[1]['subscription'] == 1

        # callback_start_instances
        # ['tid', 'callback_start_timestamp', 'callback_object', 'is_intra_process']
        assert lttng.data.callback_start_instances.\
            data[0].data['callback_object'] == TIMER_CALLBACK and \
            lttng.data.callback_start_instances.\
            data[0].data['callback_start_timestamp'] == 100100750 and \
            lttng.data.callback_start_instances.data[0].data['is_intra_process'] == 100

        # Note: 1 and 2 are registered in callback_object of SUBSCRIPTION,HDL_SERVICE,
        #   so it becomes 3.
        assert lttng.data.callback_start_instances.data[1].data['callback_object'] == 3 and \
            lttng.data.callback_start_instances.\
            data[1].data['callback_start_timestamp'] == 100102002 and \
            lttng.data.callback_start_instances.data[1].data['is_intra_process'] == 101

        # callback_end_instances
        # ['tid', 'callback_end_timestamp', 'callback_object']
        assert lttng.data.callback_end_instances.\
            data[0].data['callback_object'] == SERVICE_CALLBACK and \
            lttng.data.callback_end_instances.\
            data[0].data['callback_end_timestamp'] == 100100501

        # Note: 1 and 3 are registered in callback_object of SUBSCRIPTION,TIMER_CALLBACK,
        #   so it becomes 2.
        assert lttng.data.callback_end_instances.\
            data[1].data['callback_object'] == 2 and \
            lttng.data.callback_end_instances.\
            data[1].data['callback_end_timestamp'] == 100102102

        # message_construct_instances
        # ['message_construct_timestamp', 'original_message', 'constructed_message']
        assert lttng.data.message_construct_instances.\
            data[0].data['original_message'] == 10 and \
            lttng.data.message_construct_instances.\
            data[0].data['constructed_message'] == 20 and \
            lttng.data.message_construct_instances.\
            data[0].data['message_construct_timestamp'] == 100102201

        # rclcpp_intra_publish_instances
        # ['tid', 'rclcpp_intra_publish_timestamp',
        #  'publisher_handle', 'message', 'message_timestamp']
        assert lttng.data.rclcpp_intra_publish_instances.\
            data[0].data['publisher_handle'] == HDL_PUBLISHER and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[0].data['rclcpp_intra_publish_timestamp'] == 100100302 and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[0].data['message_timestamp'] == 100100305

        assert lttng.data.rclcpp_intra_publish_instances.\
            data[1].data['publisher_handle'] == 1 and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[1].data['rclcpp_intra_publish_timestamp'] == 100102303 and \
            lttng.data.rclcpp_intra_publish_instances.\
            data[1].data['message_timestamp'] == 100102304

        # dispatch_subscription_callback_instances
        # ['dispatch_subscription_callback_timestamp', 'callback_object',
        #  'message', 'source_timestamp', 'message_timestamp']
        assert lttng.data.dispatch_subscription_callback_instances.\
            data[0].data['callback_object'] == SUBSCRIPTION_CALLBACK and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[0].data['source_timestamp'] == 100100453 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[0].data['message_timestamp'] == 100100454 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[0].data['dispatch_subscription_callback_timestamp'] == 100100450

        assert lttng.data.dispatch_subscription_callback_instances.\
            data[1].data['callback_object'] == 1 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[1].data['source_timestamp'] == 100102404 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[1].data['message_timestamp'] == 100102405 and \
            lttng.data.dispatch_subscription_callback_instances.\
            data[1].data['dispatch_subscription_callback_timestamp'] == 100102402

        # rmw_take_instances
        # ['tid', 'rmw_take_timestamp', 'rmw_subscription_handle',
        #  'message', 'source_timestamp']
        assert lttng.data.rmw_take_instances.\
            data[0].data['rmw_subscription_handle'] == HDL_RMW_SUBSCRIPTION and \
            lttng.data.rmw_take_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.rmw_take_instances.\
            data[0].data['source_timestamp'] == 100100453 and \
            lttng.data.rmw_take_instances.\
            data[0].data['rmw_take_timestamp'] == 100100401

        assert lttng.data.rmw_take_instances.\
            data[1].data['rmw_subscription_handle'] == 2 and \
            lttng.data.rmw_take_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.rmw_take_instances.\
            data[1].data['source_timestamp'] == 100102504 and \
            lttng.data.rmw_take_instances.\
            data[1].data['rmw_take_timestamp'] == 100102503

        # dispatch_intra_process_subscription_callback_instances
        # ['dispatch_intra_process_subscription_callback_timestamp', 'callback_object',
        #  'message', 'message_timestamp']
        assert lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[0].data['callback_object'] == SUBSCRIPTION_CALLBACK and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[0].data['message_timestamp'] == 100100454 and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.data[0].\
            data['dispatch_intra_process_subscription_callback_timestamp'] == 100100450

        assert lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[1].data['callback_object'] == 1 and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.\
            data[1].data['message_timestamp'] == 100102603 and \
            lttng.data.dispatch_intra_process_subscription_callback_instances.data[1].\
            data['dispatch_intra_process_subscription_callback_timestamp'] == 100102602

        # rcl_publish_instances
        # ['tid', 'rcl_publish_timestamp', 'publisher_handle', 'message']
        assert lttng.data.rcl_publish_instances.\
            data[0].data['publisher_handle'] == HDL_PUBLISHER and \
            lttng.data.rcl_publish_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.rcl_publish_instances.\
            data[0].data['rcl_publish_timestamp'] == 100100302

        assert lttng.data.rcl_publish_instances.\
            data[1].data['publisher_handle'] == 1 and \
            lttng.data.rcl_publish_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.rcl_publish_instances.\
            data[1].data['rcl_publish_timestamp'] == 100102703

        # rclcpp_publish_instances
        # ['tid', 'rclcpp_inter_publish_timestamp',
        #  'publisher_handle', 'message', 'message_timestamp']
        assert lttng.data.rclcpp_publish_instances.\
            data[0].data['publisher_handle'] == HDL_PUBLISHER and \
            lttng.data.rclcpp_publish_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.rclcpp_publish_instances.\
            data[0].data['message_timestamp'] == 100100306 and \
            lttng.data.rclcpp_publish_instances.\
            data[0].data['rclcpp_inter_publish_timestamp'] == 100100302

        assert lttng.data.rclcpp_publish_instances.\
            data[1].data['publisher_handle'] == 1 and \
            lttng.data.rclcpp_publish_instances.\
            data[1].data['message'] == 101 and \
            lttng.data.rclcpp_publish_instances.\
            data[1].data['message_timestamp'] == 100102804 and \
            lttng.data.rclcpp_publish_instances.\
            data[1].data['rclcpp_inter_publish_timestamp'] == 100102803

        assert lttng.data.rclcpp_publish_instances.\
            data[2].data['publisher_handle'] == 0 and \
            lttng.data.rclcpp_publish_instances.\
            data[2].data['message'] == 102 and \
            lttng.data.rclcpp_publish_instances.\
            data[2].data['message_timestamp'] == 0 and \
            lttng.data.rclcpp_publish_instances.\
            data[2].data['rclcpp_inter_publish_timestamp'] == 100102804

        # dds_write_instances
        # ['tid', 'dds_write_timestamp', 'message']
        assert lttng.data.dds_write_instances.\
            data[0].data['message'] == 100 and \
            lttng.data.dds_write_instances.\
            data[0].data['dds_write_timestamp'] == 100102901

        # dds_bind_addr_to_stamp
        # ['tid', 'dds_bind_addr_to_stamp_timestamp', 'addr', 'source_timestamp']
        assert lttng.data.dds_bind_addr_to_stamp.\
            data[0].data['addr'] == 100 and \
            lttng.data.dds_bind_addr_to_stamp.\
            data[0].data['source_timestamp'] == 101 and \
            lttng.data.dds_bind_addr_to_stamp.\
            data[0].data['dds_bind_addr_to_stamp_timestamp'] == 100103001

        # dds_bind_addr_to_addr
        # ['dds_bind_addr_to_addr_timestamp', 'addr_from', 'addr_to']
        assert lttng.data.dds_bind_addr_to_addr.\
            data[0].data['addr_from'] == 100 and \
            lttng.data.dds_bind_addr_to_addr.\
            data[0].data['addr_to'] == 101 and \
            lttng.data.dds_bind_addr_to_addr.\
            data[0].data['dds_bind_addr_to_addr_timestamp'] == 100103101

        # lifecycle_transitions
        # ['state_machine_handle', 'start_label', 'goal_label', 'timestamp']
        assert lttng.data.lifecycle_transitions.\
            df.iloc[0]['state_machine_handle'] == STATE_MACHINE and \
            lttng.data.lifecycle_transitions.df.iloc[0]['timestamp'] == 100100801 and \
            lttng.data.lifecycle_transitions.df.iloc[0]['start_label'] == 100 and \
            lttng.data.lifecycle_transitions.df.iloc[0]['goal_label'] == 101

        assert lttng.data.lifecycle_transitions.df.iloc[1]['state_machine_handle'] == 1 and \
            lttng.data.lifecycle_transitions.df.iloc[1]['timestamp'] == 100103201 and \
            lttng.data.lifecycle_transitions.df.iloc[1]['start_label'] == 200 and \
            lttng.data.lifecycle_transitions.df.iloc[1]['goal_label'] == 201

        # on_data_available_instances
        # ['on_data_available_timestamp', 'source_timestamp']
        assert lttng.data.on_data_available_instances.\
            data[0].data['source_timestamp'] == 100 and \
            lttng.data.on_data_available_instances.\
            data[0].data['on_data_available_timestamp'] == 100103301

        # sim_time
        # ['system_time', 'sim_time']
        assert lttng.data.sim_time.\
            data[0].data['sim_time'] == 100 and \
            lttng.data.sim_time.\
            data[0].data['system_time'] == 100103401

        # rclcpp_ring_buffer_enqueue_instances
        # ['tid', 'rclcpp_ring_buffer_enqueue_timestamp', 'buffer', 'index', 'size', 'overwritten']
        assert lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[0].data['buffer'] == RING_BUFFER and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[0].data['index'] == 100 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[0].data['size'] == 101 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[0].data['overwritten'] == 102 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[0].data['rclcpp_ring_buffer_enqueue_timestamp'] == 100101702

        assert lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[1].data['buffer'] == 1 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[1].data['index'] == 200 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[1].data['size'] == 201 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[1].data['overwritten'] == 202 and \
            lttng.data.rclcpp_ring_buffer_enqueue_instances.\
            data[1].data['rclcpp_ring_buffer_enqueue_timestamp'] == 100103501

        # rclcpp_ring_buffer_dequeue_instances
        # ['tid', 'rclcpp_ring_buffer_dequeue_timestamp', 'buffer', 'index', 'size']
        assert lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[0].data['buffer'] == RING_BUFFER and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[0].data['index'] == 100 and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[0].data['size'] == 101 and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[0].data['rclcpp_ring_buffer_dequeue_timestamp'] == 100101702

        assert lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[1].data['buffer'] == 1 and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[1].data['index'] == 200 and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[1].data['size'] == 201 and \
            lttng.data.rclcpp_ring_buffer_dequeue_instances.\
            data[1].data['rclcpp_ring_buffer_dequeue_timestamp'] == 100103601
