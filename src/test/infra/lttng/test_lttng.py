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


from datetime import datetime
import functools

from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.event_counter import EventCounter
from caret_analyze.infra.lttng.lttng import EventCollection, IterableEvents, MultiHostIdRemapper
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

    @pytest.mark.parametrize(
        'need_validate',
        [True, False]
    )
    def test_multi_host_validate(self, caplog, mocker, need_validate):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))
        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
                     return_value=counter_mock)
        mocker.patch('os.path.exists', return_value=True)

        Lttng(['test1', 'test2'], validate=need_validate)
        if need_validate:
            assert 'multiple LTTng log is not supported.' in caplog.messages[0]
        else:
            assert len(caplog.messages) == 0

    def test_get_nodes(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        mocker.patch.object(lttng_info_mock, 'get_rmw_impl', return_value='rmw')
        assert lttng.get_rmw_impl() == 'rmw'

    def test_get_publishers(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
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
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'callback_records',
                            records_mock)
        assert lttng.compose_callback_records() == records_mock

    def test_compose_rmw_take_records(self, mocker):
        data_mock = mocker.Mock(spec=Ros2DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data_mock, {}, 0, 1))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.lttng.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir')

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'rmw_take_records',
                            records_mock)
        assert lttng.compose_rmw_take_records() == records_mock

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

    @pytest.mark.parametrize(
        'distribution',
        ['jazzy', 'iron'],
    )
    def test_compare_init_event(self, distribution):
        init_events = []
        event_collection = {}

        TIME_ORDER = 0
        SORT_ORDER_ADD_IRON = 4
        if distribution[0] >= 'jazzy'[0]:
            SORT_ORDER_EXE_AND_CB = 10
            SORT_ORDER_INIT = 20
            SMALL_ORDER = 50
            RESULT_ORDER_INIT = 0
            RESULT_ORDER_EXE_AND_CB = 34
            RESULT_ORDER_ETC = 47
        else:
            SORT_ORDER_EXE_AND_CB = 10
            SORT_ORDER_INIT = 18
            SMALL_ORDER = 48
            RESULT_ORDER_INIT = 0
            RESULT_ORDER_EXE_AND_CB = 34
            RESULT_ORDER_ETC = 45

        # for testing sorting with _timestamp
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

        # executor and callback group
        if distribution[0] >= 'jazzy'[0]:
            event_collection[SORT_ORDER_EXE_AND_CB+0] = {
                '_name': 'ros2_caret:callback_group_add_client', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+1] = {
                '_name': 'ros2_caret:callback_group_add_service', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+2] = {
                '_name': 'ros2_caret:callback_group_add_subscription', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+3] = {
                '_name': 'ros2_caret:callback_group_add_timer', '_timestamp': 600, }

            event_collection[SORT_ORDER_EXE_AND_CB+4] = {
                '_name': 'ros2_caret:add_callback_group_static_executor', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+5] = {
                '_name': 'ros2_caret:add_callback_group', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+6] = {
                '_name': 'ros2_caret:construct_static_executor', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+7] = {
                '_name': 'ros2_caret:construct_executor', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+8] = {
                '_name': 'ros2_caret:callback_group_to_executor_entity_collector',
                '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+9] = {
                '_name': 'ros2_caret:executor_entity_collector_to_executor', '_timestamp': 600, }

        else:
            event_collection[SORT_ORDER_EXE_AND_CB+0] = {
                '_name': 'ros2_caret:callback_group_add_client', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+1] = {
                '_name': 'ros2_caret:callback_group_add_service', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+2] = {
                '_name': 'ros2_caret:callback_group_add_subscription', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+3] = {
                '_name': 'ros2_caret:callback_group_add_timer', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+4] = {
                '_name': 'ros2_caret:add_callback_group_static_executor', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+5] = {
                '_name': 'ros2_caret:add_callback_group', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+6] = {
                '_name': 'ros2_caret:construct_static_executor', '_timestamp': 600, }
            event_collection[SORT_ORDER_EXE_AND_CB+7] = {
                '_name': 'ros2_caret:construct_executor', '_timestamp': 600, }
        # initialization trace points
        event_collection[SORT_ORDER_INIT+0] = {
            '_name': 'ros2_caret:rmw_implementation', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+1] = {
            '_name': 'ros2_caret:caret_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+2] = {
            '_name': 'ros2:rcl_lifecycle_state_machine_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+3] = {
            '_name': 'ros2_caret:rcl_lifecycle_state_machine_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+4] = {
            '_name': 'ros2:rclcpp_callback_register', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+5] = {
            '_name': 'ros2_caret:rclcpp_callback_register', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+6] = {
            '_name': 'ros2:rclcpp_timer_link_node', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+7] = {
            '_name': 'ros2_caret:rclcpp_timer_link_node', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+8] = {
            '_name': 'ros2:rclcpp_timer_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+9] = {
            '_name': 'ros2_caret:rclcpp_timer_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+10] = {
            '_name': 'ros2:rcl_timer_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+11] = {
            '_name': 'ros2_caret:rcl_timer_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+12] = {
            '_name': 'ros2:rcl_client_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+13] = {
            '_name': 'ros2_caret:rcl_client_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+14] = {
            '_name': 'ros2:rclcpp_service_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+15] = {
            '_name': 'ros2_caret:rclcpp_service_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+16] = {
            '_name': 'ros2:rcl_service_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+17] = {
            '_name': 'ros2_caret:rcl_service_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+18] = {
            '_name': 'ros2:rclcpp_subscription_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+19] = {
            '_name': 'ros2_caret:rclcpp_subscription_callback_added', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+20] = {
            '_name': 'ros2:rclcpp_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+21] = {
            '_name': 'ros2_caret:rclcpp_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+22] = {
            '_name': 'ros2:rcl_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+23] = {
            '_name': 'ros2_caret:rcl_subscription_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+24] = {
            '_name': 'ros2:rcl_publisher_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+25] = {
            '_name': 'ros2_caret:rcl_publisher_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+26] = {
            '_name': 'ros2:rcl_node_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+27] = {
            '_name': 'ros2_caret:rcl_node_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+28] = {
            '_name': 'ros2:rcl_init', '_timestamp': 600, }
        event_collection[SORT_ORDER_INIT+29] = {
            '_name': 'ros2_caret:rcl_init', '_timestamp': 600, }
        # also check when _timestamp is small
        event_collection[SMALL_ORDER+0] = {
            '_name': 'ros2_caret:callback_group_add_client', '_timestamp': 400, }
        event_collection[SMALL_ORDER+1] = {
            '_name': 'ros2_caret:callback_group_add_service', '_timestamp': 400, }
        event_collection[SMALL_ORDER+2] = {
            '_name': 'ros2_caret:callback_group_add_subscription', '_timestamp': 400, }

        for i in range(len(event_collection)):
            init_events.append(event_collection[i])

        init_events.sort(key=functools.cmp_to_key(Lttng._compare_init_event))

        # Sorted in the following priority
        # first  : sorted by _prioritized_init_events order
        # second : sorted by timestamp
        assert init_events[RESULT_ORDER_INIT+0]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+0]['_name'] == 'ros2_caret:rcl_init'
        assert init_events[RESULT_ORDER_INIT+1]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+1]['_name'] == 'ros2:rcl_init'
        assert init_events[RESULT_ORDER_INIT+2]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+2]['_name'] == 'ros2_caret:rcl_node_init'
        assert init_events[RESULT_ORDER_INIT+3]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+3]['_name'] == 'ros2:rcl_node_init'
        assert init_events[RESULT_ORDER_INIT+4]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+4]['_name'] == 'ros2_caret:rcl_publisher_init'
        assert init_events[RESULT_ORDER_INIT+5]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+5]['_name'] == 'ros2:rcl_publisher_init'
        assert init_events[RESULT_ORDER_INIT+6]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+6]['_name'] == 'ros2_caret:rcl_subscription_init'
        assert init_events[RESULT_ORDER_INIT+7]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+7]['_name'] == 'ros2:rcl_subscription_init'
        assert init_events[RESULT_ORDER_INIT+8]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+8]['_name'] == 'ros2_caret:rclcpp_subscription_init'
        assert init_events[RESULT_ORDER_INIT+9]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+9]['_name'] == 'ros2:rclcpp_subscription_init'
        assert init_events[RESULT_ORDER_INIT+10]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+10]['_name'] == \
            'ros2_caret:rclcpp_subscription_callback_added'
        assert init_events[RESULT_ORDER_INIT+11]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+11]['_name'] == \
            'ros2:rclcpp_subscription_callback_added'
        assert init_events[RESULT_ORDER_INIT+12]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+12]['_name'] == 'ros2_caret:rcl_service_init'
        assert init_events[RESULT_ORDER_INIT+13]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+13]['_name'] == 'ros2:rcl_service_init'
        assert init_events[RESULT_ORDER_INIT+14]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+14]['_name'] == \
            'ros2_caret:rclcpp_service_callback_added'
        assert init_events[RESULT_ORDER_INIT+15]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+15]['_name'] == 'ros2:rclcpp_service_callback_added'
        assert init_events[RESULT_ORDER_INIT+16]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+16]['_name'] == 'ros2_caret:rcl_client_init'
        assert init_events[RESULT_ORDER_INIT+17]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+17]['_name'] == 'ros2:rcl_client_init'
        assert init_events[RESULT_ORDER_INIT+18]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+18]['_name'] == 'ros2_caret:rcl_timer_init'
        assert init_events[RESULT_ORDER_INIT+19]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+19]['_name'] == 'ros2:rcl_timer_init'
        assert init_events[RESULT_ORDER_INIT+20]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+20]['_name'] == \
            'ros2_caret:rclcpp_timer_callback_added'
        assert init_events[RESULT_ORDER_INIT+21]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+21]['_name'] == 'ros2:rclcpp_timer_callback_added'
        assert init_events[RESULT_ORDER_INIT+22]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+22]['_name'] == 'ros2_caret:rclcpp_timer_link_node'
        assert init_events[RESULT_ORDER_INIT+23]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+23]['_name'] == 'ros2:rclcpp_timer_link_node'
        assert init_events[RESULT_ORDER_INIT+24]['_timestamp'] == 500 and \
            init_events[RESULT_ORDER_INIT+24]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+25]['_timestamp'] == 501 and \
            init_events[RESULT_ORDER_INIT+25]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+26]['_timestamp'] == 502 and \
            init_events[RESULT_ORDER_INIT+26]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+27]['_timestamp'] == 503 and \
            init_events[RESULT_ORDER_INIT+27]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+28]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+28]['_name'] == 'ros2_caret:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+29]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+29]['_name'] == 'ros2:rclcpp_callback_register'
        assert init_events[RESULT_ORDER_INIT+30]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+30]['_name'] == \
            'ros2_caret:rcl_lifecycle_state_machine_init'
        assert init_events[RESULT_ORDER_INIT+31]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+31]['_name'] == 'ros2:rcl_lifecycle_state_machine_init'
        assert init_events[RESULT_ORDER_INIT+32]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+32]['_name'] == 'ros2_caret:caret_init'
        assert init_events[RESULT_ORDER_INIT+33]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_INIT+33]['_name'] == 'ros2_caret:rmw_implementation'

        if distribution[0] >= 'jazzy'[0]:
            assert init_events[RESULT_ORDER_EXE_AND_CB+0]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+0]['_name'] == \
                'ros2_caret:executor_entity_collector_to_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+1]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+1]['_name'] == \
                'ros2_caret:callback_group_to_executor_entity_collector'
            assert init_events[RESULT_ORDER_EXE_AND_CB+2]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+2]['_name'] == \
                'ros2_caret:construct_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+3]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+3]['_name'] == \
                'ros2_caret:construct_static_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+4]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+4]['_name'] == \
                'ros2_caret:add_callback_group'
            assert init_events[RESULT_ORDER_EXE_AND_CB+5]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+5]['_name'] == \
                'ros2_caret:add_callback_group_static_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+6]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+6]['_name'] == \
                'ros2_caret:callback_group_add_timer'
            assert init_events[RESULT_ORDER_EXE_AND_CB+7]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+7]['_name'] == \
                'ros2_caret:callback_group_add_subscription'
            assert init_events[RESULT_ORDER_EXE_AND_CB+8]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+8]['_name'] == \
                'ros2_caret:callback_group_add_subscription'
            assert init_events[RESULT_ORDER_EXE_AND_CB+9]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+9]['_name'] == \
                'ros2_caret:callback_group_add_service'
            assert init_events[RESULT_ORDER_EXE_AND_CB+10]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+10]['_name'] == \
                'ros2_caret:callback_group_add_service'
            assert init_events[RESULT_ORDER_EXE_AND_CB+11]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+11]['_name'] == \
                'ros2_caret:callback_group_add_client'
            assert init_events[RESULT_ORDER_EXE_AND_CB+12]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+12]['_name'] == \
                'ros2_caret:callback_group_add_client'
        else:
            assert init_events[RESULT_ORDER_EXE_AND_CB+0]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+0]['_name'] == \
                'ros2_caret:construct_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+1]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+1]['_name'] == \
                'ros2_caret:construct_static_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+2]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+2]['_name'] == \
                'ros2_caret:add_callback_group'
            assert init_events[RESULT_ORDER_EXE_AND_CB+3]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+3]['_name'] == \
                'ros2_caret:add_callback_group_static_executor'
            assert init_events[RESULT_ORDER_EXE_AND_CB+4]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+4]['_name'] == \
                'ros2_caret:callback_group_add_timer'
            assert init_events[RESULT_ORDER_EXE_AND_CB+5]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+5]['_name'] == \
                'ros2_caret:callback_group_add_subscription'
            assert init_events[RESULT_ORDER_EXE_AND_CB+6]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+6]['_name'] == \
                'ros2_caret:callback_group_add_subscription'
            assert init_events[RESULT_ORDER_EXE_AND_CB+7]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+7]['_name'] == \
                'ros2_caret:callback_group_add_service'
            assert init_events[RESULT_ORDER_EXE_AND_CB+8]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+8]['_name'] == \
                'ros2_caret:callback_group_add_service'
            assert init_events[RESULT_ORDER_EXE_AND_CB+9]['_timestamp'] == 400 and \
                init_events[RESULT_ORDER_EXE_AND_CB+9]['_name'] == \
                'ros2_caret:callback_group_add_client'
            assert init_events[RESULT_ORDER_EXE_AND_CB+10]['_timestamp'] == 600 and \
                init_events[RESULT_ORDER_EXE_AND_CB+10]['_name'] == \
                'ros2_caret:callback_group_add_client'

        assert init_events[RESULT_ORDER_ETC+0]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+0]['_name'] == 'ros2_caret:rclcpp_construct_ring_buffer'
        assert init_events[RESULT_ORDER_ETC+1]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+1]['_name'] == 'ros2:rclcpp_construct_ring_buffer'
        assert init_events[RESULT_ORDER_ETC+2]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+2]['_name'] == 'ros2_caret:rclcpp_buffer_to_ipb'
        assert init_events[RESULT_ORDER_ETC+3]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+3]['_name'] == 'ros2:rclcpp_buffer_to_ipb'
        assert init_events[RESULT_ORDER_ETC+4]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+4]['_name'] == 'ros2_caret:rclcpp_ipb_to_subscription'
        assert init_events[RESULT_ORDER_ETC+5]['_timestamp'] == 600 and \
            init_events[RESULT_ORDER_ETC+5]['_name'] == 'ros2:rclcpp_ipb_to_subscription'

    def test_duplicated_events_contexts(self):
        HDL_CONTEXT = 1000101
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # contexts
        # ['timestamp', 'pid']
        assert lttng.data.contexts.df.index[0] == HDL_CONTEXT and \
            lttng.data.contexts.df.iloc[0]['timestamp'] == 100100101
        # HDL_CONTEXT is duplicated and "1" is newly assigned
        assert lttng.data.contexts.df.index[1] == 1 and \
            lttng.data.contexts.df.iloc[1]['timestamp'] == 100100102

    def test_duplicated_events_nodes(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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

    def test_duplicated_events_publisher(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_PUBLISHER = 1000301
        HDL_RMW_PUBLISHER = 1000311
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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

    def test_duplicated_events_subscriptions(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_SUBSCRIPTION = 1000401
        HDL_RMW_SUBSCRIPTION = 1000411
        SUBSCRIPTION = 1000451
        SUBSCRIPTION_CALLBACK = 1000461
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
                '_timestamp': 100100405,
                # '_timestamp': 100100450,
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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
            lttng.data.callback_objects.df.iloc[0]['timestamp'] == 100100405 and \
            lttng.data.callback_objects.df.iloc[0]['callback_object'] == SUBSCRIPTION_CALLBACK

        assert lttng.data.callback_objects.df.index[1] == 1 and \
            lttng.data.callback_objects.df.iloc[1]['timestamp'] == 100100462 and \
            lttng.data.callback_objects.df.iloc[1]['callback_object'] == 1

    def test_duplicated_events_service(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_SERVICE = 1000501
        HDL_RMW_SERVICE = 1000511
        SERVICE_CALLBACK = 1000561
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
                '_timestamp': 100100212,
                'service_name': 'my_service_name',
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_service_callback_added',
                'service_handle': HDL_SERVICE,
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100100205,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:rclcpp_service_callback_added',
                'service_handle': HDL_SERVICE,
                'callback': SERVICE_CALLBACK,
                '_timestamp': 100100221,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # services
        # ['timestamp', 'node_handle', 'rmw_handle', 'service_name']
        assert lttng.data.services.df.index[0] == HDL_SERVICE and \
            lttng.data.services.df.iloc[0]['timestamp'] == 100100202 and \
            lttng.data.services.df.iloc[0]['node_handle'] == HDL_NODE and \
            lttng.data.services.df.iloc[0]['rmw_handle'] == HDL_RMW_SERVICE

        assert lttng.data.services.df.index[1] == 1 and \
            lttng.data.services.df.iloc[1]['timestamp'] == 100100212 and \
            lttng.data.services.df.iloc[1]['node_handle'] == 1 and \
            lttng.data.services.df.iloc[1]['rmw_handle'] == 1

        # callback_objects
        # ['timestamp', 'callback_object']
        assert lttng.data.callback_objects.df.index[0] == HDL_SERVICE and \
            lttng.data.callback_objects.df.iloc[0]['timestamp'] == 100100205 and \
            lttng.data.callback_objects.df.iloc[0]['callback_object'] == SERVICE_CALLBACK

        assert lttng.data.callback_objects.df.index[1] == 1 and \
            lttng.data.callback_objects.df.iloc[1]['timestamp'] == 100100221 and \
            lttng.data.callback_objects.df.iloc[1]['callback_object'] == 1

    def test_duplicated_events_clients(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_CLIENT = 1000601
        HDL_RMW_CLIENT = 1000611
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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

    def test_duplicated_events_timers(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        TIMER_CALLBACK = 1000661
        HDL_TIMER = 1000701
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
                '_timestamp': 100100705,
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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
        assert lttng.data.callback_objects.df.index[0] == HDL_TIMER and \
            lttng.data.callback_objects.df.iloc[0]['timestamp'] == 100100702 and \
            lttng.data.callback_objects.df.iloc[0]['callback_object'] == TIMER_CALLBACK

        # Note: 1 and 2 are registered in callback_object of SUBSCRIPTION,HDL_SERVICE,
        #   so it becomes 3.
        assert lttng.data.callback_objects.df.index[1] == 1 and \
            lttng.data.callback_objects.df.iloc[1]['timestamp'] == 100100751 and \
            lttng.data.callback_objects.df.iloc[1]['callback_object'] == 1

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
            lttng.data.callback_symbols.df.iloc[0]['timestamp'] == 100100705 and \
            lttng.data.callback_symbols.df.iloc[0]['symbol'] == 100

        assert lttng.data.callback_symbols.df.index[1] == 1 and \
            lttng.data.callback_symbols.df.iloc[1]['timestamp'] == 100100782 and \
            lttng.data.callback_symbols.df.iloc[1]['symbol'] == 101

    def test_duplicated_events_lifecycle_state_machines(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        STATE_MACHINE = 1000801
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # lifecycle_state_machines
        # ['node_handle']
        assert lttng.data.lifecycle_state_machines.df.index[0] == STATE_MACHINE and \
            lttng.data.lifecycle_state_machines.df.iloc[0]['node_handle'] == HDL_NODE

        assert lttng.data.lifecycle_state_machines.df.index[1] == 1 and \
            lttng.data.lifecycle_state_machines.df.iloc[1]['node_handle'] == 1

    def test_duplicated_events_caret_init(self):
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
            {
                '_name': 'ros2_caret:caret_init',
                'clock_offset': 10,
                'distribution': 'dummy',
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # caret_init
        # ['timestamp', 'clock_offset', 'distribution']
        assert lttng.data.caret_init.df.iloc[0]['timestamp'] == 100100901

        # rmw_implementation
        # ['rmw_impl']
        assert lttng.data.rmw_impl.df.iloc[0]['rmw_impl'] == 10

    def test_duplicated_events_entities_collector(self):
        HDL_EXECUTOR = 1001101
        EXECUTOR_CALLBACK = 1001261
        HDL_EXECUTOR_ENTITY = 1000701
        VTID1 = 500001
        VPID1 = 600001

        events = [
            {
                '_name': 'ros2_caret:caret_init',
                'clock_offset': 10,
                'distribution': 'jazzy',
                '_timestamp': 100100901,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_to_executor_entity_collector',
                'entities_collector_addr': HDL_EXECUTOR_ENTITY,
                'callback_group_addr': EXECUTOR_CALLBACK,
                'group_type_name': 'reentrant',
                '_timestamp': 100101101,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:callback_group_to_executor_entity_collector',
                'entities_collector_addr': HDL_EXECUTOR_ENTITY,
                'callback_group_addr': EXECUTOR_CALLBACK,
                'group_type_name': 'mutually_exclusive',
                '_timestamp': 100101260,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:executor_entity_collector_to_executor',
                'executor_addr': HDL_EXECUTOR,
                'entities_collector_addr': HDL_EXECUTOR_ENTITY,
                '_timestamp': 100101102,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2_caret:executor_entity_collector_to_executor',
                'executor_addr': HDL_EXECUTOR,
                'entities_collector_addr': HDL_EXECUTOR_ENTITY,
                '_timestamp': 100101261,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

        # executors
        # ['timestamp', 'callback_group_addr', 'callback_group_collection_addr']
        assert lttng.data.callback_group_to_executor_entity_collector.\
            df.index[0] == HDL_EXECUTOR_ENTITY and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[0]['group_type_name'] == 'reentrant' and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[0]['timestamp'] == 100101101 and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[0]['callback_group_addr'] == EXECUTOR_CALLBACK

        assert lttng.data.callback_group_to_executor_entity_collector.\
            df.index[1] == 1 and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[1]['group_type_name'] == 'mutually_exclusive' and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[1]['timestamp'] == 100101260 and \
            lttng.data.callback_group_to_executor_entity_collector.\
            df.iloc[1]['callback_group_addr'] == 1

    @pytest.mark.parametrize(
        'distribution',
        ['jazzy', 'iron'],
    )
    def test_duplicated_events_executors(self, distribution):
        HDL_EXECUTOR = 1001101
        HDL_EXECUTOR_STATIC = 1001201
        HDL_ENTITIES = 1001211
        VTID1 = 500001
        VPID1 = 600001

        if distribution[0] >= 'jazzy'[0]:
            events = [
                {
                    '_name': 'ros2_caret:caret_init',
                    'clock_offset': 10,
                    'distribution': distribution,
                    '_timestamp': 100100901,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:executor_entity_collector_to_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    '_timestamp': 100101102,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:executor_entity_collector_to_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    '_timestamp': 100101261,
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
                    '_timestamp': 100101263,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:construct_static_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101103,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:construct_static_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101264,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
            ]

            lttng = Lttng(events, event_filters=[], validate=False)

            # executors
            # ['timestamp', 'executor_type_name']
            assert lttng.data.executors.df.index[0] == HDL_EXECUTOR and \
                lttng.data.executors.df.iloc[0]['timestamp'] == 100101101

            assert lttng.data.executors.df.index[1] == 1 and \
                lttng.data.executors.df.iloc[1]['timestamp'] == 100101263

            # executors_static
            # ['timestamp', 'entities_collector_addr', 'executor_type_name']
            assert lttng.data.executors_static.df.index[0] == HDL_EXECUTOR and \
                lttng.data.executors_static.df.iloc[0]['timestamp'] == 100101103 and \
                lttng.data.executors_static.df.iloc[0]['entities_collector_addr'] == HDL_ENTITIES

            assert lttng.data.executors_static.df.index[1] == 1 and \
                lttng.data.executors_static.df.iloc[1]['timestamp'] == 100101264 and \
                lttng.data.executors_static.df.iloc[1]['entities_collector_addr'] == 1

            # ['timestamp', 'entities_collector_addr']
            assert lttng.data.executor_entity_collector_to_executor.\
                df.index[0] == HDL_EXECUTOR and \
                lttng.data.executor_entity_collector_to_executor.\
                df.iloc[0]['timestamp'] == 100101102 and \
                lttng.data.executor_entity_collector_to_executor.\
                df.iloc[0]['entities_collector_addr'] == HDL_ENTITIES

            assert lttng.data.executor_entity_collector_to_executor.df.index[1] == 1 and \
                lttng.data.executor_entity_collector_to_executor.\
                df.iloc[1]['timestamp'] == 100101261 and \
                lttng.data.executor_entity_collector_to_executor.\
                df.iloc[1]['entities_collector_addr'] == 1
        else:
            events = [
                # Initialization trace points
                {
                    '_name': 'ros2_caret:caret_init',
                    'clock_offset': 10,
                    'distribution': distribution,
                    '_timestamp': 100100901,
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
            ]

            lttng = Lttng(events, event_filters=[], validate=False)

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

    @pytest.mark.parametrize(
        'distribution',
        ['jazzy', 'iron'],
    )
    def test_duplicated_events_callback_groups(self, distribution):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_TIMER = 1000701
        HDL_SUBSCRIPTION = 1000401
        HDL_RMW_SUBSCRIPTION = 1000411
        HDL_SERVICE = 1000501
        HDL_RMW_SERVICE = 1000511
        HDL_CLIENT = 1000601
        HDL_RMW_CLIENT = 1000611
        SERVICE_CALLBACK = 1000561
        HDL_EXECUTOR = 1001101
        HDL_EXECUTOR_STATIC = 1001201
        HDL_ENTITIES = 1001211
        EXECUTOR_CALLBACK = 1001261
        EXECUTOR_STA_CALLBACK = 1001281
        VTID1 = 500001
        VPID1 = 600001

        if distribution[0] >= 'jazzy'[0]:
            events = [
                # Initialization trace points
                {
                    '_name': 'ros2_caret:caret_init',
                    'clock_offset': 10,
                    'distribution': distribution,
                    '_timestamp': 100100901,
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
                    '_name': 'ros2:rcl_subscription_init',
                    'subscription_handle': HDL_SUBSCRIPTION,
                    'node_handle': HDL_NODE,
                    'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                    '_timestamp': 100101202,
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
                    '_timestamp': 100102403,
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
                    '_timestamp': 100103404,
                    'topic_name': 'my_topic_name',
                    'queue_depth': 1,
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
                    '_timestamp': 100100212,
                    'service_name': 'my_service_name',
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
                    '_name': 'ros2:rclcpp_service_callback_added',
                    'service_handle': HDL_SERVICE,
                    'callback': SERVICE_CALLBACK,
                    '_timestamp': 100100205,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2:rclcpp_service_callback_added',
                    'service_handle': HDL_SERVICE,
                    'callback': SERVICE_CALLBACK,
                    '_timestamp': 100100221,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                # ################
                # JAZZY added>
                {
                    '_name': 'ros2_caret:callback_group_to_executor_entity_collector',
                    'entities_collector_addr': HDL_ENTITIES,
                    'callback_group_addr': EXECUTOR_CALLBACK,
                    'group_type_name': 'reentrant',
                    '_timestamp': 100101101,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:callback_group_to_executor_entity_collector',
                    'entities_collector_addr': HDL_ENTITIES,
                    'callback_group_addr': EXECUTOR_CALLBACK,
                    'group_type_name': 'mutually_exclusive',
                    '_timestamp': 100101260,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:executor_entity_collector_to_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    '_timestamp': 100101102,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:executor_entity_collector_to_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    '_timestamp': 100101261,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                # <JAZZY added
                # JAZZY changed>
                {
                    '_name': 'ros2_caret:construct_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101112,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:construct_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101213,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:construct_static_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101122,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:construct_static_executor',
                    'executor_addr': HDL_EXECUTOR,
                    'entities_collector_addr': HDL_ENTITIES,
                    'executor_type_name': 'my_executor_name',
                    '_timestamp': 100101203,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },

                # <JAZZY changed
                # JAZZY deleted>
                # {
                #    '_name': 'ros2_caret:add_callback_group',
                #    'executor_addr': HDL_EXECUTOR_STATIC,
                #    'callback_group_addr': EXECUTOR_CALLBACK,
                #    'group_type_name': 'my_group_name',
                #    '_timestamp': 100101102,
                #    '_vtid': VTID1,
                #    '_vpid': VPID1
                # },
                # {
                #    '_name': 'ros2_caret:add_callback_group',
                #    'executor_addr': HDL_EXECUTOR_STATIC,
                #    'callback_group_addr': EXECUTOR_CALLBACK,
                #    'group_type_name': 'my_group_name',
                #    '_timestamp': 100101261,
                #    '_vtid': VTID1,
                #    '_vpid': VPID1
                # },
                # {
                #    '_name': 'ros2_caret:add_callback_group_static_executor',
                #    'entities_collector_addr': HDL_ENTITIES,
                #    'callback_group_addr': EXECUTOR_STA_CALLBACK,
                #    'group_type_name': 'my_group_name',
                #    '_timestamp': 100101204,
                #    '_vtid': VTID1,
                #    '_vpid': VPID1
                # },
                # {
                #    '_name': 'ros2_caret:add_callback_group_static_executor',
                #    'entities_collector_addr': HDL_ENTITIES,
                #    'callback_group_addr': EXECUTOR_STA_CALLBACK,
                #    'group_type_name': 'my_group_name',
                #    '_timestamp': 100101281,
                #    '_vtid': VTID1,
                #    '_vpid': VPID1
                # },
                # <JAZZY deleted
                # ################

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
                    '_timestamp': 100102402,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:callback_group_add_service',
                    'service_handle': HDL_SERVICE,
                    'callback_group_addr': EXECUTOR_CALLBACK,
                    '_timestamp': 100100201,
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
                    '_timestamp': 100100201,
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
            ]
        else:
            events = [
                # Initialization trace points
                {
                    '_name': 'ros2_caret:caret_init',
                    'clock_offset': 10,
                    'distribution': distribution,
                    '_timestamp': 100100901,
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
                    '_name': 'ros2:rcl_subscription_init',
                    'subscription_handle': HDL_SUBSCRIPTION,
                    'node_handle': HDL_NODE,
                    'rmw_subscription_handle': HDL_RMW_SUBSCRIPTION,
                    '_timestamp': 100101202,
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
                    '_timestamp': 100102403,
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
                    '_timestamp': 100103404,
                    'topic_name': 'my_topic_name',
                    'queue_depth': 1,
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
                    '_timestamp': 100100212,
                    'service_name': 'my_service_name',
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
                    '_name': 'ros2:rclcpp_service_callback_added',
                    'service_handle': HDL_SERVICE,
                    'callback': SERVICE_CALLBACK,
                    '_timestamp': 100100205,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2:rclcpp_service_callback_added',
                    'service_handle': HDL_SERVICE,
                    'callback': SERVICE_CALLBACK,
                    '_timestamp': 100100221,
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
                    '_timestamp': 100102402,
                    '_vtid': VTID1,
                    '_vpid': VPID1
                },
                {
                    '_name': 'ros2_caret:callback_group_add_service',
                    'service_handle': HDL_SERVICE,
                    'callback_group_addr': EXECUTOR_CALLBACK,
                    '_timestamp': 100100201,
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
                    '_timestamp': 100100201,
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
            ]

        lttng = Lttng(events, event_filters=[], validate=False)

        if distribution[0] >= 'jazzy'[0]:
            assert lttng.data.callback_group_to_executor_entity_collector.\
                df.index[0] == HDL_ENTITIES and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[0]['group_type_name'] == 'reentrant' and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[0]['timestamp'] == 100101101 and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[0]['callback_group_addr'] == EXECUTOR_CALLBACK

            assert lttng.data.callback_group_to_executor_entity_collector.\
                df.index[1] == 1 and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[1]['group_type_name'] == 'mutually_exclusive' and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[1]['timestamp'] == 100101260 and \
                lttng.data.callback_group_to_executor_entity_collector.\
                df.iloc[1]['callback_group_addr'] == 1
        else:
            # jazzy does not have this trace point #
            # ['timestamp', 'executor_addr', 'group_type_name']
            assert lttng.data.callback_groups.df.index[0] == EXECUTOR_CALLBACK and \
                lttng.data.callback_groups.df.iloc[0]['timestamp'] == 100101102 and \
                lttng.data.callback_groups.df.iloc[0]['executor_addr'] == HDL_EXECUTOR_STATIC

            assert lttng.data.callback_groups.df.index[1] == 1 and \
                lttng.data.callback_groups.df.iloc[1]['timestamp'] == 100101261 and \
                lttng.data.callback_groups.df.iloc[1]['executor_addr'] == 3

            # ['timestamp', 'entities_collector_addr', 'group_type_name']
            assert lttng.data.callback_groups_static.df.index[0] == EXECUTOR_STA_CALLBACK and \
                lttng.data.callback_groups_static.df.iloc[0]['timestamp'] == 100101204 and \
                lttng.data.callback_groups_static.df.iloc[0]['entities_collector_addr'] == 1

            assert lttng.data.callback_groups_static.df.index[1] == 2 and \
                lttng.data.callback_groups_static.df.iloc[1]['timestamp'] == 100101281 and \
                lttng.data.callback_groups_static.df.iloc[1]['entities_collector_addr'] == 2

        # ['timestamp', 'timer_handle']
        assert lttng.data.callback_group_timer.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_timer.df.iloc[0]['timestamp'] == 100100702 and \
            lttng.data.callback_group_timer.df.iloc[0]['timer_handle'] == HDL_TIMER

        assert lttng.data.callback_group_timer.df.index[1] == 1 and \
            lttng.data.callback_group_timer.df.iloc[1]['timestamp'] == 100101302 and \
            lttng.data.callback_group_timer.df.iloc[1]['timer_handle'] == 2

        # callback_group_subscription
        # ['timestamp', 'subscription_handle']
        assert lttng.data.callback_group_subscription.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_subscription.df.iloc[0]['timestamp'] == 100100401 and \
            lttng.data.callback_group_subscription.df.iloc[0]['subscription_handle'] \
            == HDL_SUBSCRIPTION

        assert lttng.data.callback_group_subscription.df.index[1] == 1 and \
            lttng.data.callback_group_subscription.df.iloc[1]['timestamp'] == 100102402 and \
            lttng.data.callback_group_subscription.df.iloc[1]['subscription_handle'] == 1

        # callback_group_service
        # ['timestamp', 'service_handle']
        assert lttng.data.callback_group_service.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_service.df.iloc[0]['timestamp'] == 100100201 and \
            lttng.data.callback_group_service.df.iloc[0]['service_handle'] == HDL_SERVICE

        assert lttng.data.callback_group_service.df.index[1] == 1 and \
            lttng.data.callback_group_service.df.iloc[1]['timestamp'] == 100101502 and \
            lttng.data.callback_group_service.df.iloc[1]['service_handle'] == 1

        # callback_group_client
        # ['timestamp', 'client_handle']
        assert lttng.data.callback_group_client.df.index[0] == EXECUTOR_CALLBACK and \
            lttng.data.callback_group_client.df.iloc[0]['timestamp'] == 100100201 and \
            lttng.data.callback_group_client.df.iloc[0]['client_handle'] == HDL_CLIENT

        assert lttng.data.callback_group_client.df.index[1] == 1 and \
            lttng.data.callback_group_client.df.iloc[1]['timestamp'] == 100101602 and \
            lttng.data.callback_group_client.df.iloc[1]['client_handle'] == 1

    def test_duplicated_events_buffer(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_SUBSCRIPTION = 1000401
        HDL_RMW_SUBSCRIPTION = 1000411
        SUBSCRIPTION = 1000451
        RING_BUFFER = 1001701
        BUFFER_JPB = 1001801
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Initialization trace points
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
                '_timestamp': 100101705,
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
        ]

        lttng = Lttng(events, event_filters=[], validate=False)

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
        # ['timestamp', 'ipb', 'subscription']
        assert lttng.data.ipb_to_subscriptions.df.index[0] == BUFFER_JPB and \
            lttng.data.ipb_to_subscriptions.df.iloc[0]['timestamp'] == 100101705 and \
            lttng.data.ipb_to_subscriptions.df.iloc[0]['subscription'] == 1

        assert lttng.data.ipb_to_subscriptions.df.index[1] == 1 and \
            lttng.data.ipb_to_subscriptions.df.iloc[1]['timestamp'] == 100101902 and \
            lttng.data.ipb_to_subscriptions.df.iloc[1]['subscription'] == 1

    def test_duplicated_events_runtime(self):
        HDL_NODE = 1000201
        HDL_RMW = 1000211
        HDL_PUBLISHER = 1000301
        HDL_RMW_PUBLISHER = 1000311
        HDL_SUBSCRIPTION = 1000401
        HDL_RMW_SUBSCRIPTION = 1000411
        SUBSCRIPTION = 1000451
        SUBSCRIPTION_CALLBACK = 1000461
        TIMER_CALLBACK = 1000661
        HDL_TIMER = 1000701
        STATE_MACHINE = 1000801
        RING_BUFFER = 1001701
        VTID1 = 500001
        VPID1 = 600001

        events = [
            # Runtime trace events
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
                '_name': 'ros2:rclcpp_subscription_callback_added',
                'subscription': SUBSCRIPTION,
                'callback': SUBSCRIPTION_CALLBACK,
                '_timestamp': 100100405,
                # '_timestamp': 100100450,
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
                'callback': TIMER_CALLBACK,
                '_timestamp': 100100501,
                '_vtid': VTID1,
                '_vpid': VPID1
            },
            {
                '_name': 'ros2:callback_end',
                'callback': TIMER_CALLBACK,
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

        # callback_start_instances
        # ['tid', 'callback_start_timestamp', 'callback_object', 'is_intra_process']
        assert lttng.data.callback_start_instances.\
            data[0].data['callback_object'] == TIMER_CALLBACK and \
            lttng.data.callback_start_instances.\
            data[0].data['callback_start_timestamp'] == 100100750 and \
            lttng.data.callback_start_instances.data[0].data['is_intra_process'] == 100

        # Note: 1 are registered in callback_object of SUBSCRIPTION
        #   so it becomes 2.
        assert lttng.data.callback_start_instances.data[1].data['callback_object'] == 2 and \
            lttng.data.callback_start_instances.\
            data[1].data['callback_start_timestamp'] == 100102002 and \
            lttng.data.callback_start_instances.data[1].data['is_intra_process'] == 101

        # callback_end_instances
        # ['tid', 'callback_end_timestamp', 'callback_object']
        assert lttng.data.callback_end_instances.\
            data[0].data['callback_object'] == TIMER_CALLBACK and \
            lttng.data.callback_end_instances.\
            data[0].data['callback_end_timestamp'] == 100100501

        # Note: 1 are registered in callback_object of SUBSCRIPTION
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


@pytest.fixture
def set_pickle_collection_time_range(mocker):
    def _set_pickle_collection_time_range(time_min: int, time_max: int):
        pickle_collection_mock = mocker.Mock(spec=IterableEvents)
        mocker.patch('caret_analyze.infra.lttng.lttng.PickleEventCollection',
                     return_value=pickle_collection_mock)

        mocker.patch.object(
            pickle_collection_mock, 'time_range', return_value=(time_min, time_max))
    return _set_pickle_collection_time_range


@pytest.fixture
def set_ctf_collection_time_range(mocker):
    def _set_ctf_collection_time_range(time_min: int, time_max: int):
        ctf_collection_mock = mocker.Mock(spec=IterableEvents)
        mocker.patch('caret_analyze.infra.lttng.lttng.CtfEventCollection',
                     return_value=ctf_collection_mock)

        mocker.patch.object(
            ctf_collection_mock, 'time_range', return_value=(time_min, time_max))
    return _set_ctf_collection_time_range


@pytest.fixture
def set_trace_dir_exists(mocker):
    def _set_trace_dir_exists(exists: bool):
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCollection._trace_dir_exists',
                     return_value=exists)
    return _set_trace_dir_exists


@pytest.fixture
def set_cache_exists(mocker):
    def _set_cache_exists(exists: bool):
        mocker.patch('caret_analyze.infra.lttng.lttng.EventCollection._cache_exists',
                     return_value=exists)
    return _set_cache_exists


class TestEventCollection:

    def test_force_conversion_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', True, store_cache=False)
        assert 'Converted to' in caplog.messages[0]

    def test_cache_not_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(False)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', True, store_cache=False)
        assert 'Converted to' in caplog.messages[0]

    def test_valid_cache_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(0, 1)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', False, store_cache=False)
        assert 'Found converted file' in caplog.messages[0]

    def test_invalid_cache_exists_case(
        self,
        caplog,
        set_pickle_collection_time_range,
        set_ctf_collection_time_range,
        set_trace_dir_exists,
        set_cache_exists
    ):
        set_trace_dir_exists(True)
        set_cache_exists(True)
        set_pickle_collection_time_range(1, 2)
        set_ctf_collection_time_range(0, 1)

        EventCollection('', False, store_cache=False)
        assert 'Converted to' in caplog.messages[0]


class TestMultiHostIdRemapper:

    def test_remap(self):
        original_event = {'_vtid': 1000}
        event = original_event.copy()
        id_remapper = MultiHostIdRemapper('_vtid')

        id_remapper.remap(event)
        assert original_event == event
        id_remapper.remap(event)
        assert original_event == event  # Duplicate ids are not converted within the same host

        id_remapper.change_host()
        id_remapper.remap(event)
        assert original_event != event

    def test_change_host(self):
        original_event = {'_vtid': 1000}
        event = original_event.copy()
        id_remapper = MultiHostIdRemapper('_vtid')

        id_remapper.remap(event)

        assert len(id_remapper._other_host_ids) == 0
        assert len(id_remapper._current_host_remapped_ids) == 0
        assert len(id_remapper._current_host_not_remapped_ids) == 1
        assert len(id_remapper._current_host_id_map) == 0

        id_remapper.change_host()

        assert len(id_remapper._other_host_ids) == 1
        assert len(id_remapper._current_host_remapped_ids) == 0
        assert len(id_remapper._current_host_not_remapped_ids) == 0
        assert len(id_remapper._current_host_id_map) == 0

    def test_single_host_case(self):
        host1_events = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]
        id_remapper = MultiHostIdRemapper('_vtid')
        for event in host1_events:
            id_remapper.remap(event)

        expect = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]

        assert host1_events == expect

    def test_two_host_case_without_duplicated_ids(self):
        host1_events = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]

        host2_events = [
            {'_vtid': 1003},
            {'_vtid': 1004},
            {'_vtid': 1005},
        ]

        id_remapper = MultiHostIdRemapper('_vtid')
        for event in host1_events:
            id_remapper.remap(event)
        id_remapper.change_host()
        for event in host2_events:
            id_remapper.remap(event)

        remapped_events = []
        remapped_events.extend(host1_events)
        remapped_events.extend(host2_events)

        expect = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
            {'_vtid': 1003},
            {'_vtid': 1004},
            {'_vtid': 1005},
        ]

        assert remapped_events == expect

    def test_two_host_case_with_duplicated_ids(self):
        host1_events = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]

        host2_events = [
            {'_vtid': 1003},
            {'_vtid': 1001},  # duplicated id
            {'_vtid': 1002},  # duplicated id
        ]

        id_remapper = MultiHostIdRemapper('_vtid')
        for event in host1_events:
            id_remapper.remap(event)
        id_remapper.change_host()
        for event in host2_events:
            id_remapper.remap(event)

        remapped_events = []
        remapped_events.extend(host1_events)
        remapped_events.extend(host2_events)

        expect = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
            {'_vtid': 1003},
            {'_vtid': 1000000000},  # host2 _vtid: 1001
            {'_vtid': 1000000001},  # host2 _vtid: 1002
        ]

        assert remapped_events == expect

    def test_two_host_case_with_duplicated_remapped_ids(self):
        host1_events = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]

        host2_events = [
            {'_vtid': 1003},
            {'_vtid': 1001},  # duplicated id
            {'_vtid': 1000000000},        # duplicated id
        ]

        id_remapper = MultiHostIdRemapper('_vtid')
        for event in host1_events:
            id_remapper.remap(event)
        id_remapper.change_host()
        for event in host2_events:
            id_remapper.remap(event)

        remapped_events = []
        remapped_events.extend(host1_events)
        remapped_events.extend(host2_events)

        expect = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
            {'_vtid': 1003},
            {'_vtid': 1000000000},  # host2 _vtid: 1001
            {'_vtid': 1000000001},  # host2 _vtid: 1000000000
        ]

        assert remapped_events == expect

    def test_three_host_case_with_duplicated_ids(self):
        host1_events = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
        ]

        host2_events = [
            {'_vtid': 1003},
            {'_vtid': 1001},        # duplicated id
            {'_vtid': 1000000000},  # duplicated id
        ]

        host3_events = [
            {'_vtid': 1003},  # duplicated id
            {'_vtid': 1004},
            {'_vtid': 1005},
        ]

        id_remapper = MultiHostIdRemapper('_vtid')
        for event in host1_events:
            id_remapper.remap(event)
        id_remapper.change_host()
        for event in host2_events:
            id_remapper.remap(event)
        id_remapper.change_host()
        for event in host3_events:
            id_remapper.remap(event)

        remapped_events = []
        remapped_events.extend(host1_events)
        remapped_events.extend(host2_events)
        remapped_events.extend(host3_events)

        expect = [
            {'_vtid': 1000},
            {'_vtid': 1001},
            {'_vtid': 1002},
            {'_vtid': 1003},
            {'_vtid': 1000000000},  # host2 _vtid: 1001
            {'_vtid': 1000000001},  # host2 _vtid: 1000000000
            {'_vtid': 1000000002},  # host3 _vtid: 1003
            {'_vtid': 1004},
            {'_vtid': 1005},
        ]

        assert remapped_events == expect
