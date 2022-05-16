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
from caret_analyze.infra.lttng.event_counter import EventCounter
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.infra.lttng.records_source import RecordsSource
from caret_analyze.infra.lttng.ros2_tracing.data_model import DataModel
from caret_analyze.infra.lttng.value_objects import (PublisherValueLttng,
                                                     SubscriptionCallbackValueLttng,
                                                     TimerCallbackValueLttng)
from caret_analyze.record.interface import RecordsInterface
from caret_analyze.value_objects import ExecutorValue
from caret_analyze.value_objects.node import NodeValue

from pytest_mock import MockerFixture


class TestLttng:

    def test_get_nodes(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        node = NodeValue('/node', None)
        mocker.patch.object(lttng_info_mock, 'get_nodes', return_value=[node])
        nodes = lttng.get_nodes()
        assert nodes == [node]

    def test_get_rmw_implementation(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)

        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        mocker.patch.object(lttng_info_mock, 'get_rmw_impl', return_value='rmw')
        assert lttng.get_rmw_impl() == 'rmw'

    def test_get_publishers(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        pub_mock = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_publishers', return_value=[pub_mock])
        assert lttng.get_publishers(NodeValue('node', None)) == [pub_mock]

    def test_get_timer_callbacks(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)
        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_timer_callbacks',
                            return_value=[timer_cb_mock])
        assert lttng.get_timer_callbacks(NodeValue('node', None)) == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        sub_cb_mock = mocker.Mock(spec=SubscriptionCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_subscription_callbacks',
                            return_value=[sub_cb_mock])
        assert lttng.get_subscription_callbacks(NodeValue('node', None)) == [sub_cb_mock]

    def test_get_executors(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        exec_info_mock = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_info_mock, 'get_executors',
                            return_value=[exec_info_mock])
        assert lttng.get_executors() == [exec_info_mock]

    def test_compose_inter_proc_comm_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'inter_proc_comm_records',
                            records_mock)
        assert lttng.compose_inter_proc_comm_records() == records_mock

    def test_compose_intra_proc_comm_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'intra_proc_comm_records',
                            records_mock)
        assert lttng.compose_intra_proc_comm_records() == records_mock

    def test_compose_callback_records(self, mocker: MockerFixture):
        data_mock = mocker.Mock(spec=DataModel)
        mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data_mock, {}))

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                     return_value=lttng_info_mock)

        records_source_mock = mocker.Mock(spec=RecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                     return_value=records_source_mock)
        counter_mock = mocker.Mock(spec=EventCounter)
        mocker.patch('caret_analyze.infra.lttng.event_counter.EventCounter',
                     return_value=counter_mock)

        lttng = Lttng('trace_dir', force_conversion=False, use_singleton_cache=False)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'clone', return_value=records_mock)
        mocker.patch.object(records_source_mock, 'callback_records',
                            records_mock)
        assert lttng.compose_callback_records() == records_mock
