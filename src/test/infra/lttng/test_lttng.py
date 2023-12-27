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
from caret_analyze.infra.lttng.lttng import EventCollection, IterableEvents
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
