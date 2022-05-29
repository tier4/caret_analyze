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


from caret_analyze.common import ClockConverter
from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.bridge import LttngBridge
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.infra.lttng.records_provider_lttng import RecordsProviderLttng
from caret_analyze.infra.lttng.records_source import RecordsSource
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import PublisherValueLttng
from caret_analyze.record import (
    ColumnValue,
    RecordFactory,
    RecordsFactory,
    RecordsInterface,
)
from caret_analyze.value_objects import (
    CallbackStructValue,
    CommunicationStructValue,
    IntraProcessBufferStructValue,
    NodePathStructValue,
    PublisherStructValue,
    Qos,
    SubscriptionStructValue,
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
)

import pytest


class TestRecordsProviderLttng:

    @pytest.fixture
    def lttng_info_mock(self, mocker):

        lttng_info_mock = mocker.Mock(spec=LttngInfo)
        mocker.patch.object(lttng_info_mock, 'get_callback_groups', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_client_callbacks', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_executors', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_ipc_buffers', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_nodes', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_publisher_qos', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_publishers', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_rmw_impl', return_value=None)
        mocker.patch.object(lttng_info_mock, 'get_service_callbacks', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_subscription_qos', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_subscriptions', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_tf_broadcaster', return_value=None)
        mocker.patch.object(lttng_info_mock, 'get_tf_buffer', return_value=None)
        mocker.patch.object(lttng_info_mock, 'get_tf_frames', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(lttng_info_mock, 'get_timers', return_value=[])
        return lttng_info_mock

    @pytest.fixture
    def source_mock(self, mocker):
        source = mocker.Mock(spec=RecordsSource)
        return source

    @pytest.fixture
    def bridge_mock(self, mocker):
        bridge = mocker.Mock(spec=LttngBridge)
        return bridge

    @pytest.fixture
    def create_lttng(self, mocker):
        def _create(lttng_info, source, bridge):
            data = Ros2DataModel()
            data.finalize()
            mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data, {}))
            mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                         return_value=lttng_info)
            mocker.patch('caret_analyze.infra.lttng.records_source.RecordsSource',
                         return_value=source)
            mocker.patch('caret_analyze.infra.lttng.bridge.LttngBridge',
                         return_value=bridge)
            lttng = Lttng('')
            return lttng
        return _create

    def test_callback_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        expect = mocker.Mock(RecordsInterface)
        mocker.patch.object(source_mock, 'callback_records', return_value=expect)
        callback_mock = mocker.Mock(spec=CallbackStructValue)
        provider = RecordsProviderLttng(lttng)
        records = provider.callback_records(callback_mock)

        assert records == expect

    def test_node_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        expect = mocker.Mock(RecordsInterface)
        mocker.patch.object(source_mock, 'get_node_records', return_value=expect)
        node_mock = mocker.Mock(spec=NodePathStructValue)
        provider = RecordsProviderLttng(lttng)
        records = provider.node_records(node_mock)

        assert records == expect

    def test_timer_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        expect = mocker.Mock(RecordsInterface)
        mocker.patch.object(source_mock, 'get_timer_records', return_value=expect)
        node_mock = mocker.Mock(spec=NodePathStructValue)
        provider = RecordsProviderLttng(lttng)
        records = provider.timer_records(node_mock)

        assert records == expect

    def test_subscribe_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        expect = mocker.Mock(RecordsInterface)
        mocker.patch.object(source_mock, 'subscribe_records', return_value=expect)
        node_mock = mocker.Mock(spec=SubscriptionStructValue)
        provider = RecordsProviderLttng(lttng)
        records = provider.subscribe_records(node_mock)

        assert records == expect

    def test_publish_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        expect = mocker.Mock(RecordsInterface)
        mocker.patch.object(source_mock, 'publish_records', return_value=expect)
        node_mock = mocker.Mock(spec=PublisherStructValue)
        provider = RecordsProviderLttng(lttng)
        records = provider.publish_records(node_mock)

        assert records == expect

    def test_get_rmw_impl(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        mocker.patch.object(lttng_info_mock, 'get_rmw_impl', return_value='rmw')
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)
        assert provider.get_rmw_implementation() == 'rmw'

    def test_intra_proc_comm_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'intra_proc_comm_records', return_value=expect)
        mocker.patch.object(lttng_info_mock, 'is_intra_process_communication', return_value=True)

        comm_mock = mocker.Mock(CommunicationStructValue)
        records = provider.communication_records(comm_mock)
        assert records == expect

    def test_inter_proc_comm_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'inter_proc_comm_records', return_value=expect)
        mocker.patch.object(lttng_info_mock, 'is_intra_process_communication', return_value=False)

        comm_mock = mocker.Mock(CommunicationStructValue)
        records = provider.communication_records(comm_mock)
        assert records == expect

    def test_ipc_buffer_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'ipc_buffer_records', return_value=expect)

        comm_mock = mocker.Mock(IntraProcessBufferStructValue)
        records = provider.ipc_buffer_records(comm_mock)
        assert records == expect

    def test_tf_broadcast_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'send_transform_records', return_value=expect)

        comm_mock = mocker.Mock(TransformFrameBroadcasterStructValue)
        records = provider.tf_broadcast_records(comm_mock)
        assert records == expect

    def test_tf_communication_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'get_inter_proc_tf_comm_records', return_value=expect)

        comm_mock = mocker.Mock(TransformCommunicationStructValue)
        records = provider.tf_communication_records(comm_mock)
        assert records == expect

    def test_tf_lookup_records(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(source_mock, 'lookup_transform_records', return_value=expect)

        mock = mocker.Mock(TransformFrameBufferStructValue)
        records = provider.tf_lookup_records(mock)
        assert records == expect

    def test_qos(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        expect = mocker.Mock(spec=Qos)
        mocker.patch.object(lttng_info_mock, 'get_subscription_qos', return_value=expect)
        mocker.patch.object(lttng_info_mock, 'get_publisher_qos', return_value=expect)

        publisher_lttng = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(bridge_mock, 'get_publishers', return_value=[publisher_lttng])

        mock = mocker.Mock(SubscriptionStructValue)
        records = provider.get_qos(mock)
        assert records == expect

        mock = mocker.Mock(PublisherStructValue)
        records = provider.get_qos(mock)
        assert records == expect

    def test_sim_time_converter(
        self,
        mocker,
        create_lttng,
        lttng_info_mock,
        source_mock,
        bridge_mock,
    ):
        lttng = create_lttng(lttng_info_mock, source_mock, bridge_mock)
        provider = RecordsProviderLttng(lttng)

        records = RecordsFactory.create_instance(
            [
                RecordFactory.create_instance({'system_time': 0, 'sim_time': 0}),
                RecordFactory.create_instance({'system_time': 1, 'sim_time': 1})
            ], [
                ColumnValue('system_time'),
                ColumnValue('sim_time'),
            ]
        )
        mocker.patch.object(source_mock, 'system_and_sim_times', records)

        converter = provider.get_sim_time_converter()
        assert isinstance(converter, ClockConverter)
