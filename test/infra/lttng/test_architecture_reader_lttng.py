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
from caret_analyze.infra.lttng.architecture_reader_lttng import ArchitectureReaderLttng
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (
    IntraProcessBufferValueLttng,
    NodeValueLttng,
    PublisherValueLttng,
    ServiceCallbackValueLttng,
    SubscriptionValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from caret_analyze.value_objects import (
    BroadcastedTransformValue,
    CallbackGroupValue,
    ClientCallbackValue,
    ExecutorValue,
    PublisherValue,
    SubscriptionCallbackValue,
    TimerCallbackValue,
    TimerValue,
)

import pytest


class TestArchitectureReaderLttng:

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
    def create_lttng(self, mocker):
        def _create(lttng_info):
            data = Ros2DataModel()
            data.finalize()
            mocker.patch.object(Lttng, '_parse_lttng_data', return_value=(data, {}))
            mocker.patch('caret_analyze.infra.lttng.lttng_info.LttngInfo',
                         return_value=lttng_info)
            lttng = Lttng('')
            return lttng
        return _create

    def test_get_nodes(self, mocker, create_lttng, lttng_info_mock):
        node_mock = mocker.Mock(spec=NodeValueLttng)
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_nodes() == []

        mocker.patch.object(lttng_info_mock, 'get_nodes', return_value=[node_mock])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_nodes() == [node_mock]

    def test_get_publishers(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_publishers('/node') == []

        pub_mock = mocker.Mock(spec=PublisherValue)
        mocker.patch.object(lttng, 'get_publishers', return_value=[pub_mock])

        expect = mocker.Mock(PublisherValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_publishers', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_publishers('/node') == [pub_mock]

    def test_get_timer_callbacks(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_timer_callbacks('/node') == []

        timer_cb_mock = mocker.Mock(spec=TimerCallbackValue)
        mocker.patch.object(lttng_info_mock, 'get_timer_callbacks', return_value=[timer_cb_mock])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_timer_callbacks('/node') == [timer_cb_mock]

    def test_get_subscription_callbacks(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_subscription_callbacks('') == []

        expect = mocker.Mock(spec=SubscriptionCallbackValue)
        mocker.patch.object(lttng_info_mock, 'get_subscription_callbacks', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_subscription_callbacks('') == [expect]

    def test_get_executors(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_executors() == []

        expect = mocker.Mock(spec=ExecutorValue)
        mocker.patch.object(lttng_info_mock, 'get_executors', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_executors() == [expect]

    def test_get_variable_passings(self, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_variable_passings('') == []

    def test_get_named_paths(self, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_paths() == []

    def test_get_subscriptions(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_subscription_callbacks('') == []

        expect = mocker.Mock(SubscriptionValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_subscriptions', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        subs = reader.get_subscriptions('')
        assert subs == [expect]

    def test_get_callback_groups(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_callback_groups('') == []

        expect = mocker.Mock(spec=CallbackGroupValue)
        mocker.patch.object(lttng_info_mock, 'get_callback_groups', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        cbgs = reader.get_callback_groups('')
        assert cbgs == [expect]

    def test_get_timers(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_timers('') == []

        expect = mocker.Mock(TimerValue)
        mocker.patch.object(lttng_info_mock, 'get_timers', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_timers('')
        assert values == [expect]

    def test_get_tf_frames(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_tf_frames() == []

        expect = BroadcastedTransformValue(
            'frame_id', 'child_frame_id'
        )
        mocker.patch.object(lttng_info_mock, 'get_tf_frames', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_tf_frames()
        assert values == [expect]

    def test_get_ipc_buffers(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_ipc_buffers('') == []

        expect = mocker.Mock(IntraProcessBufferValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_ipc_buffers', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_ipc_buffers('')
        assert values == [expect]

    def test_tf_broadcaster(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_tf_broadcaster('') is None

        expect = mocker.Mock(TransformBroadcasterValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_tf_broadcaster', return_value=expect)
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_tf_broadcaster('')
        assert values == expect

    def test_tf_buffer(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_tf_buffer('') is None

        expect = mocker.Mock(TransformBufferValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_tf_buffer', return_value=expect)
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_tf_buffer('')
        assert values == expect

    def test_client_callbacks(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_tf_buffer('') is None

        expect = mocker.Mock(ClientCallbackValue)
        mocker.patch.object(lttng_info_mock, 'get_client_callbacks', return_value=expect)
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_client_callbacks('')
        assert values == expect

    def test_service_callbacks(self, mocker, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_tf_buffer('') is None

        expect = mocker.Mock(ServiceCallbackValueLttng)
        mocker.patch.object(lttng_info_mock, 'get_service_callbacks', return_value=[expect])
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        values = reader.get_service_callbacks('')
        assert values == [expect]

    def test_message_contexts(self, create_lttng, lttng_info_mock):
        lttng = create_lttng(lttng_info_mock)
        reader = ArchitectureReaderLttng(lttng)
        assert reader.get_message_contexts('') == []
