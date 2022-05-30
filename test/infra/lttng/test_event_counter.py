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


from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.event_counter import CategoryServer, EventCounter, UNKNOWN
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import NodeValueLttng

import pandas as pd
import pytest


@pytest.fixture
def lttng_mock(mocker):
    lttng = mocker.Mock(spec=Lttng)
    mocker.patch.object(lttng, 'get_nodes', return_value=[])
    mocker.patch.object(lttng, 'get_callback_groups', return_value=[])
    mocker.patch.object(lttng, 'get_client_callbacks', return_value=[])
    mocker.patch.object(lttng, 'get_publishers', return_value=[])
    mocker.patch.object(lttng, 'get_subscription_callbacks', return_value=[])
    mocker.patch.object(lttng, 'get_service_callbacks', return_value=[])
    mocker.patch.object(lttng, 'get_timer_callbacks', return_value=[])
    mocker.patch.object(lttng, 'get_tf_broadcaster', return_value=None)
    mocker.patch.object(lttng, 'get_tf_buffer', return_value=None)
    return lttng


class TestEventCounter:

    def test_empty(self, lttng_mock):
        data = Ros2DataModel()
        data.finalize()
        counter = EventCounter(data, lttng_mock)
        count = counter.get_count()
        assert len(count) == 0

    def test_invalid_argument(self, lttng_mock):
        data = Ros2DataModel()
        data.finalize()
        counter = EventCounter(data, lttng_mock)
        with pytest.raises(InvalidArgumentError):
            counter.get_count(['invalid'])

    def test_count(self, lttng_mock):
        data = Ros2DataModel()
        data.add_callback_start(0, 0, 0, 0, False)
        data.add_sim_time(0, 0, 0, 0)
        data.finalize()
        counter = EventCounter(data, lttng_mock)

        df = counter.get_count()
        df_expect = pd.DataFrame(
            {
                'trace_point': ['ros2:callback_start', 'ros2_caret:sim_time'],
                'node_name': [UNKNOWN, UNKNOWN],
                'topic_name': [UNKNOWN, UNKNOWN],
                'size': [1, 1],
            }, columns=['trace_point', 'node_name', 'topic_name', 'size']
        )
        assert df.equals(df_expect)

        df = counter.get_count(['trace_point'])
        df_expect = pd.DataFrame(
            {
                'trace_point': ['ros2:callback_start', 'ros2_caret:sim_time'],
                'size': [1, 1],
            }, columns=['trace_point', 'size']
        )
        assert df.equals(df_expect)


class TestCategoryServer:

    def test_empty(self, lttng_mock):
        server = CategoryServer(lttng_mock)
        category = server.get(0, 0)
        assert category.node_name == UNKNOWN
        assert category.topic_name == UNKNOWN
        assert category.trace_point == UNKNOWN

    @pytest.fixture
    def create_node_mock(self, mocker):
        def _create(pid, node_handle, node_name):
            node_mock = mocker.Mock(spec=NodeValueLttng)
            mocker.patch.object(node_mock, 'node_name', node_name)
            mocker.patch.object(node_mock, 'node_handle', node_handle)
            mocker.patch.object(node_mock, 'pid', pid)
            return node_mock
        return _create

    def test_node(self, mocker, lttng_mock, create_node_mock):
        node_mock = create_node_mock(1, 2, 'node_name')
        mocker.patch.object(lttng_mock, 'get_nodes', return_value=[node_mock])
        server = CategoryServer(lttng_mock)
        category = server.get(1, 2)
        assert category.node_name == 'node_name'
        assert category.topic_name == UNKNOWN
        assert category.trace_point == UNKNOWN
