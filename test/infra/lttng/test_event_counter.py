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


from logging import getLogger

from caret_analyze.exceptions import InvalidTraceFormatError
from caret_analyze.infra.lttng.event_counter import EventCounter
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.ros2_tracing.processor import Ros2Handler

import pytest
from pytest_mock import MockerFixture


class TestEventCounter:

    def test_build_count_df_empty_records(self, mocker: MockerFixture):
        data = Ros2DataModel()
        data.finalize()

        df = EventCounter._build_count_df(data)
        assert set(df.columns) == {'size', 'node_name', 'trace_point', 'topic_name'}
        assert set(df['trace_point'].values) == set(Ros2Handler.get_trace_points())
        assert list(df['size']) == [0] * len(df)

    def test_build_count_df_increment_count(self, mocker: MockerFixture):
        data = Ros2DataModel()
        data.add_context(0, 0, 0, 0)
        data.add_node(0, 0, 0, 0, 'name', '/')
        data.add_publisher(0, 0, 0, 0, 0, 0)
        data.add_rcl_subscription(0, 0, 0, 0, 0, 0)
        data.add_rclcpp_subscription(0, 0, 0)
        data.add_service(0, 0, 0, 0, 0)
        data.add_client(0, 0, 0, 0, 0)
        data.add_timer(0, 0, 0, 0)
        data.add_tilde_subscribe_added(0, 0, 0, 0)
        data.add_timer_node_link(0, 0, 0)
        data.add_callback_object(0, 0, 0)
        data.add_callback_symbol(0, 0, 0)
        data.add_lifecycle_state_machine(0, 0)
        data.add_lifecycle_state_transition(0, 0, 0, 0)
        data.add_tilde_subscription(0, 0, 0, 0)
        data.add_tilde_publisher(0, 0, 0, 0)
        data.add_callback_start_instance(0, 0, False)
        data.add_callback_end_instance(0, 0)
        data.add_rclcpp_intra_publish_instance(0, 0, 0, 0)
        data.add_rclcpp_publish_instance(0, 0, 0, 0)
        data.add_rcl_publish_instance(0, 0, 0)
        data.add_dds_write_instance(0, 0)
        data.add_dds_bind_addr_to_addr(0, 0, 0)
        data.add_dds_bind_addr_to_stamp(0, 0, 0)
        data.add_on_data_available_instance(0, 0)
        data.add_message_construct_instance(0, 0, 0)
        data.add_dispatch_subscription_callback_instance(0, 0, 0, 0, 0)
        data.add_sim_time(0, 0)
        data.add_rmw_implementation('')
        data.add_dispatch_intra_process_subscription_callback_instance(0, 0, 0, 0)
        data.add_tilde_subscribe(0, 0, 0)
        data.add_tilde_publish(0, 0, 0, 0)
        data.add_executor(0, 0, '')
        data.add_executor_static(0, 0, 0, '')
        data.add_callback_group(0, 0, 0, '')
        data.add_callback_group_static_executor(0, 0, 0, '')
        data.callback_group_add_timer(0, 0, 0)
        data.callback_group_add_subscription(0, 0, 0)
        data.callback_group_add_service(0, 0, 0)
        data.callback_group_add_client(0, 0, 0)

        data.finalize()

        df = EventCounter._build_count_df(data)
        assert list(df['size']) == [1] * len(df)

    def test_validation_without_ld_preload(
        self,
        mocker: MockerFixture,
    ):
        data = Ros2DataModel()
        data.add_dds_write_instance(0, 0)
        data.finalize()

        logger = getLogger('caret_analyze.infra.lttng.event_counter')
        logger.propagate = True

        with pytest.raises(InvalidTraceFormatError):
            EventCounter(data)

    def test_validation_without_forked_rclcpp(
        self,
        mocker: MockerFixture,
    ):
        data = Ros2DataModel()
        data.add_dispatch_subscription_callback_instance(0, 0, 0, 0, 0)
        data.finalize()

        logger = getLogger('caret_analyze.infra.lttng.event_counter')
        logger.propagate = True

        with pytest.raises(InvalidTraceFormatError):
            EventCounter(data)

    def test_validation_valid_case(
        self,
        mocker: MockerFixture,
    ):
        data = Ros2DataModel()
        data.add_dispatch_subscription_callback_instance(0, 0, 0, 0, 0)
        data.add_dds_write_instance(0, 0)
        data.finalize()

        logger = getLogger('caret_analyze.infra.lttng.event_counter')
        logger.propagate = True

        EventCounter(data)
