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


from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel


class TestLttngInfo:

    def test_add_tracepoints(self):
        data = Ros2DataModel()
        data.add_rcl_init(0, 0, 0, 0, 'version')
        data.add_rcl_node_init(0, 0, 0, 0, 0, 'name', 'ns')
        data.add_rcl_publisher_init(0, 0, 0, 0, 0, 0, 'topic_name', 0)
        data.add_rcl_subscription_init(0, 0, 0, 0, 0, 0, 'topic_name', 0)
        data.add_rclcpp_subscription_init(0, 0, 0, 0, 0)
        data.add_rcl_service_init(0, 0, 0, 0, 0, 0, 'service_name')
        data.add_rcl_client_init(0, 0, 0, 0, 0, 0, 'service_name')
        data.add_rcl_timer_init(0, 0, 0, 0, 0)
        data.add_rclcpp_timer_link_node(0, 0, 0, 0, 0)
        data.add_rclcpp_callback_register(0, 0, 0, 0, 'symbol')
        data.add_callback_start(0, 0, 0, 0, True)
        data.add_callback_end(0, 0, 0, 0)
        data.add_rclcpp_intra_publish(0, 0, 0, 0, 0, 0)
        data.add_rclcpp_publish(0, 0, 0, 0, 0, 0)
        data.add_rcl_publish(0, 0, 0, 0, 0)
        data.add_dds_write(0, 0, 0, 0)
        data.add_dds_bind_addr_to_addr(0, 0, 0, 0, 0)
        data.add_message_construct(0, 0, 0, 0, 0)
        data.add_dispatch_subscription_callback(0, 0, 0, 0, 0, 0, 0)
        data.add_dispatch_intra_process_subscription_callback(0, 0, 0, 0, 0, 0)
        data.add_construct_executor(0, 0, 0, 0, 'executor_type')
        data.add_construct_static_executor(0, 0, 0, 0, 0, 'executor_type')
        data.add_add_callback_group(0, 0, 0, 0, 0, 'callback_group_type')
        data.add_add_callback_group_static_executor(0, 0, 0, 0, 0, 'callback_group_type')
        data.add_callback_group_add_timer(0, 0, 0, 0, 0)
        data.add_callback_group_add_subscription(0, 0, 1, 0, 0)
        data.add_callback_group_add_service(0, 0, 0, 0, 0)
        data.add_callback_group_add_client(0, 0, 0, 0, 0)
        data.finalize()
