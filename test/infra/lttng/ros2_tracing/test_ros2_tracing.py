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


class TestDataModel:

    def test_add_data(self):
        data = Ros2DataModel()
        data.add_add_callback_group(0, 0, 0, 0, 0, 'mutually_exclusive')
        data.add_add_callback_group_static_executor(0, 0, 0, 0, 0, 'mutually_exclusive')
        data.add_callback_end(0, 0, 0, 0)
        data.add_callback_group_add_client(0, 0, 0, 0, 0)
        data.add_callback_group_add_service(0, 0, 0, 0, 0)
        data.add_callback_group_add_subscription(0, 0, 1, 0, 0)
        data.add_callback_group_add_timer(0, 0, 0, 0, 0)
        data.add_callback_start(0, 0, 0, 0, True)
        data.add_construct_executor(0, 0, 0, 0, 'single_threaded_executor')
        data.add_construct_ipm(0, 0, 0, 0)
        data.add_construct_node_hook(0, 0, 0, 0, 0)
        data.add_construct_ring_buffer(0, 0, 0, 0, 0)
        data.add_construct_static_executor(0, 0, 0, 0, 0, 'single_threaded_executor')
        data.add_construct_tf_buffer(0, 0, 0, 0, 0, 0)
        data.add_dds_bind_addr_to_addr(0, 0, 0, 0, 0)
        data.add_dds_bind_addr_to_stamp(0, 0, 0, 0, 0)
        data.add_dds_write(0, 0, 0, 0)
        data.add_dispatch_intra_process_subscription_callback(0, 0, 0, 0, 0, 0)
        data.add_dispatch_subscription_callback(0, 0, 0, 0, 0, 0, 0)
        data.add_init_bind_tf_buffer_core(0, 0, 0, 0, 0)
        data.add_init_bind_transform_broadcaster(0, 0, 0, 0, 0)
        data.add_init_tf_broadcaster_frame_id_compact(0, 0, 0, 0, '', 0)
        data.add_init_tf_buffer_frame_id_compact(0, 0, 0, 0, 0, 0)
        data.add_init_tf_buffer_lookup_transform(0, 0, 0, 0, '', '')
        data.add_init_tf_buffer_set_transform(0, 0, 0, 0, '', '')
        data.add_inter_callback_duration(0, 0, 0, 0, 0, 0, 0, 0)
        data.add_inter_publish(0, 0, 0, 0, 0, 0, 0, 0)
        data.add_intra_callback_duration(0, 0, 0, 0, 0, 0, 0)
        data.add_ipm_add_publisher(0, 0, 0, 0, 0, 0)
        data.add_ipm_add_subscription(0, 0, 0, 0, 0, 0)
        data.add_ipm_insert_sub_id_for_pub(0, 0, 0, 0, 0, 0, 1)
        data.add_message_construct(0, 0, 0, 0, 0)
        data.add_on_data_available(0, 0, 0, 0)
        data.add_rcl_client_init(0, 0, 0, 0, 0, 0, 'service_name')
        data.add_rcl_init(0, 0, 0, 0, '')
        data.add_rcl_init_caret('', 0, 0, 0)
        data.add_rcl_lifecycle_state_machine_init(0, 0, 0, 0)
        data.add_rcl_lifecycle_transition(0, 0, 0, '', '', 0)
        data.add_rcl_node_init(0, 0, 0, 0, 0, 'name', 'ns')
        data.add_rcl_publish(0, 0, 0, 0, 0)
        data.add_rcl_publisher_init(0, 0, 0, 0, 0, 0, 'topic_name', 0)
        data.add_rcl_service_init(0, 0, 0, 0, 0, 0, 'service_name')
        data.add_rcl_subscription_init(0, 0, 0, 0, 0, 0, '0', 0)
        data.add_rcl_subscription_init(0, 0, 0, 0, 0, 0, 'topic_name', 0)
        data.add_rcl_timer_init(0, 0, 0, 0, 0)
        data.add_rclcpp_callback_register(0, 0, 0, 0, 'symbol')
        data.add_rclcpp_intra_publish(0, 0, 0, 0, 0, 0)
        data.add_rclcpp_publish(0, 0, 0, 0, 0, 0)
        data.add_rclcpp_publisher_init(0, '0.0', 0, 0, 0)
        data.add_rclcpp_service_callback_added(0, 0, 0, 0, 0)
        data.add_rclcpp_subscription_callback_added(0, 0, 0, 0, 0)
        data.add_rclcpp_subscription_init(0, 0, 0, 0, 0)
        data.add_rclcpp_timer_callback_added(0, 0, 0, 0, 0)
        data.add_rclcpp_timer_link_node(0, 0, 0, 0, 0)
        data.add_ring_buffer_clear(0, 0, 0, 0)
        data.add_ring_buffer_dequeue(0, 0, 0, 0, 0, 0)
        data.add_ring_buffer_enqueue(0, 0, 0, 0, 0, 0, 1)
        data.add_rmw_implementation(0, 0, 'rmw_impl')
        data.add_send_transform(0, 0, 0, 0, 0, 0, 0)
        data.add_sim_time(0, 0, 0, 0)
        data.add_sim_time(0, 0, 0, 0)
        data.add_symbol_rename(0, 0, 0, '', '')
        data.add_tf_buffer_find_closest(0, 0, 0, 0, 0, 0, 0, 1, 2, 3)
        data.add_tf_lookup_transform(0, 0, 0, 0, 0, 0, 0)
        data.add_tf_lookup_transform_end(0, 0, 0, 0)
        data.add_tf_lookup_transform_start(0, 0, 0, 0, 0, 0, 0)
        data.add_tf_set_transform(0, 0, 0, 0, 0, 0, 0)
        data.add_tilde_publish(0, 0, 0, 0, 0, 0)
        data.add_tilde_publisher_init(0, 0, 0, '', '', 0)
        data.add_tilde_subscribe(0, 0, 0, 0, 0)
        data.add_tilde_subscribe_added(0, 0, 0, '', '', 0)
        data.add_tilde_subscription_init(0, 0, 0, '', 0, 0)
        data.finalize()
