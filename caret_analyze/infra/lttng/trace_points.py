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

from typing import NamedTuple


class TracePoint(NamedTuple):
    ADD_CALLBACK_GROUP: str = 'ros2_caret:add_callback_group'
    ADD_CALLBACK_GROUP_STATIC_EXECUTOR: str = 'ros2_caret:add_callback_group_static_executor'
    CALLBACK_END: str = 'ros2:callback_end'
    CALLBACK_GROUP_ADD_CLIENT: str = 'ros2_caret:callback_group_add_client'
    CALLBACK_GROUP_ADD_SERVICE: str = 'ros2_caret:callback_group_add_service'
    CALLBACK_GROUP_ADD_SUBSCRIPTION: str = 'ros2_caret:callback_group_add_subscription'
    CALLBACK_GROUP_ADD_TIMER: str = 'ros2_caret:callback_group_add_timer'
    CALLBACK_START: str = 'ros2:callback_start'
    CONSTRUCT_EXECUTOR: str = 'ros2_caret:construct_executor'
    CONSTRUCT_IPM: str = 'ros2_caret:construct_ipm'
    CONSTRUCT_NODE_HOOK: str = 'ros2_caret:construct_node_hook'
    CONSTRUCT_RING_BUFFER: str = 'ros2:construct_ring_buffer'
    CONSTRUCT_STATIC_EXECUTOR: str = 'ros2_caret:construct_static_executor'
    CONSTRUCT_TF_BUFFER: str = 'ros2_caret:construct_tf_buffer'
    DDS_BIND_ADDR_TO_ADDR: str = 'ros2_caret:dds_bind_addr_to_addr'
    DDS_BIND_ADDR_TO_STAMP: str = 'ros2_caret:dds_bind_addr_to_stamp'
    DDS_WRITE: str = 'ros2_caret:dds_write'
    DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK: str = \
        'ros2:dispatch_intra_process_subscription_callback'
    DISPATCH_SUBSCRIPTION_CALLBACK: str = 'ros2:dispatch_subscription_callback'
    INIT_BIND_TF_BROADCASTER_SEND_TRANSFORM: str = \
        'ros2_caret:init_bind_tf_broadcaster_send_transform'
    INIT_BIND_TF_BUFFER_CORE: str = 'ros2_caret:init_bind_tf_buffer_core'
    INIT_BIND_TRANSFORM_BROADCASTER: str = 'ros2_caret:init_bind_transform_broadcaster'
    INIT_TF_BROADCASTER_FRAME_ID_COMPACT: str = 'ros2_caret:init_tf_broadcaster_frame_id_compact'
    INIT_TF_BUFFER_FRAME_ID_COMPACT: str = 'ros2_caret:init_tf_buffer_frame_id_compact'
    INIT_TF_BUFFER_LOOKUP_TRANSFORM: str = 'ros2_caret:init_tf_buffer_lookup_transform'
    INIT_TF_BUFFER_SET_TRANSFORM: str = 'ros2_caret:init_tf_buffer_set_transform'
    INTER_CALLBACK_DURATION: str = 'ros2_caret:inter_callback_duration'
    INTER_PUBLISH: str = 'ros2_caret:inter_publish'
    INTRA_CALLBACK_DURATION: str = 'ros2_caret:intra_callback_duration'
    IPM_ADD_PUBLISHER: str = 'ros2_caret:ipm_add_publisher'
    IPM_ADD_SUBSCRIPTION: str = 'ros2_caret:ipm_add_subscription'
    IPM_INSERT_SUB_ID_FOR_PUB: str = 'ros2_caret:ipm_insert_sub_id_for_pub'
    MESSAGE_CONSTRUCT: str = 'ros2:message_construct'
    ON_DATA_AVAILABLE: str = 'ros2_caret:on_data_available'
    RCLCPP_CALLBACK_REGISTER: str = 'ros2:rclcpp_callback_register'
    RCLCPP_INTRA_PUBLISH: str = 'ros2:rclcpp_intra_publish'
    RCLCPP_PUBLISH: str = 'ros2:rclcpp_publish'
    RCLCPP_PUBLISHER_INIT: str = 'ros2:rclcpp_publisher_init'
    RCLCPP_SERVICE_CALLBACK_ADDED: str = 'ros2:rclcpp_service_callback_added'
    RCLCPP_SUBSCRIPTION_CALLBACK_ADDED: str = 'ros2:rclcpp_subscription_callback_added'
    RCLCPP_SUBSCRIPTION_INIT: str = 'ros2:rclcpp_subscription_init'
    RCLCPP_TIMER_CALLBACK_ADDED: str = 'ros2:rclcpp_timer_callback_added'
    RCLCPP_TIMER_LINK_NODE: str = 'ros2:rclcpp_timer_link_node'
    RCL_CLIENT_INIT: str = 'ros2:rcl_client_init'
    RCL_INIT: str = 'ros2:rcl_init'
    RCL_INIT_CARET: str = 'ros2_caret:rcl_init_caret'
    RCL_LIFECYCLE_STATE_MACHINE_INIT: str = 'ros2:rcl_lifecycle_state_machine_init'
    RCL_LIFECYCLE_TRANSITION: str = 'ros2:rcl_lifecycle_transition'
    RCL_NODE_INIT: str = 'ros2:rcl_node_init'
    RCL_PUBLISH: str = 'ros2:rcl_publish'
    RCL_PUBLISHER_INIT: str = 'ros2:rcl_publisher_init'
    RCL_SERVICE_INIT: str = 'ros2:rcl_service_init'
    RCL_SUBSCRIPTION_INIT: str = 'ros2:rcl_subscription_init'
    RCL_TIMER_INIT: str = 'ros2:rcl_timer_init'
    RING_BUFFER_CLEAR: str = 'ros2:ring_buffer_clear'
    RING_BUFFER_DEQUEUE: str = 'ros2:ring_buffer_dequeue'
    RING_BUFFER_ENQUEUE: str = 'ros2:ring_buffer_enqueue'
    RMW_IMPLEMENTATION: str = 'ros2_caret:rmw_implementation'
    SEND_TRANSFORM: str = 'ros2_caret:send_transform'
    SIM_TIME: str = 'ros2_caret:sim_time'
    SYMBOL_RENAME: str = 'ros2_caret:symbol_rename'
    TF_BUFFER_FIND_CLOSEST: str = 'ros2_caret:tf_buffer_find_closest'
    TF_LOOKUP_TRANSFORM: str = 'ros2_caret:tf_lookup_transform'
    TF_LOOKUP_TRANSFORM_END: str = 'ros2_caret:tf_lookup_transform_end'
    TF_LOOKUP_TRANSFORM_START: str = 'ros2_caret:tf_lookup_transform_start'
    TF_SET_TRANSFORM: str = 'ros2_caret:tf_set_transform'
    TILDE_PUBLISH: str = 'ros2_caret:tilde_publish'
    TILDE_PUBLISHER_INIT: str = 'ros2_caret:tilde_publisher_init'
    TILDE_SUBSCRIBE: str = 'ros2_caret:tilde_subscribe'
    TILDE_SUBSCRIBE_ADDED: str = 'ros2_caret:tilde_subscribe_added'
    TILDE_SUBSCRIPTION_INIT: str = 'ros2_caret:tilde_subscription_init'


TRACE_POINT = TracePoint()
