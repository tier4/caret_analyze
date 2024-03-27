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

from __future__ import annotations

from typing import NamedTuple


class ColumnName(NamedTuple):
    CALLBACK_START_TIMESTAMP: str = 'callback_start_timestamp'
    CALLBACK_END_TIMESTAMP: str = 'callback_end_timestamp'
    RCLCPP_PUBLISH_TIMESTAMP: str = 'rclcpp_publish_timestamp'
    RCLCPP_INTER_PUBLISH_TIMESTAMP: str = 'rclcpp_inter_publish_timestamp'
    RCL_PUBLISH_TIMESTAMP: str = 'rcl_publish_timestamp'
    DDS_WRITE_TIMESTAMP: str = 'dds_write_timestamp'
    ON_DATA_AVAILABLE_TIMESTAMP: str = 'on_data_available_timestamp'
    RCLCPP_INTRA_PUBLISH_TIMESTAMP: str = 'rclcpp_intra_publish_timestamp'
    RCLCPP_RING_BUFFER_ENQUEUE_TIMESTAMP: str = 'rclcpp_ring_buffer_enqueue_timestamp'
    RCLCPP_RING_BUFFER_DEQUEUE_TIMESTAMP: str = 'rclcpp_ring_buffer_dequeue_timestamp'
    TIMER_EVENT_TIMESTAMP: str = 'timer_event_timestamp'

    MESSAGE: str = 'message'
    DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP: str = \
        'dispatch_intra_process_subscription_callback_timestamp'
    DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP: str = \
        'dispatch_subscription_callback_timestamp'
    RMW_TAKE_TIMESTAMP: str = 'rmw_take_timestamp'
    RMW_SUBSCRIPTION_HANDLE: str = 'rmw_subscription_handle'
    SOURCE_TIMESTAMP: str = 'source_timestamp'
    CALLBACK_OBJECT: str = 'callback_object'
    MESSAGE_CONSTRUCT_TIMESTAMP: str = 'message_construct_timestamp'
    ORIGINAL_MESSAGE: str = 'original_message'
    CONSTRUCTED_MESSAGE: str = 'constructed_message'
    DDS_BIND_ADDR_TO_ADDR_TIMESTAMP: str = 'dds_bind_addr_to_addr_timestamp'
    DDS_BIND_ADDR_TO_STAMP_TIMESTAMP: str = 'dds_bind_addr_to_stamp_timestamp'
    ADDR_FROM: str = 'addr_from'
    ADDR_TO: str = 'addr_to'
    ADDR: str = 'addr'
    IS_INTRA_PROCESS: str = 'is_intra_process'
    PUBLISHER_HANDLE: str = 'publisher_handle'
    MESSAGE_TIMESTAMP: str = 'message_timestamp'

    SUBSCRIPTION_HANDLE: str = 'subscription_handle'
    NODE_HANDLE: str = 'node_handle'
    TOPIC_NAME: str = 'topic_name'
    DEPTH: str = 'depth'
    NODE_NAME: str = 'node_name'
    SYMBOL: str = 'symbol'
    CALLBACK_NAME: str = 'callback_name'
    TID: str = 'tid'
    INDEX: str = 'index'
    BUFFER: str = 'buffer'
    SIZE: str = 'size'
    OVERWRITTEN: str = 'overwritten'

    TILDE_PUBLISHER: str = 'tilde_publisher'
    TILDE_SUBSCRIPTION: str = 'tilde_subscription'
    TILDE_PUBLISH_TIMESTAMP: str = 'tilde_publish_timestamp'
    TILDE_SUBSCRIBE_TIMESTAMP: str = 'tilde_subscribe_timestamp'
    TILDE_MESSAGE_ID: str = 'tilde_message_id'


COLUMN_NAME = ColumnName()
