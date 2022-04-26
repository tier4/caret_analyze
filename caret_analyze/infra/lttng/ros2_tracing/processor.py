# Copyright 2019 Robert Bosch GmbH
# Copyright 2020 Christophe Bedard
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

"""Module for trace events processor and ROS 2 model creation."""

from typing import Dict, List, Set, Tuple

from tracetools_analysis.processor import (EventHandler, EventMetadata,
                                           HandlerMap)
from tracetools_read import get_field

from .data_model import Ros2DataModel


class Ros2Handler(EventHandler):
    """
    ROS 2-aware event handling class implementation.

    Handles a trace's events and builds a model with the data.
    """

    def __init__(
        self,
        **kwargs,
    ) -> None:
        """Create a Ros2Handler."""
        # Link a ROS trace event to its corresponding handling method

        handler_map: HandlerMap = {}

        handler_map['ros2:rcl_init'] = self._handle_rcl_init
        handler_map['ros2:rcl_node_init'] = self._handle_rcl_node_init
        handler_map['ros2:rcl_publisher_init'] = self._handle_rcl_publisher_init
        handler_map['ros2:rcl_subscription_init'] = self._handle_rcl_subscription_init
        handler_map['ros2:rclcpp_subscription_init'] = self._handle_rclcpp_subscription_init
        handler_map[
            'ros2:rclcpp_subscription_callback_added'
        ] = self._handle_rclcpp_subscription_callback_added
        handler_map['ros2:rcl_service_init'] = self._handle_rcl_service_init
        handler_map[
            'ros2:rclcpp_service_callback_added'
        ] = self._handle_rclcpp_service_callback_added
        handler_map['ros2:rcl_client_init'] = self._handle_rcl_client_init
        handler_map['ros2:rcl_timer_init'] = self._handle_rcl_timer_init
        handler_map['ros2:rclcpp_timer_callback_added'] = self._handle_rclcpp_timer_callback_added
        handler_map['ros2:rclcpp_timer_link_node'] = self._handle_rclcpp_timer_link_node
        handler_map['ros2:rclcpp_callback_register'] = self._handle_rclcpp_callback_register
        handler_map['ros2:callback_start'] = self._handle_callback_start
        handler_map['ros2:callback_end'] = self._handle_callback_end
        handler_map[
            'ros2:rcl_lifecycle_state_machine_init'
        ] = self._handle_rcl_lifecycle_state_machine_init
        handler_map['ros2:rcl_lifecycle_transition'] = self._handle_rcl_lifecycle_transition
        handler_map['ros2:rclcpp_publish'] = self._handle_rclcpp_publish
        handler_map['ros2:message_construct'] = self._handle_message_construct
        handler_map['ros2:rclcpp_intra_publish'] = self._handle_rclcpp_intra_publish
        handler_map[
            'ros2:dispatch_subscription_callback'
        ] = self._handle_dispatch_subscription_callback
        handler_map[
            'ros2:dispatch_intra_process_subscription_callback'
        ] = self._handle_dispatch_intra_process_subscription_callback
        handler_map['ros2_caret:on_data_available'] = self._handle_on_data_available
        handler_map['ros2:rcl_publish'] = self._handle_rcl_publish
        handler_map['ros2_caret:dds_write'] = self._handle_dds_write
        handler_map['ros2_caret:dds_bind_addr_to_stamp'] = self._handle_dds_bind_addr_to_stamp
        handler_map['ros2_caret:dds_bind_addr_to_addr'] = self._handle_dds_bind_addr_to_addr
        handler_map['ros2_caret:rmw_implementation'] = self._handle_rmw_implementation
        handler_map['ros2_caret:add_callback_group'] = self._handle_add_callback_group
        handler_map['ros2_caret:add_callback_group_static_executor'] = \
            self._handle_add_callback_group_static_executor
        handler_map['ros2_caret:construct_executor'] = self._handle_construct_executor
        handler_map['ros2_caret:construct_static_executor'] = \
            self._handle_construct_static_executor
        handler_map['ros2_caret:callback_group_add_timer'] = \
            self._handle_callback_group_add_timer
        handler_map['ros2_caret:callback_group_add_subscription'] = \
            self._handle_callback_group_add_subscription
        handler_map['ros2_caret:callback_group_add_service'] = \
            self._handle_callback_group_add_service
        handler_map['ros2_caret:callback_group_add_client'] = \
            self._handle_callback_group_add_client
        handler_map['ros2_caret:tilde_subscription_init'] = \
            self._handle_tilde_subscription_init
        handler_map['ros2_caret:tilde_publisher_init'] = \
            self._handle_tilde_publisher_init
        handler_map['ros2_caret:tilde_subscribe'] = \
            self._handle_tilde_subscribe
        handler_map['ros2_caret:tilde_publish'] = \
            self._handle_tilde_publish
        handler_map['ros2_caret:tilde_subscribe_added'] = \
            self._handle_tilde_subscribe_added
        handler_map['ros2_caret:sim_time'] = \
            self._handle_sim_time
        handler_map['ros2_caret:symbol_rename'] = \
            self._handle_symbol_rename
        handler_map['ros2_caret:init_bind_transform_broadcaster'] = \
            self._handle_transform_broadcaster
        handler_map['ros2_caret:init_bind_transform_broadcaster_frames'] = \
            self._handle_transform_broadcaster_frames
        handler_map['ros2_caret:construct_tf_buffer'] = \
            self._handle_construct_tf_buffer
        handler_map['ros2_caret:init_bind_tf_buffer_core'] = \
            self._handle_init_bind_tf_buffer_core
        handler_map['ros2_caret:construct_node_hook'] = \
            self._handle_construct_node_hook
        handler_map['ros2_caret:send_transform'] = \
            self._handle_send_transform
        handler_map['ros2_caret:init_tf_broadcaster_frame_id_compact'] = \
            self._handle_init_tf_broadcaster_frame_id_compact
        handler_map['ros2_caret:init_tf_buffer_frame_id_compact'] = \
            self._handle_init_tf_buffer_frame_id_compact
        handler_map['ros2_caret:tf_lookup_transform_start'] = \
            self._handle_tf_lookup_transform_start
        handler_map['ros2_caret:tf_lookup_transform_end'] = \
            self._handle_tf_lookup_transform_end
        handler_map['ros2_caret:init_bind_tf_buffer_cache'] = \
            self._handle_init_bind_tf_buffer_cache
        handler_map['ros2_caret:tf_buffer_find_closest'] = \
            self._handle_tf_buffer_find_closest
        handler_map['ros2_caret:tf_set_transform'] = \
            self._handle_tf_set_transform

        super().__init__(
            handler_map=handler_map,
            data_model=Ros2DataModel(),
            **kwargs,
        )

        # Temporary buffers
        self._callback_instances: Dict[int, Tuple[Dict, EventMetadata]] = {}

    @staticmethod
    def get_trace_points() -> List[str]:
        return [
            'ros2:rcl_init',
            'ros2:rcl_node_init',
            'ros2:rcl_publisher_init',
            'ros2:rcl_subscription_init',
            'ros2:rclcpp_subscription_init',
            'ros2:rclcpp_subscription_callback_added',
            'ros2:rcl_service_init',
            'ros2:rclcpp_service_callback_added',
            'ros2:rcl_client_init',
            'ros2:rcl_timer_init',
            'ros2:rclcpp_timer_callback_added',
            'ros2:rclcpp_timer_link_node',
            'ros2:rclcpp_callback_register',
            'ros2:callback_start',
            'ros2:callback_end',
            'ros2:rcl_lifecycle_state_machine_init',
            'ros2:rcl_lifecycle_transition',
            'ros2:rclcpp_publish',
            'ros2:message_construct',
            'ros2:rclcpp_intra_publish',
            'ros2:dispatch_subscription_callback',
            'ros2:dispatch_intra_process_subscription_callback',
            'ros2_caret:on_data_available',
            'ros2:rcl_publish',
            'ros2_caret:dds_write',
            'ros2_caret:dds_bind_addr_to_stamp',
            'ros2_caret:dds_bind_addr_to_addr',
            'ros2_caret:rmw_implementation',
            'ros2_caret:add_callback_group',
            'ros2_caret:add_callback_group_static_executor',
            'ros2_caret:construct_executor',
            'ros2_caret:construct_static_executor',
            'ros2_caret:callback_group_add_timer',
            'ros2_caret:callback_group_add_subscription',
            'ros2_caret:callback_group_add_service',
            'ros2_caret:callback_group_add_client',
            'ros2_caret:tilde_subscription_init',
            'ros2_caret:tilde_publisher_init',
            'ros2_caret:tilde_subscribe',
            'ros2_caret:tilde_publish',
            'ros2_caret:tilde_subscribe_added',
            'ros2_caret:sim_time',
        ]

    @staticmethod
    def required_events() -> Set[str]:
        return {
            'ros2:rcl_init',
        }

    @property
    def data(self) -> Ros2DataModel:
        return super().data  # type: ignore

    def _handle_rcl_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        context_handle = get_field(event, 'context_handle')
        timestamp = metadata.timestamp
        pid = metadata.pid
        tid = metadata.tid
        version = get_field(event, 'version')
        self.data.add_context(context_handle, timestamp, pid, tid, version)

    def _handle_rcl_node_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'node_handle')
        timestamp = metadata.timestamp
        rmw_handle = get_field(event, 'rmw_handle')
        name = get_field(event, 'node_name')
        namespace = get_field(event, 'namespace')
        self.data.add_node(metadata.pid, metadata.tid, handle, timestamp, rmw_handle, name, namespace)

    def _handle_rcl_publisher_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_publisher_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')
        self.data.add_publisher(
            metadata.pid, metadata.tid,
            handle, timestamp, node_handle, rmw_handle, topic_name, depth)

    def _handle_rcl_subscription_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'subscription_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_subscription_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')
        self.data.add_rcl_subscription(
            metadata.pid,
            metadata.tid,
            handle,
            timestamp,
            node_handle,
            rmw_handle,
            topic_name,
            depth,
        )

    def _handle_rclcpp_subscription_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        subscription_pointer = get_field(event, 'subscription')
        timestamp = metadata.timestamp
        handle = get_field(event, 'subscription_handle')
        self.data.add_rclcpp_subscription(
            metadata.pid,
            metadata.tid,
            subscription_pointer, timestamp, handle)

    def _handle_rclcpp_subscription_callback_added(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        subscription_pointer = get_field(event, 'subscription')
        timestamp = metadata.timestamp
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(
            metadata.pid,
            metadata.tid,
            subscription_pointer, timestamp, callback_object)

    def _handle_rcl_service_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'service_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_service_handle')
        service_name = get_field(event, 'service_name')
        self.data.add_service(
            metadata.pid,
            metadata.tid,
            handle, timestamp, node_handle, rmw_handle, service_name)

    def _handle_rclcpp_service_callback_added(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'service_handle')
        timestamp = metadata.timestamp
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(
            metadata.pid,
            metadata.tid,
            handle, timestamp, callback_object)

    def _handle_rcl_client_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'client_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_client_handle')
        service_name = get_field(event, 'service_name')
        self.data.add_client(
            metadata.pid,
            metadata.tid,
            handle, timestamp, node_handle, rmw_handle, service_name)

    def _handle_rcl_timer_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        period = get_field(event, 'period')
        self.data.add_timer(
            metadata.pid,
            metadata.tid,
            handle, timestamp, period)

    def _handle_rclcpp_timer_callback_added(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(
            metadata.pid,
            metadata.tid,
            handle, timestamp, callback_object)

    def _handle_rclcpp_timer_link_node(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        self.data.add_timer_node_link(
            metadata.pid,
            metadata.tid,
            handle, timestamp, node_handle)

    def _handle_rclcpp_callback_register(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        timestamp = metadata.timestamp
        symbol = get_field(event, 'symbol')
        self.data.add_callback_symbol(
            metadata.pid,
            metadata.tid,
            callback_object, timestamp, symbol)

    def _handle_callback_start(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        # Add to dict
        callback = get_field(event, 'callback')
        timestamp = metadata.timestamp
        is_intra_process = get_field(event, 'is_intra_process', raise_if_not_found=False)
        self.data.add_callback_start_instance(
            metadata.pid,
            metadata.tid,
            timestamp,
            callback,
            is_intra_process)

    def _handle_callback_end(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        # Fetch from dict
        callback = get_field(event, 'callback')
        timestamp = metadata.timestamp
        self.data.add_callback_end_instance(
            metadata.pid,
            metadata.tid,
            timestamp,
            callback
        )

    def _handle_rcl_lifecycle_state_machine_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        node_handle = get_field(event, 'node_handle')
        state_machine = get_field(event, 'state_machine')
        self.data.add_lifecycle_state_machine(
            metadata.pid,
            metadata.tid,
            state_machine, node_handle)

    def _handle_rcl_lifecycle_transition(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        state_machine = get_field(event, 'state_machine')
        start_label = get_field(event, 'start_label')
        goal_label = get_field(event, 'goal_label')
        self.data.add_lifecycle_state_transition(
            metadata.pid,
            metadata.tid,
            state_machine, start_label, goal_label, timestamp)

    def _handle_rclcpp_publish(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        message = get_field(event, 'message')
        message_timestamp = get_field(event, 'message_timestamp')
        self.data.add_rclcpp_publish_instance(
            metadata.pid,
            metadata.tid,
            timestamp, publisher_handle, message, message_timestamp)

    def _handle_rcl_publish(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        message = get_field(event, 'message')
        self.data.add_rcl_publish_instance(
            metadata.pid,
            metadata.tid,
            timestamp, publisher_handle, message)

    def _handle_message_construct(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        original_message = get_field(event, 'original_message')
        constructed_message = get_field(event, 'constructed_message')
        timestamp = metadata.timestamp
        self.data.add_message_construct_instance(
            metadata.pid,
            metadata.tid,
            timestamp, original_message, constructed_message)

    def _handle_rclcpp_intra_publish(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        message = get_field(event, 'message')
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        message_timestamp = get_field(event, 'message_timestamp')
        self.data.add_rclcpp_intra_publish_instance(
            metadata.pid,
            metadata.tid,
            timestamp, publisher_handle, message, message_timestamp)

    def _handle_dispatch_subscription_callback(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        message = get_field(event, 'message')
        timestamp = metadata.timestamp
        source_stamp = get_field(event, 'source_stamp')
        message_timestamp = get_field(event, 'message_timestamp')
        self.data.add_dispatch_subscription_callback_instance(
            metadata.pid,
            metadata.tid,
            timestamp, callback_object, message, source_stamp, message_timestamp
        )

    def _handle_dispatch_intra_process_subscription_callback(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        message = get_field(event, 'message')
        timestamp = metadata.timestamp
        message_timestamp = get_field(event, 'message_timestamp')
        self.data.add_dispatch_intra_process_subscription_callback_instance(
            metadata.pid,
            metadata.tid,
            timestamp, callback_object, message, message_timestamp
        )

    def _handle_on_data_available(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        source_stamp = get_field(event, 'source_stamp')
        self.data.add_on_data_available_instance(
            metadata.pid,
            metadata.tid,
            timestamp, source_stamp)

    def _handle_dds_write(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        message = get_field(event, 'message')
        self.data.add_dds_write_instance(
            metadata.pid,
            metadata.tid,
            timestamp, message)

    def _handle_dds_bind_addr_to_stamp(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        addr = get_field(event, 'addr')
        source_stamp = get_field(event, 'source_stamp')
        self.data.add_dds_bind_addr_to_stamp(
            metadata.pid,
            metadata.tid,
            timestamp, addr, source_stamp)

    def _handle_dds_bind_addr_to_addr(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        addr_from = get_field(event, 'addr_from')
        addr_to = get_field(event, 'addr_to')
        self.data.add_dds_bind_addr_to_addr(
            metadata.pid,
            metadata.tid,
            timestamp, addr_from, addr_to)

    def _handle_rmw_implementation(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        metadata
        rmw_impl = get_field(event, 'rmw_impl')
        self.data.add_rmw_implementation(rmw_impl)

    def _handle_construct_executor(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        executor_addr = get_field(event, 'executor_addr')
        executor_type_name = get_field(event, 'executor_type_name')
        self.data.add_executor(
            metadata.pid,
            metadata.tid,
            executor_addr, stamp, executor_type_name)

    def _handle_construct_static_executor(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        executor_addr = get_field(event, 'executor_addr')
        collector_addr = get_field(event, 'entities_collector_addr')
        executor_type_name = get_field(event, 'executor_type_name')
        self.data.add_executor_static(
            metadata.pid,
            metadata.tid,
            executor_addr, collector_addr, stamp, executor_type_name)

    def _handle_add_callback_group(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        executor_addr = get_field(event, 'executor_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')
        self.data.add_callback_group(
            metadata.pid,
            metadata.tid,
            executor_addr, stamp, callback_group_addr, group_type_name)

    def _handle_add_callback_group_static_executor(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        collector_addr = get_field(event, 'entities_collector_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')
        self.data.add_callback_group_static_executor(
            metadata.pid,
            metadata.tid,
            collector_addr, stamp, callback_group_addr, group_type_name)

    def _handle_callback_group_add_timer(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        timer_handle = get_field(event, 'timer_handle')
        self.data.callback_group_add_timer(
            metadata.pid,
            metadata.tid,
            callback_group_addr, stamp, timer_handle)

    def _handle_callback_group_add_subscription(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        subscription_handle = get_field(event, 'subscription_handle')
        self.data.callback_group_add_subscription(
            metadata.pid,
            metadata.tid,
            callback_group_addr, stamp, subscription_handle)

    def _handle_callback_group_add_service(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        service_handle = get_field(event, 'service_handle')
        self.data.callback_group_add_service(
            metadata.pid,
            metadata.tid,
            callback_group_addr, stamp, service_handle)

    def _handle_callback_group_add_client(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        client_handle = get_field(event, 'client_handle')
        self.data.callback_group_add_client(
            metadata.pid,
            metadata.tid,
            callback_group_addr, stamp, client_handle)

    def _handle_tilde_subscription_init(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription = get_field(event, 'subscription')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscription(
            metadata.pid,
            metadata.tid,
            subscription, node_name, topic_name, timestamp)

    def _handle_tilde_publisher_init(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        publisher = get_field(event, 'publisher')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_publisher(
            metadata.pid,
            metadata.tid,
            publisher, node_name, topic_name, timestamp)

    def _handle_tilde_subscribe(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription = get_field(event, 'subscription')
        tilde_message_id = get_field(event, 'tilde_message_id')
        self.data.add_tilde_subscribe(
            metadata.pid,
            metadata.tid,
            timestamp, subscription, tilde_message_id)

    def _handle_tilde_publish(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        publisher = get_field(event, 'publisher')
        publish_tilde_timestamp = metadata.timestamp
        message_info_ids = get_field(event, 'message_info_ids')
        message_ids = get_field(event, 'message_ids')
        for message_info_id, message_id in zip(message_info_ids, message_ids):
            self.data.add_tilde_publish(
                metadata.pid,
                metadata.tid,
                publish_tilde_timestamp,
                publisher,
                message_info_id,
                message_id)

    def _handle_tilde_subscribe_added(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription_id = get_field(event, 'subscription_id')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscribe_added(
            metadata.pid,
            metadata.tid,
            subscription_id, node_name, topic_name, timestamp)

    def _handle_sim_time(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        sim_time = get_field(event, 'stamp')
        self.data.add_sim_time(
            metadata.pid,
            metadata.tid,
            timestamp, sim_time)

    def _handle_symbol_rename(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        symbol_from = get_field(event, 'symbol_from')
        symbol_to = get_field(event, 'symbol_to')
        self.data.add_symbol_rename(
            metadata.pid,
            metadata.tid,
            timestamp, symbol_from, symbol_to)

    def _handle_transform_broadcaster(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        broadcaster = get_field(event, 'tf_broadcaster')
        publisher_handle = get_field(event, 'publisher_handle')
        self.data.add_transform_broadcaster(
            metadata.pid,
            metadata.tid,
            timestamp, broadcaster, publisher_handle)

    def _handle_transform_broadcaster_frames(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        broadcaster = get_field(event, 'tf_broadcaster')
        frame_id = get_field(event, 'frame_id')
        child_frame_id = get_field(event, 'child_frame_id')
        self.data.add_transform_broadcaster_frames(
            metadata.pid,
            metadata.tid,
            timestamp, broadcaster, frame_id, child_frame_id)

    def _handle_construct_tf_buffer(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer = get_field(event, 'tf_buffer')
        buffer_core = get_field(event, 'tf_buffer_core')
        clock = get_field(event, 'clock')
        self.data.add_construct_tf_buffer(
            metadata.pid,
            metadata.tid,
            timestamp, buffer, buffer_core, clock)

    def _handle_init_bind_tf_buffer_core(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        callback = get_field(event, 'callback')
        self.data.add_init_bind_tf_buffer_core(
            metadata.pid,
            metadata.tid,
            timestamp, buffer_core, callback)

    def _handle_construct_node_hook(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        clock = get_field(event, 'clock')
        self.data.add_construct_node_hook(
            metadata.pid,
            metadata.tid,
            timestamp, node_handle, clock)

    def _handle_send_transform(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        broadcaster = get_field(event, 'tf_broadcaster')
        stamps = get_field(event, 'stamps')
        frame_ids_compact = get_field(event, 'frame_ids_compact')
        child_frame_ids_compact = get_field(event, 'child_frame_ids_compact')
        for i in range(len(child_frame_ids_compact)):
            self.data.add_send_transform(
                metadata.pid,
                metadata.tid,
                timestamp,
                broadcaster,
                stamps[i],
                frame_ids_compact[i],
                child_frame_ids_compact[i],
            )

    def _handle_init_tf_broadcaster_frame_id_compact(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        broadcaster = get_field(event, 'tf_broadcaster')
        frame_id = get_field(event, 'frame_id')
        frame_id_compact = get_field(event, 'frame_id_compact')
        self.data.add_broadcaster_frame_id_compact(
            metadata.pid,
            metadata.tid,
            timestamp,
            broadcaster,
            frame_id,
            frame_id_compact
        )

    def _handle_init_tf_buffer_frame_id_compact(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        transform_buffer_core = get_field(event, 'tf_buffer_core')
        frame_id = get_field(event, 'frame_id')
        frame_id_compact = get_field(event, 'frame_id_compact')
        self.data.add_buffer_frame_id_compact(
            metadata.pid,
            metadata.tid,
            timestamp,
            transform_buffer_core,
            frame_id,
            frame_id_compact
        )

    def _handle_tf_lookup_transform_start(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        target_time = get_field(event, 'target_time')
        frame_id_compact = get_field(event, 'frame_id_compact')
        child_frame_id_compact = get_field(event, 'child_frame_id_compact')
        self.data.add_tf_lookup_transform_start(
            metadata.pid,
            metadata.tid,
            timestamp,
            buffer_core,
            target_time,
            frame_id_compact,
            child_frame_id_compact
        )

    def _handle_tf_lookup_transform_end(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        self.data.add_tf_lookup_transform_end(
            metadata.pid,
            metadata.tid,
            timestamp,
            buffer_core,
        )

    def _handle_init_bind_tf_buffer_cache(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        buffer_cache = get_field(event, 'tf_buffer_cache')
        self.data.add_init_bind_tf_buffer_cache(
            metadata.pid,
            metadata.tid,
            timestamp,
            buffer_core,
            buffer_cache
        )

    def _handle_tf_buffer_find_closest(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        frame_id_compact = get_field(event, 'frame_id_compact')
        child_frame_id_compact = get_field(event, 'child_frame_id_compact')
        stamp = get_field(event, 'stamp')
        frame_id_compact_ = get_field(event, 'frame_id_compact_')
        child_frame_id_compact_ = get_field(event, 'child_frame_id_compact_')
        stamp_ = get_field(event, 'stamp_')

        self.data.add_tf_find_closest(
            metadata.pid,
            metadata.tid,
            timestamp,
            buffer_core,
            frame_id_compact,
            child_frame_id_compact,
            stamp,
            frame_id_compact_,
            child_frame_id_compact_,
            stamp_
        )

    def _handle_tf_set_transform(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        buffer_core = get_field(event, 'tf_buffer_core')
        stamp = get_field(event, 'stamp')
        frame_id_compact = get_field(event, 'frame_id_compact')
        child_frame_id_compact = get_field(event, 'child_frame_id_compact')

        self.data.add_tf_set_transform(
            metadata.pid,
            metadata.tid,
            timestamp,
            buffer_core,
            stamp,
            frame_id_compact,
            child_frame_id_compact,
        )
