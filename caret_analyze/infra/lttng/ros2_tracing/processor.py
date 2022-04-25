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
        version = get_field(event, 'version')
        self.data.add_context(context_handle, timestamp, pid, version)

    def _handle_rcl_node_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'node_handle')
        timestamp = metadata.timestamp
        tid = metadata.tid
        rmw_handle = get_field(event, 'rmw_handle')
        name = get_field(event, 'node_name')
        namespace = get_field(event, 'namespace')
        self.data.add_node(handle, timestamp, tid, rmw_handle, name, namespace)

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
            handle, timestamp, node_handle, rmw_handle, service_name)

    def _handle_rclcpp_service_callback_added(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'service_handle')
        timestamp = metadata.timestamp
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(handle, timestamp, callback_object)

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
        self.data.add_client(handle, timestamp, node_handle,
                             rmw_handle, service_name)

    def _handle_rcl_timer_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        period = get_field(event, 'period')
        tid = metadata.tid
        self.data.add_timer(handle, timestamp, period, tid)

    def _handle_rclcpp_timer_callback_added(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        callback_object = get_field(event, 'callback')
        self.data.add_callback_object(handle, timestamp, callback_object)

    def _handle_rclcpp_timer_link_node(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = metadata.timestamp
        node_handle = get_field(event, 'node_handle')
        self.data.add_timer_node_link(handle, timestamp, node_handle)

    def _handle_rclcpp_callback_register(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        callback_object = get_field(event, 'callback')
        timestamp = metadata.timestamp
        symbol = get_field(event, 'symbol')
        self.data.add_callback_symbol(callback_object, timestamp, symbol)

    def _handle_callback_start(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        # Add to dict
        callback = get_field(event, 'callback')
        timestamp = metadata.timestamp
        is_intra_process = get_field(
            event, 'is_intra_process', raise_if_not_found=False)
        self.data.add_callback_start_instance(
            timestamp, callback, is_intra_process)

    def _handle_callback_end(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        # Fetch from dict
        callback = get_field(event, 'callback')
        timestamp = metadata.timestamp
        self.data.add_callback_end_instance(timestamp, callback)

    def _handle_rcl_lifecycle_state_machine_init(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        node_handle = get_field(event, 'node_handle')
        state_machine = get_field(event, 'state_machine')
        self.data.add_lifecycle_state_machine(state_machine, node_handle)

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
            state_machine, start_label, goal_label, timestamp)

    def _handle_rclcpp_publish(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        message = get_field(event, 'message')
        tid = metadata.tid
        message_timestamp = get_field(event, 'message_timestamp')
        self.data.add_rclcpp_publish_instance(
            tid, timestamp, publisher_handle, message, message_timestamp)

    def _handle_rcl_publish(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = metadata.timestamp
        tid = metadata.tid
        message = get_field(event, 'message')
        self.data.add_rcl_publish_instance(
            tid, timestamp, publisher_handle, message)

    def _handle_message_construct(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        original_message = get_field(event, 'original_message')
        constructed_message = get_field(event, 'constructed_message')
        timestamp = metadata.timestamp
        self.data.add_message_construct_instance(
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
        tid = metadata.tid
        self.data.add_rclcpp_intra_publish_instance(
            tid, timestamp, publisher_handle, message, message_timestamp)

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
            timestamp, callback_object, message, message_timestamp
        )

    def _handle_on_data_available(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        source_stamp = get_field(event, 'source_stamp')
        self.data.add_on_data_available_instance(timestamp, source_stamp)

    def _handle_dds_write(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        message = get_field(event, 'message')
        tid = metadata.tid
        self.data.add_dds_write_instance(tid, timestamp, message)

    def _handle_dds_bind_addr_to_stamp(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        addr = get_field(event, 'addr')
        tid = metadata.tid
        source_stamp = get_field(event, 'source_stamp')
        self.data.add_dds_bind_addr_to_stamp(tid, timestamp, addr, source_stamp)

    def _handle_dds_bind_addr_to_addr(
        self,
        event: Dict,
        metadata: EventMetadata,
    ) -> None:
        timestamp = metadata.timestamp
        addr_from = get_field(event, 'addr_from')
        addr_to = get_field(event, 'addr_to')
        self.data.add_dds_bind_addr_to_addr(timestamp, addr_from, addr_to)

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
        self.data.add_executor(executor_addr, stamp, executor_type_name)

    def _handle_construct_static_executor(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        executor_addr = get_field(event, 'executor_addr')
        collector_addr = get_field(event, 'entities_collector_addr')
        executor_type_name = get_field(event, 'executor_type_name')
        self.data.add_executor_static(executor_addr, collector_addr, stamp, executor_type_name)

    def _handle_add_callback_group(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        executor_addr = get_field(event, 'executor_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')
        self.data.add_callback_group(executor_addr, stamp, callback_group_addr, group_type_name)

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
            collector_addr, stamp, callback_group_addr, group_type_name)

    def _handle_callback_group_add_timer(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        timer_handle = get_field(event, 'timer_handle')
        self.data.callback_group_add_timer(callback_group_addr, stamp, timer_handle)

    def _handle_callback_group_add_subscription(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        subscription_handle = get_field(event, 'subscription_handle')
        self.data.callback_group_add_subscription(callback_group_addr, stamp, subscription_handle)

    def _handle_callback_group_add_service(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        service_handle = get_field(event, 'service_handle')
        self.data.callback_group_add_service(callback_group_addr, stamp, service_handle)

    def _handle_callback_group_add_client(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        stamp = metadata.timestamp
        callback_group_addr = get_field(event, 'callback_group_addr')
        client_handle = get_field(event, 'client_handle')
        self.data.callback_group_add_client(callback_group_addr, stamp, client_handle)

    def _handle_tilde_subscription_init(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription = get_field(event, 'subscription')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscription(subscription, node_name, topic_name, timestamp)

    def _handle_tilde_publisher_init(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        publisher = get_field(event, 'publisher')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_publisher(publisher, node_name, topic_name, timestamp)

    def _handle_tilde_subscribe(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription = get_field(event, 'subscription')
        tilde_message_id = get_field(event, 'tilde_message_id')
        self.data.add_tilde_subscribe(timestamp, subscription, tilde_message_id)

    def _handle_tilde_publish(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        # timestamp = metadata.timestamp
        publisher = get_field(event, 'publisher')
        publish_tilde_timestamp = get_field(event, 'tilde_publish_timestamp')
        tilde_message_id = get_field(event, 'tilde_message_id')
        subscription_id = get_field(event, 'subscription_id')
        self.data.add_tilde_publish(
            publish_tilde_timestamp,
            publisher,
            subscription_id,
            tilde_message_id)

    def _handle_tilde_subscribe_added(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        subscription_id = get_field(event, 'subscription_id')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscribe_added(subscription_id, node_name, topic_name, timestamp)

    def _handle_sim_time(
        self,
        event: Dict,
        metadata: EventMetadata
    ) -> None:
        timestamp = metadata.timestamp
        sim_time = get_field(event, 'stamp')
        self.data.add_sim_time(timestamp, sim_time)
