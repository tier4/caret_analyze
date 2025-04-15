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

from __future__ import annotations

from collections import defaultdict

from logging import getLogger

from typing import Any, TYPE_CHECKING

import bt2

from .data_model import Ros2DataModel

logger = getLogger(__name__)

if TYPE_CHECKING:
    from ..id_remapper import IDRemapperCollection


def get_field(event, key):
    e = event[key]
    if isinstance(e, bt2._StringFieldConst):
        return str(e)
    if isinstance(e, bt2._IntegerFieldConst):
        return int(e)
    return e


def pop_field(event, key):
    e = event.pop(key)
    if isinstance(e, bt2._StringFieldConst):
        return str(e)
    if isinstance(e, bt2._IntegerFieldConst):
        return int(e)
    return e


class Ros2Handler():
    """
    ROS 2-aware event handling class implementation.

    Handles a trace's events and builds a model with the data.
    """

    def __init__(
        self,
        data: Ros2DataModel,
        remapper: IDRemapperCollection,
        monotonic_to_system_time_offset: int | None,
    ) -> None:
        """
        Create Ros2Handler.

        Parameters
        ----------
        data : Ros2DataModel
            DataModel to be handles
        remapper : IDRemapperCollection
            Collection of IDRemappers.
        monotonic_to_system_time_offset : int | None
            Offset time to convert monotonic time to system time.
            This values should be valid number if a recording was done with runtime recording.
            None is given, if recording begins before launch of the application,
            to use time sampled from the trace point.

        """
        self._remapper = remapper

        self._monotonic_to_system_offset: int | None = monotonic_to_system_time_offset
        self._caret_init_recorded: defaultdict[int, bool] = defaultdict(lambda: False)
        self._distributions_cache: str | None = None

        # Temporary buffers
        self._callback_instances: dict[int, tuple[dict, Any]] = {}
        self._data = data

    @staticmethod
    def get_trace_points(include_wrapped_tracepoints=True) -> list[str]:
        tracepoints = [
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
            'ros2:rclcpp_ring_buffer_enqueue',
            'ros2:rclcpp_ring_buffer_dequeue',
            'ros2:dispatch_subscription_callback',
            'ros2:rmw_take',
            'ros2:dispatch_intra_process_subscription_callback',
            'ros2_caret:on_data_available',
            'ros2:rcl_publish',
            'ros2_caret:caret_init',
            'ros2_caret:dds_write',
            'ros2_caret:dds_bind_addr_to_stamp',
            'ros2_caret:dds_bind_addr_to_addr',
            'ros2_caret:rmw_implementation',
            'ros2_caret:add_callback_group',
            'ros2_caret:add_callback_group_static_executor',
            'ros2_caret:callback_group_to_executor_entity_collector',
            'ros2_caret:executor_entity_collector_to_executor',
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
            'ros2:rclcpp_buffer_to_ipb',
            'ros2:rclcpp_ipb_to_subscription',
            'ros2:rclcpp_construct_ring_buffer',
        ]

        if include_wrapped_tracepoints:
            tracepoints.extend(
                [
                    'ros2_caret:rcl_init',
                    'ros2_caret:rcl_node_init',
                    'ros2_caret:rcl_publisher_init',
                    'ros2_caret:rcl_subscription_init',
                    'ros2_caret:rclcpp_subscription_init',
                    'ros2_caret:rclcpp_subscription_callback_added',
                    'ros2_caret:rcl_service_init',
                    'ros2_caret:rclcpp_service_callback_added',
                    'ros2_caret:rcl_client_init',
                    'ros2_caret:rcl_timer_init',
                    'ros2_caret:rclcpp_timer_callback_added',
                    'ros2_caret:rclcpp_timer_link_node',
                    'ros2_caret:rclcpp_callback_register',
                    'ros2_caret:rcl_lifecycle_state_machine_init',
                    'ros2_caret:rclcpp_buffer_to_ipb',
                    'ros2_caret:rclcpp_ipb_to_subscription',
                    'ros2_caret:rclcpp_construct_ring_buffer',
                ]
            )
        return tracepoints

    @staticmethod
    def required_events() -> set[str]:
        return {
            'ros2:rcl_init',
        }

    @property
    def data(self) -> Ros2DataModel:
        return self._data  # type: ignore

    def create_init_handler_map(
        self,
    ) -> None:
        """Link a ROS trace initialize event to its corresponding handling method."""
        # Link a ROS trace event to its corresponding handling method

        handler_map = {}

        #  Tracepoints of initialization defined in ros2_tracing
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

        #  Tracepoints of initialization defined in ros2_tracing
        handler_map[
            'ros2:rcl_lifecycle_state_machine_init'
        ] = self._handle_rcl_lifecycle_state_machine_init

        #  The iron trace points for measurements defined by ros2_tracing
        handler_map['ros2:rclcpp_buffer_to_ipb'] = self._handle_rclcpp_buffer_to_ipb
        handler_map['ros2:rclcpp_ipb_to_subscription'] = self._handle_rclcpp_ipb_to_subscription
        handler_map['ros2:rclcpp_construct_ring_buffer'] = \
            self._handle_rclcpp_construct_ring_buffer

        #  Trace points of initialization defined by caret_trace
        handler_map['ros2_caret:rmw_implementation'] = \
            self._handle_rmw_implementation
        handler_map['ros2_caret:add_callback_group'] = \
            self._handle_add_callback_group
        handler_map['ros2_caret:add_callback_group_static_executor'] = \
            self._handle_add_callback_group_static_executor
        handler_map['ros2_caret:callback_group_to_executor_entity_collector'] = \
            self._handle_callback_group_to_executor_entity_collector
        handler_map['ros2_caret:executor_entity_collector_to_executor'] = \
            self._handle_executor_entity_collector_to_executor
        handler_map['ros2_caret:construct_executor'] = \
            self._handle_construct_executor
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

        #  Trace points of initialization defined in TILDE
        handler_map['ros2_caret:tilde_subscription_init'] = \
            self._handle_tilde_subscription_init
        handler_map['ros2_caret:tilde_publisher_init'] = \
            self._handle_tilde_publisher_init

        #  Trace points of initialization defined in TILDE
        handler_map['ros2_caret:tilde_subscribe_added'] = \
            self._handle_tilde_subscribe_added

        #  Trace points of initialization redefined in caret_trace
        handler_map['ros2_caret:rcl_timer_init'] = \
            self._handle_rcl_timer_init
        handler_map['ros2_caret:caret_init'] = self._handle_caret_init

        #  Trace points of initialization redefined in caret_trace
        handler_map['ros2_caret:rcl_init'] = \
            self._handle_rcl_init
        handler_map['ros2_caret:rcl_node_init'] = \
            self._handle_rcl_node_init
        handler_map['ros2_caret:rcl_publisher_init'] = \
            self._handle_rcl_publisher_init
        handler_map['ros2_caret:rcl_subscription_init'] = \
            self._handle_rcl_subscription_init
        handler_map['ros2_caret:rclcpp_subscription_init'] = \
            self._handle_rclcpp_subscription_init
        handler_map['ros2_caret:rclcpp_subscription_callback_added'] = \
            self._handle_rclcpp_subscription_callback_added
        handler_map['ros2_caret:rcl_service_init'] = \
            self._handle_rcl_service_init
        handler_map['ros2_caret:rclcpp_service_callback_added'] = \
            self._handle_rclcpp_service_callback_added
        handler_map['ros2_caret:rcl_client_init'] = \
            self._handle_rcl_client_init
        handler_map['ros2_caret:rclcpp_timer_callback_added'] = \
            self._handle_rclcpp_timer_callback_added
        handler_map['ros2_caret:rclcpp_timer_link_node'] = \
            self._handle_rclcpp_timer_link_node
        handler_map['ros2_caret:rclcpp_callback_register'] = \
            self._handle_rclcpp_callback_register
        handler_map['ros2_caret:rcl_lifecycle_state_machine_init'] = \
            self._handle_rcl_lifecycle_state_machine_init

        # The iron trace points of initialization redefined in CARET.
        handler_map['ros2_caret:rclcpp_buffer_to_ipb'] = \
            self._handle_rclcpp_buffer_to_ipb
        handler_map['ros2_caret:rclcpp_ipb_to_subscription'] = \
            self._handle_rclcpp_ipb_to_subscription
        handler_map['ros2_caret:rclcpp_construct_ring_buffer'] = \
            self._handle_rclcpp_construct_ring_buffer

        self.handler_map = handler_map

    def create_runtime_handler_map(
        self,
    ) -> None:
        """Link a ROS trace runtime event to its corresponding handling method."""
        # Link a ROS trace event to its corresponding handling method

        handler_map = {}

        #  Trace points for measurements defined by ros2_tracing
        handler_map['ros2:callback_start'] = self._handle_callback_start
        handler_map['ros2:callback_end'] = self._handle_callback_end

        #  Trace points for measurements defined by ros2_tracing
        handler_map['ros2:rcl_lifecycle_transition'] = self._handle_rcl_lifecycle_transition
        handler_map['ros2:rclcpp_publish'] = self._handle_rclcpp_publish
        handler_map['ros2:message_construct'] = self._handle_message_construct
        handler_map['ros2:rclcpp_intra_publish'] = self._handle_rclcpp_intra_publish
        handler_map['ros2:dispatch_subscription_callback'] = \
            self._handle_dispatch_subscription_callback
        handler_map['ros2:rmw_take'] = self._handle_rmw_take
        handler_map['ros2:dispatch_intra_process_subscription_callback'] = \
            self._handle_dispatch_intra_process_subscription_callback
        handler_map['ros2_caret:on_data_available'] = self._handle_on_data_available
        handler_map['ros2:rcl_publish'] = self._handle_rcl_publish

        #  Trace points for measurements defined by caret_trace
        handler_map['ros2_caret:dds_write'] = self._handle_dds_write
        handler_map['ros2_caret:dds_bind_addr_to_stamp'] = self._handle_dds_bind_addr_to_stamp
        handler_map['ros2_caret:dds_bind_addr_to_addr'] = self._handle_dds_bind_addr_to_addr

        #  Trace points for measurements defined in TILDE
        handler_map['ros2_caret:tilde_subscribe'] = \
            self._handle_tilde_subscribe
        handler_map['ros2_caret:tilde_publish'] = \
            self._handle_tilde_publish

        #  Trace points for measurements defined by caret_trace
        handler_map['ros2_caret:sim_time'] = \
            self._handle_sim_time

        #  The iron trace points for measurements defined by ros2_tracing
        handler_map['ros2:rclcpp_ring_buffer_enqueue'] = self._handle_rclcpp_ring_buffer_enqueue
        handler_map['ros2:rclcpp_ring_buffer_dequeue'] = self._handle_rclcpp_ring_buffer_dequeue

        self.handler_map = handler_map

    def _is_valid_data(self, event: dict) -> bool:
        """
        Confirm that the data to be converted is appropriate.

        Parameters
        ----------
        event : dict
            Target event.

        Returns
        -------
        bool
            False for runtime recording if it is before caret_init is called,otherwise, True.
            runtime tracepoints only.

        """
        exists_caret_trace = self._monotonic_to_system_offset
        if not exists_caret_trace:
            return True

        pid = get_field(event, '_vpid')
        assert isinstance(pid, int)
        return self._caret_init_recorded[pid]

    def _get_distribution(self, data: Ros2DataModel) -> str:

        if self._distributions_cache is not None:
            return self._distributions_cache

        caret_init_df = data._caret_init._data
        distributions = list(set(caret_init_df['distribution']))
        if len(distributions) > 1:
            logger.info('Multiple ros distributions are found.')

        if len(distributions) == 0:
            return 'NOTFOUND'

        self._distributions_cache = distributions[0]
        return distributions[0]

    def _handle_rcl_init(
        self,
        event: dict,
    ) -> None:
        context_handle = get_field(event, 'context_handle')
        timestamp = get_field(event, '_timestamp')
        pid = get_field(event, '_vpid')
        # version is defined within tracetools.
        # It is ignored because CARET does not plan to use it.
        # version = get_field(event, 'version')

        context_handle = self._remapper.context_handle_remapper.register_and_get_object_id(
            context_handle, event)
        self.data.add_context(pid, context_handle, timestamp)

    def _handle_rcl_node_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'node_handle')
        timestamp = get_field(event, '_timestamp')
        tid = get_field(event, '_vtid')
        rmw_handle = get_field(event, 'rmw_handle')
        name = get_field(event, 'node_name')
        namespace = get_field(event, 'namespace')

        handle = self._remapper.node_handle_remapper.register_and_get_object_id(handle, event)
        rmw_handle = \
            self._remapper.rmw_node_handle_remapper.register_and_get_object_id(rmw_handle, event)
        self.data.add_node(tid, handle, timestamp, rmw_handle, name, namespace)

    def _handle_rcl_publisher_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'publisher_handle')
        timestamp = get_field(event, '_timestamp')
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_publisher_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')

        handle = self._remapper.publisher_handle_remapper.register_and_get_object_id(handle, event)
        node_handle = self._remapper.node_handle_remapper.get_nearest_object_id(node_handle, event)
        rmw_handle = self._remapper.rmw_publisher_handle_remapper.register_and_get_object_id(
            rmw_handle, event)
        self.data.add_publisher(
            handle, timestamp, node_handle, rmw_handle, topic_name, depth)

    def _handle_rcl_subscription_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'subscription_handle')
        timestamp = get_field(event, '_timestamp')
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_subscription_handle')
        topic_name = get_field(event, 'topic_name')
        depth = get_field(event, 'queue_depth')

        handle = \
            self._remapper.subscription_handle_remapper.register_and_get_object_id(handle, event)
        node_handle = self._remapper.node_handle_remapper.get_nearest_object_id(node_handle, event)
        rmw_handle = self._remapper.rmw_subscription_handle_remapper.register_and_get_object_id(
            rmw_handle, event)
        self.data.add_rcl_subscription(
            handle,
            timestamp,
            node_handle,
            rmw_handle,
            topic_name,
            depth,
        )

    def _handle_rclcpp_buffer_to_ipb(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        buffer = get_field(event, 'buffer')
        ipb = get_field(event, 'ipb')

        buffer = self._remapper.buffer_remapper.get_nearest_object_id(buffer, event)
        ipb = self._remapper.ipb_remapper.register_and_get_object_id(ipb, event)

        self.data.add_buffer_to_ipb(
            timestamp,
            buffer,
            ipb
        )

    def _handle_rclcpp_ipb_to_subscription(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        ipb = get_field(event, 'ipb')
        subscription = get_field(event, 'subscription')

        ipb = self._remapper.ipb_remapper.get_nearest_object_id(ipb, event)
        subscription = self._remapper.callback_holder_id_remapper.get_nearest_object_id(
            subscription, event)

        self.data.add_ipb_to_subscription(
            timestamp,
            ipb,
            subscription,
        )

    def _handle_rclcpp_construct_ring_buffer(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        buffer = get_field(event, 'buffer')
        capacity = get_field(event, 'capacity')

        buffer = self._remapper.buffer_remapper.register_and_get_object_id(buffer, event)
        self.data.add_ring_buffer(
            timestamp,
            buffer,
            capacity,
        )

    def _handle_rclcpp_subscription_init(
        self,
        event: dict,
    ) -> None:
        subscription_pointer = get_field(event, 'subscription')
        timestamp = get_field(event, '_timestamp')
        handle = get_field(event, 'subscription_handle')

        handle = self._remapper.subscription_handle_remapper.get_nearest_object_id(
            handle, event)
        subscription_pointer = \
            self._remapper.callback_holder_id_remapper.register_and_get_object_id(
                subscription_pointer, event)
        self.data.add_rclcpp_subscription(
            subscription_pointer, timestamp, handle)

    def _handle_rclcpp_subscription_callback_added(
        self,
        event: dict,
    ) -> None:
        subscription_pointer = get_field(event, 'subscription')
        timestamp = get_field(event, '_timestamp')
        callback_object = get_field(event, 'callback')

        subscription_pointer = self._remapper.callback_holder_id_remapper.get_nearest_object_id(
            subscription_pointer, event)
        callback_object = \
            self._remapper.callback_remapper.register_and_get_object_id(callback_object, event)
        self.data.add_callback_object(
            subscription_pointer, timestamp, callback_object)

    def _handle_rcl_service_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'service_handle')
        timestamp = get_field(event, '_timestamp')
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_service_handle')
        service_name = get_field(event, 'service_name')

        handle = self._remapper.callback_holder_id_remapper.register_and_get_object_id(
            handle, event)
        node_handle = self._remapper.node_handle_remapper.get_nearest_object_id(
            node_handle, event)
        rmw_handle = \
            self._remapper.rmw_service_handle_remapper.register_and_get_object_id(
                rmw_handle, event)
        self.data.add_service(
            handle, timestamp, node_handle, rmw_handle, service_name)

    def _handle_rclcpp_service_callback_added(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'service_handle')
        timestamp = get_field(event, '_timestamp')
        callback_object = get_field(event, 'callback')

        handle = self._remapper.callback_holder_id_remapper.get_nearest_object_id(handle, event)
        callback_object = \
            self._remapper.callback_remapper.register_and_get_object_id(callback_object, event)
        self.data.add_callback_object(handle, timestamp, callback_object)

    def _handle_rcl_client_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'client_handle')
        timestamp = get_field(event, '_timestamp')
        node_handle = get_field(event, 'node_handle')
        rmw_handle = get_field(event, 'rmw_client_handle')
        service_name = get_field(event, 'service_name')

        handle = self._remapper.client_handle_remapper.register_and_get_object_id(handle, event)
        node_handle = self._remapper.node_handle_remapper.get_nearest_object_id(node_handle, event)
        rmw_handle = self._remapper.rmw_client_handle_remapper.register_and_get_object_id(
            rmw_handle, event)
        self.data.add_client(handle, timestamp, node_handle,
                             rmw_handle, service_name)

    def _handle_rcl_timer_init(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = get_field(event, '_timestamp')
        period = get_field(event, 'period')
        tid = get_field(event, '_vtid')

        handle = \
            self._remapper.callback_holder_id_remapper.register_and_get_object_id(handle, event)
        self.data.add_timer(tid, handle, timestamp, period)

    def _handle_caret_init(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        clock_offset = get_field(event, 'clock_offset')
        if 'distribution' in event.keys():
            distribution = get_field(event, 'distribution')
        else:
            distribution = 'NOTFOUND'
        self.data.add_caret_init(clock_offset, timestamp, distribution)  # type: ignore
        pid = get_field(event, '_vpid')
        assert isinstance(pid, int)
        self._caret_init_recorded[pid] = True

    @staticmethod
    def get_monotonic_to_system_offset(
        event: dict,
    ) -> int:
        timestamp = get_field(event, '_timestamp')
        clock_offset = get_field(event, 'clock_offset')
        return timestamp - clock_offset  # type: ignore

    def _handle_rclcpp_timer_callback_added(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = get_field(event, '_timestamp')
        callback_object = get_field(event, 'callback')

        handle = self._remapper.callback_holder_id_remapper.get_nearest_object_id(handle, event)
        callback_object = \
            self._remapper.callback_remapper.register_and_get_object_id(callback_object, event)
        self.data.add_callback_object(handle, timestamp, callback_object)

    def _handle_rclcpp_timer_link_node(
        self,
        event: dict,
    ) -> None:
        handle = get_field(event, 'timer_handle')
        timestamp = get_field(event, '_timestamp')
        node_handle = get_field(event, 'node_handle')

        handle = self._remapper.callback_holder_id_remapper.get_nearest_object_id(handle, event)
        node_handle = self._remapper.node_handle_remapper.get_nearest_object_id(node_handle, event)
        self.data.add_timer_node_link(handle, timestamp, node_handle)

    def _handle_rclcpp_callback_register(
        self,
        event: dict,
    ) -> None:
        callback_object = get_field(event, 'callback')
        timestamp = get_field(event, '_timestamp')
        symbol = get_field(event, 'symbol')

        callback_object = \
            self._remapper.callback_remapper.get_nearest_object_id(callback_object, event)
        self.data.add_callback_symbol(callback_object, timestamp, symbol)

    def _handle_callback_start(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        # Add to dict
        callback = get_field(event, 'callback')
        tid = get_field(event, '_vtid')
        timestamp = get_field(event, '_timestamp')
        is_intra_process = get_field(event, 'is_intra_process')

        callback = self._remapper.callback_remapper.get_latest_object_id(callback, event)
        self.data.add_callback_start_instance(
            tid, timestamp, callback, is_intra_process)

    def _handle_callback_end(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        # Fetch from dict
        callback = get_field(event, 'callback')
        tid = get_field(event, '_vtid')
        timestamp = get_field(event, '_timestamp')

        callback = self._remapper.callback_remapper.get_latest_object_id(callback, event)
        self.data.add_callback_end_instance(tid, timestamp, callback)

    def _handle_rcl_lifecycle_state_machine_init(
        self,
        event: dict,
    ) -> None:
        node_handle = get_field(event, 'node_handle')
        state_machine = get_field(event, 'state_machine')

        node_handle = \
            self._remapper.node_handle_remapper.get_nearest_object_id(node_handle, event)
        state_machine = \
            self._remapper.state_machine_remapper.register_and_get_object_id(state_machine, event)
        self.data.add_lifecycle_state_machine(state_machine, node_handle)

    def _handle_rcl_lifecycle_transition(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        state_machine = get_field(event, 'state_machine')
        start_label = get_field(event, 'start_label')
        goal_label = get_field(event, 'goal_label')

        state_machine = \
            self._remapper.state_machine_remapper.get_latest_object_id(state_machine, event)
        self.data.add_lifecycle_state_transition(
            state_machine, start_label, goal_label, timestamp)

    def _handle_rclcpp_publish(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return
        if 'publisher_handle' in event.keys():
            publisher_handle = get_field(event, 'publisher_handle')
            publisher_handle = \
                self._remapper.publisher_handle_remapper.get_latest_object_id(
                    publisher_handle, event)
        else:
            publisher_handle = 0
        timestamp = get_field(event, '_timestamp')
        message = get_field(event, 'message')
        tid = get_field(event, '_vtid')
        if 'message_timestamp' in event.keys():
            message_timestamp = get_field(event, 'message_timestamp')
        else:
            message_timestamp = 0

        self.data.add_rclcpp_publish_instance(
            tid, timestamp, publisher_handle, message, message_timestamp)

    def _handle_rcl_publish(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = get_field(event, '_timestamp')
        tid = get_field(event, '_vtid')
        message = get_field(event, 'message')

        publisher_handle = self._remapper.publisher_handle_remapper.get_latest_object_id(
            publisher_handle, event)
        self.data.add_rcl_publish_instance(
            tid, timestamp, publisher_handle, message)

    def _handle_message_construct(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        original_message = get_field(event, 'original_message')
        constructed_message = get_field(event, 'constructed_message')
        timestamp = get_field(event, '_timestamp')

        self.data.add_message_construct_instance(
            timestamp, original_message, constructed_message)

    def _handle_rclcpp_intra_publish(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        message = get_field(event, 'message')
        publisher_handle = get_field(event, 'publisher_handle')
        timestamp = get_field(event, '_timestamp')
        if 'message_timestamp' in event.keys():
            message_timestamp = get_field(event, 'message_timestamp')
        else:
            message_timestamp = 0
        tid = get_field(event, '_vtid')

        publisher_handle = self._remapper.publisher_handle_remapper.get_latest_object_id(
            publisher_handle, event)
        self.data.add_rclcpp_intra_publish_instance(
            tid, timestamp, publisher_handle, message, message_timestamp)

    def _handle_rclcpp_ring_buffer_enqueue(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        buffer = get_field(event, 'buffer')
        index = get_field(event, 'index')
        size = get_field(event, 'size')
        overwritten = get_field(event, 'overwritten')
        timestamp = get_field(event, '_timestamp')
        tid = get_field(event, '_vtid')

        buffer = self._remapper.buffer_remapper.get_latest_object_id(buffer, event)
        self.data.add_rclcpp_ring_buffer_enqueue_instance(
            tid, timestamp, buffer, index, size, overwritten)

    def _handle_rclcpp_ring_buffer_dequeue(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        buffer = get_field(event, 'buffer')
        index = get_field(event, 'index')
        size = get_field(event, 'size')
        timestamp = get_field(event, '_timestamp')
        tid = get_field(event, '_vtid')

        buffer = self._remapper.buffer_remapper.get_latest_object_id(buffer, event)
        self.data.add_rclcpp_ring_buffer_dequeue_instance(
            tid, timestamp, buffer, index, size)

    def _handle_dispatch_subscription_callback(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        callback_object = get_field(event, 'callback')
        message = get_field(event, 'message')
        timestamp = get_field(event, '_timestamp')
        source_stamp = get_field(event, 'source_stamp')
        message_timestamp = get_field(event, 'message_timestamp')

        callback_object = \
            self._remapper.callback_remapper.get_latest_object_id(callback_object, event)
        self.data.add_dispatch_subscription_callback_instance(
            timestamp, callback_object, message, source_stamp, message_timestamp
        )

    def _handle_rmw_take(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        tid = get_field(event, '_vtid')
        timestamp = get_field(event, '_timestamp')
        rmw_subscription_handle = get_field(event, 'rmw_subscription_handle')
        message = get_field(event, 'message')
        source_stamp = get_field(event, 'source_timestamp')

        rmw_subscription_handle = \
            self._remapper.rmw_subscription_handle_remapper.get_latest_object_id(
                rmw_subscription_handle, event)
        self.data.add_rmw_take_instance(
            tid, timestamp, rmw_subscription_handle, message, source_stamp
        )

    def _handle_dispatch_intra_process_subscription_callback(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        callback_object = get_field(event, 'callback')
        message = get_field(event, 'message')
        timestamp = get_field(event, '_timestamp')
        message_timestamp = get_field(event, 'message_timestamp')

        callback_object = \
            self._remapper.callback_remapper.get_latest_object_id(callback_object, event)
        self.data.add_dispatch_intra_process_subscription_callback_instance(
            timestamp, callback_object, message, message_timestamp
        )

    def _handle_on_data_available(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        source_stamp = get_field(event, 'source_stamp')
        self.data.add_on_data_available_instance(timestamp, source_stamp)

    def _handle_dds_write(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        message = get_field(event, 'message')
        tid = get_field(event, '_vtid')
        # memo: "timestamp" is not read because alternative data is used
        # memo: "timestamp" and "_timestamp" are different
        # memo: "rmw_publisher_handle" is not used so will not be loaded

        self.data.add_dds_write_instance(tid, timestamp, message)

    def _handle_dds_bind_addr_to_stamp(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        addr = get_field(event, 'addr')
        tid = get_field(event, '_vtid')
        source_stamp = get_field(event, 'source_stamp')

        self.data.add_dds_bind_addr_to_stamp(tid, timestamp, addr, source_stamp)

    def _handle_dds_bind_addr_to_addr(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        addr_from = get_field(event, 'addr_from')
        addr_to = get_field(event, 'addr_to')

        self.data.add_dds_bind_addr_to_addr(timestamp, addr_from, addr_to)

    def _handle_rmw_implementation(
        self,
        event: dict,
    ) -> None:
        rmw_impl = get_field(event, 'rmw_impl')
        self.data.add_rmw_implementation(rmw_impl)

    def _handle_callback_group_to_executor_entity_collector(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        collector_addr = get_field(event, 'entities_collector_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')

        collector_addr = \
            self._remapper.entities_collector_addr_remapper.get_nearest_object_id(
                collector_addr, event)
        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.register_and_get_object_id(
                callback_group_addr, event)
        self.data.add_callback_group_to_executor_entity_collector(
            collector_addr, callback_group_addr, group_type_name, timestamp)

    def _handle_executor_entity_collector_to_executor(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        executor_addr = get_field(event, 'executor_addr')
        collector_addr = get_field(event, 'entities_collector_addr')

        executor_addr = self._remapper.executor_addr_remapper.register_and_get_object_id(
            executor_addr, event)
        collector_addr = \
            self._remapper.entities_collector_addr_remapper.register_and_get_object_id(
                collector_addr, event)
        self.data.add_executor_entity_collector_to_executor(
            executor_addr, collector_addr, timestamp)

    def _handle_construct_executor(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        executor_addr = get_field(event, 'executor_addr')
        executor_type_name = get_field(event, 'executor_type_name')

        distribution = self._get_distribution(self.data)
        if distribution[0] >= 'jazzy'[0]:
            executor_addr = self._remapper.executor_addr_remapper.get_nearest_object_id(
                executor_addr, event)
        else:
            executor_addr = self._remapper.executor_addr_remapper.register_and_get_object_id(
                executor_addr, event)

        self.data.add_executor(executor_addr, timestamp, executor_type_name)

    def _handle_construct_static_executor(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        executor_addr = get_field(event, 'executor_addr')
        collector_addr = get_field(event, 'entities_collector_addr')
        executor_type_name = get_field(event, 'executor_type_name')

        distribution = self._get_distribution(self.data)
        if distribution[0] >= 'jazzy'[0]:
            executor_addr = self._remapper.executor_addr_remapper.get_nearest_object_id(
                executor_addr, event)
            collector_addr = \
                self._remapper.entities_collector_addr_remapper.get_nearest_object_id(
                    collector_addr, event)
        else:
            executor_addr = self._remapper.executor_addr_remapper.register_and_get_object_id(
                executor_addr, event)
            collector_addr = \
                self._remapper.entities_collector_addr_remapper.register_and_get_object_id(
                    collector_addr, event)
        self.data.add_executor_static(
            executor_addr, collector_addr, timestamp, executor_type_name)

    def _handle_add_callback_group(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        executor_addr = get_field(event, 'executor_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')

        executor_addr = \
            self._remapper.executor_addr_remapper.get_nearest_object_id(executor_addr, event)
        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.register_and_get_object_id(
                callback_group_addr, event)
        self.data.add_callback_group(
            executor_addr, timestamp, callback_group_addr, group_type_name)

    def _handle_add_callback_group_static_executor(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        collector_addr = get_field(event, 'entities_collector_addr')
        callback_group_addr = get_field(event, 'callback_group_addr')
        group_type_name = get_field(event, 'group_type_name')

        collector_addr = \
            self._remapper.entities_collector_addr_remapper.get_nearest_object_id(
                collector_addr, event)
        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.register_and_get_object_id(
                callback_group_addr, event)
        self.data.add_callback_group_static_executor(
            collector_addr, timestamp, callback_group_addr, group_type_name)

    def _handle_callback_group_add_timer(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        callback_group_addr = get_field(event, 'callback_group_addr')
        timer_handle = get_field(event, 'timer_handle')

        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.get_nearest_object_id(
                callback_group_addr, event)
        timer_handle = \
            self._remapper.callback_holder_id_remapper.get_nearest_object_id(timer_handle, event)
        self.data.callback_group_add_timer(callback_group_addr, timestamp, timer_handle)

    def _handle_callback_group_add_subscription(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        callback_group_addr = get_field(event, 'callback_group_addr')
        subscription_handle = get_field(event, 'subscription_handle')

        callback_group_addr = self._remapper.callback_group_addr_remapper.get_nearest_object_id(
            callback_group_addr, event)
        subscription_handle = self._remapper.subscription_handle_remapper.get_nearest_object_id(
            subscription_handle, event)
        self.data.callback_group_add_subscription(
            callback_group_addr, timestamp, subscription_handle)

    def _handle_callback_group_add_service(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        callback_group_addr = get_field(event, 'callback_group_addr')
        service_handle = get_field(event, 'service_handle')

        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.get_nearest_object_id(
                callback_group_addr, event)
        service_handle = \
            self._remapper.callback_holder_id_remapper.get_nearest_object_id(service_handle, event)
        self.data.callback_group_add_service(callback_group_addr, timestamp, service_handle)

    def _handle_callback_group_add_client(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        callback_group_addr = get_field(event, 'callback_group_addr')
        client_handle = get_field(event, 'client_handle')

        callback_group_addr = \
            self._remapper.callback_group_addr_remapper.get_nearest_object_id(
                callback_group_addr, event)
        client_handle = \
            self._remapper.client_handle_remapper.get_nearest_object_id(client_handle, event)
        self.data.callback_group_add_client(callback_group_addr, timestamp, client_handle)

    def _handle_tilde_subscription_init(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        subscription = get_field(event, 'subscription')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscription(subscription, node_name, topic_name, timestamp)

    def _handle_tilde_publisher_init(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        publisher = get_field(event, 'publisher')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_publisher(publisher, node_name, topic_name, timestamp)

    def _handle_tilde_subscribe(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

        timestamp = get_field(event, '_timestamp')
        subscription = get_field(event, 'subscription')
        tilde_message_id = get_field(event, 'tilde_message_id')
        self.data.add_tilde_subscribe(timestamp, subscription, tilde_message_id)

    def _handle_tilde_publish(
        self,
        event: dict,
    ) -> None:
        if not self._is_valid_data(event):
            return

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
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        subscription_id = get_field(event, 'subscription_id')
        node_name = get_field(event, 'node_name')
        topic_name = get_field(event, 'topic_name')
        self.data.add_tilde_subscribe_added(subscription_id, node_name, topic_name, timestamp)

    def _handle_sim_time(
        self,
        event: dict,
    ) -> None:
        timestamp = get_field(event, '_timestamp')
        sim_time = get_field(event, 'stamp')
        self.data.add_sim_time(timestamp, sim_time)
