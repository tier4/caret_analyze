# Copyright 2019 Robert Bosch GmbH
# Copyright 2020-2021 Christophe Bedard
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

"""Module for ROS 2 data model."""


from ...trace_point_data import TracePointIntermediateData
from ....record import ColumnValue, RecordsFactory


class Ros2DataModel():
    """
    Container to model pre-processed ROS 2 data for analysis.

    This aims to represent the data in a ROS 2-aware way.
    """

    def __init__(self) -> None:
        """Create a Ros2DataModel."""
        # Objects (one-time events, usually when something is created)
        self._contexts = TracePointIntermediateData(
            ['context_handle', 'timestamp', 'pid'])
        self._nodes = TracePointIntermediateData(
            ['node_handle', 'timestamp', 'tid', 'rmw_handle', 'namespace', 'name'])
        self._publishers = TracePointIntermediateData(
            ['publisher_handle', 'timestamp', 'node_handle', 'rmw_handle', 'topic_name', 'depth'])
        self._subscriptions = TracePointIntermediateData(
            ['subscription_handle', 'timestamp', 'node_handle',
             'rmw_handle', 'topic_name', 'depth'])
        self._subscription_objects = TracePointIntermediateData(
            ['subscription', 'timestamp', 'subscription_handle'])
        self._services = TracePointIntermediateData(
            ['service_handle', 'timestamp', 'node_handle', 'rmw_handle', 'service_name'])
        self._clients = TracePointIntermediateData(
            ['client_handle', 'timestamp', 'node_handle', 'rmw_handle', 'service_name'])
        self._timers = TracePointIntermediateData(
            ['timer_handle', 'timestamp', 'period', 'tid'])
        self._caret_init = TracePointIntermediateData(
            ['timestamp', 'clock_offset', 'distribution'])
        self._timer_node_links = TracePointIntermediateData(
            ['timer_handle', 'timestamp', 'node_handle'])
        self._callback_objects = TracePointIntermediateData(
            ['reference', 'timestamp', 'callback_object'])
        self._callback_symbols = TracePointIntermediateData(
            ['callback_object', 'timestamp', 'symbol'])
        self._lifecycle_state_machines = TracePointIntermediateData(
            ['state_machine_handle', 'node_handle'])

        self._callback_group_to_executor_entity_collector = TracePointIntermediateData(
            ['timestamp', 'entities_collector_addr',
             'callback_group_addr', 'group_type_name'])
        self._executor_entity_collector_to_executor = TracePointIntermediateData(
            ['timestamp', 'executor_addr', 'entities_collector_addr'])

        self._executors = TracePointIntermediateData(
            ['timestamp', 'executor_addr', 'executor_type_name'])
        self._executors_static = TracePointIntermediateData(
            ['timestamp', 'executor_addr', 'entities_collector_addr', 'executor_type_name'])
        self._callback_groups = TracePointIntermediateData(
            ['timestamp', 'executor_addr', 'callback_group_addr', 'group_type_name'])
        self._callback_groups_static = TracePointIntermediateData(
            ['timestamp', 'entities_collector_addr', 'callback_group_addr', 'group_type_name'])
        self._callback_group_timer = TracePointIntermediateData(
            ['timestamp', 'callback_group_addr', 'timer_handle'])
        self._callback_group_subscription = TracePointIntermediateData(
            ['timestamp', 'callback_group_addr', 'subscription_handle'])
        self._callback_group_service = TracePointIntermediateData(
            ['timestamp', 'callback_group_addr', 'service_handle'])
        self._callback_group_client = TracePointIntermediateData(
            ['timestamp', 'callback_group_addr', 'client_handle'])
        self._buffer_to_ipbs = TracePointIntermediateData(
            ['timestamp', 'buffer', 'ipb'])
        self._ipb_to_subscriptions = TracePointIntermediateData(
            ['timestamp', 'ipb', 'subscription'])
        self._ring_buffers = TracePointIntermediateData(
            ['timestamp', 'buffer', 'capacity'])
        self._rmw_impl = TracePointIntermediateData(
            ['rmw_impl'])

        self._tilde_subscriptions = TracePointIntermediateData(
            ['subscription', 'node_name', 'topic_name', 'timestamp'])
        self._tilde_publishers = TracePointIntermediateData(
            ['publisher', 'node_name', 'topic_name', 'timestamp'])
        self._tilde_subscribe_added = TracePointIntermediateData(
            ['subscription_id', 'node_name', 'topic_name', 'timestamp'])

        # Events (multiple instances, may not have a meaningful index)
        # string argument
        self._lifecycle_transitions = TracePointIntermediateData(
            ['state_machine_handle', 'start_label', 'goal_label', 'timestamp'])

        # Events (multiple instances, may not have a meaningful index)
        self.callback_start_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('callback_start_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process'),
            ]
        )
        self.callback_end_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('callback_end_timestamp'),
                ColumnValue('callback_object'),
            ]
        )
        self.dds_write_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('dds_write_timestamp'),
                ColumnValue('message'),
            ]
        )
        self.dds_bind_addr_to_stamp = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('dds_bind_addr_to_stamp_timestamp'),
                ColumnValue('addr'),
                ColumnValue('source_timestamp'),
            ]
        )
        self.dds_bind_addr_to_addr = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('dds_bind_addr_to_addr_timestamp'),
                ColumnValue('addr_from'),
                ColumnValue('addr_to'),
            ]
        )
        self.on_data_available_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('on_data_available_timestamp'),
                ColumnValue('source_timestamp'),
            ]
        )
        self.rclcpp_intra_publish_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rclcpp_intra_publish_timestamp'),
                ColumnValue('publisher_handle'),
                ColumnValue('message'),
                ColumnValue('message_timestamp'),
            ]
        )
        self.rclcpp_ring_buffer_enqueue_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rclcpp_ring_buffer_enqueue_timestamp'),
                ColumnValue('buffer'),
                ColumnValue('index'),
                ColumnValue('size'),
                ColumnValue('overwritten'),
            ]
        )
        self.rclcpp_ring_buffer_dequeue_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rclcpp_ring_buffer_dequeue_timestamp'),
                ColumnValue('buffer'),
                ColumnValue('index'),
                ColumnValue('size'),
            ]
        )
        self.rclcpp_publish_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rclcpp_publish_timestamp'),
                ColumnValue('publisher_handle'),
                ColumnValue('message'),
                ColumnValue('message_timestamp'),
            ]
        )
        self.rcl_publish_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rcl_publish_timestamp'),
                ColumnValue('publisher_handle'),
                ColumnValue('message'),
            ]
        )
        self.dispatch_subscription_callback_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('dispatch_subscription_callback_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('message'),
                ColumnValue('source_timestamp'),
                ColumnValue('message_timestamp'),
            ]
        )
        self.rmw_take_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('rmw_take_timestamp'),
                ColumnValue('rmw_subscription_handle'),
                ColumnValue('message'),
                ColumnValue('source_timestamp')
            ]
        )
        self.dispatch_intra_process_subscription_callback_instances = \
            RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue('dispatch_intra_process_subscription_callback_timestamp'),
                    ColumnValue('callback_object'),
                    ColumnValue('message'),
                    ColumnValue('message_timestamp'),
                ]
            )
        self.message_construct_instances = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('message_construct_timestamp'),
                ColumnValue('original_message'),
                ColumnValue('constructed_message'),
            ]
        )
        self.tilde_subscribe = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tilde_subscribe_timestamp'),
                ColumnValue('subscription'),
                ColumnValue('tilde_message_id'),
            ]
        )
        self.tilde_publish = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tilde_publish_timestamp'),
                ColumnValue('publisher'),
                ColumnValue('subscription_id'),
                ColumnValue('tilde_message_id'),
            ]
        )
        self.sim_time = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('system_time'),
                ColumnValue('sim_time'),
            ]
        )
        self.timer_event = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('time_event_stamp'),
            ]
        )

    def add_context(self, pid, context_handle, timestamp) -> None:
        record = {
            'context_handle': context_handle,
            'timestamp': timestamp,
            'pid': pid,
        }
        self._contexts.append(record)

    def add_node(self, tid, node_handle, timestamp, rmw_handle, name, namespace) -> None:
        record = {
            'node_handle': node_handle,
            'timestamp': timestamp,
            'tid': tid,
            'rmw_handle': rmw_handle,
            'namespace': namespace,
            'name': name,
        }
        self._nodes.append(record)

    def add_publisher(self, handle, timestamp, node_handle, rmw_handle, topic_name, depth) -> None:
        record = {
            'publisher_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._publishers.append(record)

    def add_rcl_subscription(
        self, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
        record = {
            'subscription_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._subscriptions.append(record)

    def add_rclcpp_subscription(
        self, subscription_pointer, timestamp, subscription_handle
    ) -> None:
        record = {
            'subscription': subscription_pointer,
            'timestamp': timestamp,
            'subscription_handle': subscription_handle,
        }
        self._subscription_objects.append(record)

    def add_service(self, handle, timestamp, node_handle, rmw_handle, service_name) -> None:
        record = {
            'service_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'service_name': service_name,
        }
        self._services.append(record)

    def add_client(self, handle, timestamp, node_handle, rmw_handle, service_name) -> None:
        record = {
            'client_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'service_name': service_name,
        }
        self._clients.append(record)

    def add_timer(self, tid, handle, timestamp, period) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'period': period,
            'tid': tid,
        }
        self._timers.append(record)

    def add_tilde_subscribe_added(
        self, subscription_id, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'subscription_id': subscription_id,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp
        }
        self._tilde_subscribe_added.append(record)

    def add_timer_node_link(self, handle, timestamp, node_handle) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
        }
        self._timer_node_links.append(record)

    def add_callback_object(self, reference, timestamp, callback_object) -> None:
        record = {
            'reference': reference,
            'timestamp': timestamp,
            'callback_object': callback_object,
        }
        self._callback_objects.append(record)

    def add_callback_symbol(self, callback_object, timestamp, symbol) -> None:
        record = {
            'callback_object': callback_object,
            'timestamp': timestamp,
            'symbol': symbol,
        }
        self._callback_symbols.append(record)

    def add_lifecycle_state_machine(self, handle, node_handle) -> None:
        record = {
            'state_machine_handle': handle,
            'node_handle': node_handle,
        }
        self._lifecycle_state_machines.append(record)

    def add_lifecycle_state_transition(
        self, state_machine_handle, start_label, goal_label, timestamp
    ) -> None:
        record = {
            'state_machine_handle': state_machine_handle,
            'start_label': start_label,
            'goal_label': goal_label,
            'timestamp': timestamp,
        }
        self._lifecycle_transitions.append(record)

    def add_tilde_subscription(
        self, subscription, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'subscription': subscription,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
        }
        self._tilde_subscriptions.append(record)

    def add_tilde_publisher(
        self, publisher, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'publisher': publisher,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
        }
        self._tilde_publishers.append(record)

    def add_callback_start_instance(
        self, tid: int, timestamp: int, callback: int, is_intra_process: bool
    ) -> None:
        record = {
            'tid': tid,
            'callback_start_timestamp': timestamp,
            'callback_object': callback,
            'is_intra_process': is_intra_process,
            }
        self.callback_start_instances.append(record)

    def add_callback_end_instance(self, tid: int, timestamp: int, callback: int) -> None:
        record = {
            'tid': tid,
            'callback_end_timestamp': timestamp,
            'callback_object': callback
        }
        self.callback_end_instances.append(record)

    def add_rclcpp_intra_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = {
            'tid': tid,
            'rclcpp_intra_publish_timestamp': timestamp,
            'publisher_handle': publisher_handle,
            'message': message,
            'message_timestamp': message_timestamp,
        }
        self.rclcpp_intra_publish_instances.append(record)

    def add_rclcpp_ring_buffer_enqueue_instance(
        self,
        tid: int,
        timestamp: int,
        buffer: int,
        index: int,
        size: int,
        overwritten: bool,
    ) -> None:
        record = {
            'tid': tid,
            'rclcpp_ring_buffer_enqueue_timestamp': timestamp,
            'buffer': buffer,
            'index': index,
            'size': size,
            'overwritten': overwritten,
        }
        self.rclcpp_ring_buffer_enqueue_instances.append(record)

    def add_rclcpp_ring_buffer_dequeue_instance(
        self,
        tid: int,
        timestamp: int,
        buffer: int,
        index: int,
        size: int,
    ) -> None:
        record = {
            'tid': tid,
            'rclcpp_ring_buffer_dequeue_timestamp': timestamp,
            'buffer': buffer,
            'index': index,
            'size': size,
        }
        self.rclcpp_ring_buffer_dequeue_instances.append(record)

    def add_rclcpp_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = {
            'tid': tid,
            'rclcpp_publish_timestamp': timestamp,
            'publisher_handle': publisher_handle,
            'message': message,
            'message_timestamp': message_timestamp,
        }
        self.rclcpp_publish_instances.append(record)

    def add_rcl_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = {
            'tid': tid,
            'rcl_publish_timestamp': timestamp,
            'publisher_handle': publisher_handle,
            'message': message,
        }
        self.rcl_publish_instances.append(record)

    def add_dds_write_instance(
        self,
        tid: int,
        timestamp: int,
        message: int,
    ) -> None:
        record = {
            'tid': tid,
            'dds_write_timestamp': timestamp,
            'message': message,
        }
        self.dds_write_instances.append(record)

    def add_dds_bind_addr_to_addr(
        self,
        timestamp: int,
        addr_from: int,
        addr_to: int,
    ) -> None:
        record = {
            'dds_bind_addr_to_addr_timestamp': timestamp,
            'addr_from': addr_from,
            'addr_to': addr_to,
        }
        self.dds_bind_addr_to_addr.append(record)

    def add_dds_bind_addr_to_stamp(
        self,
        tid: int,
        timestamp: int,
        addr: int,
        source_timestamp: int,
    ) -> None:
        record = {
            'tid': tid,
            'dds_bind_addr_to_stamp_timestamp': timestamp,
            'addr': addr,
            'source_timestamp': source_timestamp,
        }
        self.dds_bind_addr_to_stamp.append(record)

    def add_on_data_available_instance(
        self,
        timestamp: int,
        source_timestamp: int,
    ) -> None:
        record = {
            'on_data_available_timestamp': timestamp,
            'source_timestamp': source_timestamp,
        }
        self.on_data_available_instances.append(record)

    def add_message_construct_instance(
        self, timestamp: int, original_message: int, constructed_message: int
    ) -> None:
        record = {
            'message_construct_timestamp': timestamp,
            'original_message': original_message,
            'constructed_message': constructed_message,
        }
        self.message_construct_instances.append(record)

    def add_dispatch_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
        source_timestamp: int,
        message_timestamp: int,
    ) -> None:
        record = {
            'dispatch_subscription_callback_timestamp': timestamp,
            'callback_object': callback_object,
            'message': message,
            'source_timestamp': source_timestamp,
            'message_timestamp': message_timestamp,
        }
        self.dispatch_subscription_callback_instances.append(record)

    def add_rmw_take_instance(
        self,
        tid: int,
        timestamp: int,
        rmw_subscription_handle: int,
        message: int,
        source_timestamp: int
    ) -> None:
        record = {
            'tid': tid,
            'rmw_take_timestamp': timestamp,
            'rmw_subscription_handle': rmw_subscription_handle,
            'message': message,
            'source_timestamp': source_timestamp
        }
        self.rmw_take_instances.append(record)

    def add_sim_time(
        self,
        timestamp: int,
        sim_time: int
    ) -> None:
        record = {
            'system_time': timestamp,
            'sim_time': sim_time
        }
        self.sim_time.append(record)

    def add_buffer_to_ipb(
        self, timestamp, buffer, ipb
    ) -> None:
        record = {
            'timestamp': timestamp,
            'buffer': buffer,
            'ipb': ipb,
        }
        self._buffer_to_ipbs.append(record)

    def add_ipb_to_subscription(
        self, timestamp, ipb, subscription
    ) -> None:
        record = {
            'timestamp': timestamp,
            'ipb': ipb,
            'subscription': subscription,
        }
        self._ipb_to_subscriptions.append(record)

    def add_ring_buffer(
        self, timestamp, buffer, capacity
    ) -> None:
        record = {
            'timestamp': timestamp,
            'buffer': buffer,
            'capacity': capacity,
        }
        self._ring_buffers.append(record)

    def add_rmw_implementation(self, rmw_impl: str):
        record = {'rmw_impl': rmw_impl}
        self._rmw_impl.append(record)

    def add_dispatch_intra_process_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = {
            'dispatch_intra_process_subscription_callback_timestamp': timestamp,
            'callback_object': callback_object,
            'message': message,
            'message_timestamp': message_timestamp
        }
        self.dispatch_intra_process_subscription_callback_instances.append(
            record)

    def add_tilde_subscribe(
        self,
        timestamp: int,
        subscription: int,
        tilde_message_id: int,
    ) -> None:
        record = {
            'tilde_subscribe_timestamp': timestamp,
            'subscription': subscription,
            'tilde_message_id': tilde_message_id
        }
        self.tilde_subscribe.append(record)

    def add_tilde_publish(
        self,
        timestamp: int,
        publisher: int,
        subscription_id: int,
        tilde_message_id: int,
    ) -> None:
        record = {
            'tilde_publish_timestamp': timestamp,
            'publisher': publisher,
            'subscription_id': subscription_id,
            'tilde_message_id': tilde_message_id,
        }
        self.tilde_publish.append(record)

    def add_callback_group_to_executor_entity_collector(
        self,
        entities_collector_addr: int,
        callback_group_addr: int,
        group_type_name: str,
        timestamp: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'entities_collector_addr': entities_collector_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name,
        }
        self._callback_group_to_executor_entity_collector.append(record)

    def add_executor_entity_collector_to_executor(
        self,
        executor_addr: int,
        entities_collector_addr: int,
        timestamp: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'entities_collector_addr': entities_collector_addr,
        }
        self._executor_entity_collector_to_executor.append(record)

    def add_executor(
        self,
        executor_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        record = {
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'executor_type_name': executor_type_name,
        }
        self._executors.append(record)

    def add_executor_static(
        self,
        executor_addr: int,
        entities_collector_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        record = {
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'entities_collector_addr': entities_collector_addr,
            'executor_type_name': executor_type_name,
        }
        self._executors_static.append(record)

    def add_callback_group(
        self,
        executor_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        record = {
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        }
        self._callback_groups.append(record)

    def add_callback_group_static_executor(
        self,
        entities_collector_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        record = {
            'timestamp': timestamp,
            'entities_collector_addr': entities_collector_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        }
        self._callback_groups_static.append(record)

    def callback_group_add_timer(
        self,
        callback_group_addr: int,
        timestamp: int,
        timer_handle: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'timer_handle': timer_handle,
        }
        self._callback_group_timer.append(record)

    def callback_group_add_subscription(
        self,
        callback_group_addr: int,
        timestamp: int,
        subscription_handle: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'subscription_handle': subscription_handle,
        }
        self._callback_group_subscription.append(record)

    def callback_group_add_service(
        self,
        callback_group_addr: int,
        timestamp: int,
        service_handle: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'service_handle': service_handle,
        }
        self._callback_group_service.append(record)

    def callback_group_add_client(
        self,
        callback_group_addr: int,
        timestamp: int,
        client_handle: int
    ) -> None:
        record = {
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'client_handle': client_handle,
        }
        self._callback_group_client.append(record)

    def add_caret_init(
        self,
        clock_offset: int,
        timestamp: int,
        distribution: str,
    ) -> None:
        record = {
            'timestamp': timestamp,
            'clock_offset': clock_offset,
            'distribution': distribution,
        }
        self._caret_init.append(record)

    def finalize(self) -> None:
        self.contexts = self._contexts.get_finalized('context_handle')
        del self._contexts

        self.caret_init = self._caret_init.get_finalized()
        del self._caret_init

        self.nodes = self._nodes.get_finalized('node_handle')
        del self._nodes

        self.publishers = self._publishers.get_finalized('publisher_handle')
        del self._publishers

        self.subscriptions = self._subscriptions.get_finalized('subscription_handle')
        del self._subscriptions

        self.subscription_objects = self._subscription_objects.get_finalized('subscription')
        del self._subscription_objects

        self.services = self._services.get_finalized('service_handle')
        del self._services

        self.clients = self._clients.get_finalized('client_handle')
        del self._clients

        self.timers = self._timers.get_finalized('timer_handle')
        del self._timers

        self.timer_node_links = self._timer_node_links.get_finalized('timer_handle')
        del self._timer_node_links

        self.callback_objects = self._callback_objects.get_finalized('reference')
        del self._callback_objects

        self.callback_symbols = self._callback_symbols.get_finalized('callback_object')
        del self._callback_symbols

        self.lifecycle_state_machines = \
            self._lifecycle_state_machines.get_finalized('state_machine_handle')
        del self._lifecycle_state_machines

        self.lifecycle_transitions = self._lifecycle_transitions.get_finalized()
        del self._lifecycle_transitions

        self.callback_group_to_executor_entity_collector = \
            self._callback_group_to_executor_entity_collector.get_finalized(
                'entities_collector_addr')
        del self._callback_group_to_executor_entity_collector

        self.executor_entity_collector_to_executor = \
            self._executor_entity_collector_to_executor.get_finalized('executor_addr')
        del self._executor_entity_collector_to_executor

        self.executors = self._executors.get_finalized('executor_addr')
        del self._executors

        self.executors_static = self._executors_static.get_finalized('executor_addr')
        del self._executors_static

        self.callback_groups = self._callback_groups.get_finalized('callback_group_addr')
        del self._callback_groups

        self.callback_groups_static = \
            self._callback_groups_static.get_finalized('callback_group_addr')
        del self._callback_groups_static

        self.callback_group_timer = self._callback_group_timer.get_finalized('callback_group_addr')
        del self._callback_group_timer

        self.callback_group_subscription = \
            self._callback_group_subscription.get_finalized('callback_group_addr')
        del self._callback_group_subscription

        self.callback_group_service = \
            self._callback_group_service.get_finalized('callback_group_addr')
        del self._callback_group_service

        self.callback_group_client = \
            self._callback_group_client.get_finalized('callback_group_addr')
        del self._callback_group_client

        self.tilde_subscriptions = self._tilde_subscriptions.get_finalized('subscription')
        del self._tilde_subscriptions

        self.tilde_publishers = self._tilde_publishers.get_finalized('publisher')
        del self._tilde_publishers

        self.tilde_subscribe_added = self._tilde_subscribe_added.get_finalized('subscription_id')
        del self._tilde_subscribe_added

        self.buffer_to_ipbs = self._buffer_to_ipbs.get_finalized('buffer')
        del self._buffer_to_ipbs

        self.ipb_to_subscriptions = self._ipb_to_subscriptions.get_finalized('ipb')
        del self._ipb_to_subscriptions

        self.ring_buffers = self._ring_buffers.get_finalized('buffer')
        del self._ring_buffers

        self.rmw_impl = self._rmw_impl.get_finalized()
        del self._rmw_impl
