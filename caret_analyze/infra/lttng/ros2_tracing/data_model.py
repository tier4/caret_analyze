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

from caret_analyze.record.record_factory import RecordFactory, RecordsFactory
import pandas as pd

from tracetools_analysis.data_model import (DataModel,
                                            DataModelIntermediateStorage)


class Ros2DataModel(DataModel):
    """
    Container to model pre-processed ROS 2 data for analysis.

    This aims to represent the data in a ROS 2-aware way.
    """

    def __init__(self) -> None:
        """Create a Ros2DataModel."""
        super().__init__()
        # Objects (one-time events, usually when something is created)
        self._contexts: DataModelIntermediateStorage = []
        self._nodes: DataModelIntermediateStorage = []
        self._publishers: DataModelIntermediateStorage = []
        self._subscriptions: DataModelIntermediateStorage = []
        self._subscription_objects: DataModelIntermediateStorage = []
        self._services: DataModelIntermediateStorage = []
        self._clients: DataModelIntermediateStorage = []
        self._timers: DataModelIntermediateStorage = []
        self._timer_node_links: DataModelIntermediateStorage = []
        self._callback_objects: DataModelIntermediateStorage = []
        self._callback_symbols: DataModelIntermediateStorage = []
        self._lifecycle_state_machines: DataModelIntermediateStorage = []
        self._executors: DataModelIntermediateStorage = []
        self._executors_static: DataModelIntermediateStorage = []
        self._callback_groups: DataModelIntermediateStorage = []
        self._callback_groups_static: DataModelIntermediateStorage = []
        self._callback_group_timer: DataModelIntermediateStorage = []
        self._callback_group_subscription: DataModelIntermediateStorage = []
        self._callback_group_service: DataModelIntermediateStorage = []
        self._callback_group_client: DataModelIntermediateStorage = []
        self._rmw_impl: DataModelIntermediateStorage = []

        self._tilde_subscriptions: DataModelIntermediateStorage = []
        self._tilde_publishers: DataModelIntermediateStorage = []
        self._tilde_subscribe_added: DataModelIntermediateStorage = []

        # Events (multiple instances, may not have a meaningful index)
        # string argument
        self._lifecycle_transitions: DataModelIntermediateStorage = []

        # Events (multiple instances, may not have a meaningful index)
        self.callback_start_instances = RecordsFactory.create_instance(
            None, ['callback_start_timestamp', 'callback_object', 'is_intra_process']
        )
        self.callback_end_instances = RecordsFactory.create_instance(
            None, ['callback_end_timestamp', 'callback_object']
        )
        self.dds_write_instances = RecordsFactory.create_instance(
            None, ['tid', 'dds_write_timestamp', 'message']
        )
        self.dds_bind_addr_to_stamp = RecordsFactory.create_instance(
            None, ['tid', 'dds_bind_addr_to_stamp_timestamp', 'addr', 'source_timestamp']
        )
        self.dds_bind_addr_to_addr = RecordsFactory.create_instance(
            None, ['dds_bind_addr_to_addr_timestamp', 'addr_from', 'addr_to']
        )
        self.on_data_available_instances = RecordsFactory.create_instance(
            None, ['on_data_available_timestamp', 'source_timestamp']
        )
        self.rclcpp_intra_publish_instances = RecordsFactory.create_instance(
            None, ['tid', 'rclcpp_intra_publish_timestamp', 'publisher_handle',
                   'message', 'message_timestamp']
        )
        self.rclcpp_publish_instances = RecordsFactory.create_instance(
            None, [
                'tid', 'rclcpp_publish_timestamp', 'publisher_handle',
                'message', 'message_timestamp'
            ]
        )
        self.rcl_publish_instances = RecordsFactory.create_instance(
            None, ['tid', 'rcl_publish_timestamp', 'publisher_handle', 'message']
        )
        self.dispatch_subscription_callback_instances = RecordsFactory.create_instance(
            None, ['dispatch_subscription_callback_timestamp', 'callback_object', 'message',
                   'source_timestamp', 'message_timestamp'])
        self.dispatch_intra_process_subscription_callback_instances = \
            RecordsFactory.create_instance(
                None,
                ['dispatch_intra_process_subscription_callback_timestamp', 'callback_object',
                 'message', 'message_timestamp']
            )
        self.message_construct_instances = RecordsFactory.create_instance(
            None, ['message_construct_timestamp', 'original_message', 'constructed_message']
        )

        self.tilde_subscribe = RecordsFactory.create_instance(
            None, [
                'tilde_subscribe_timestamp',
                'subscription',
                'tilde_message_id']
        )

        self.tilde_publish = RecordsFactory.create_instance(
            None, [
                'tilde_publish_timestamp',
                'publisher',
                'subscription_id',
                'tilde_message_id']
        )
        self.sim_time = RecordsFactory.create_instance(
            None, [
                'system_time',
                'sim_time']
        )
        self.timer_event = RecordsFactory.create_instance(
            None, [
                'time_event_stamp']
        )

    def add_context(self, context_handle, timestamp, pid, version) -> None:
        record = {
            'context_handle': context_handle,
            'timestamp': timestamp,
            'pid': pid,
            'version': version,  # Comment out to align with Dict[str: int64_t]
        }
        self._contexts.append(record)

    def add_node(self, node_handle, timestamp, tid, rmw_handle, name, namespace) -> None:
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

    def add_timer(self, handle, timestamp, period, tid) -> None:
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
        self, timestamp: int, callback: int, is_intra_process: bool
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'callback_start_timestamp': timestamp,
                'callback_object': callback,
                'is_intra_process': is_intra_process,
            }
        )
        self.callback_start_instances.append(record)

    def add_callback_end_instance(self, timestamp: int, callback: int) -> None:
        record = RecordFactory.create_instance(
            {'callback_end_timestamp': timestamp, 'callback_object': callback}
        )
        self.callback_end_instances.append(record)

    def add_rclcpp_intra_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tid': tid,
                'rclcpp_intra_publish_timestamp': timestamp,
                'publisher_handle': publisher_handle,
                'message': message,
                'message_timestamp': message_timestamp,
            }
        )
        self.rclcpp_intra_publish_instances.append(record)

    def add_rclcpp_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tid': tid,
                'rclcpp_publish_timestamp': timestamp,
                'publisher_handle': publisher_handle,
                'message': message,
                'message_timestamp': message_timestamp,
            }
        )
        self.rclcpp_publish_instances.append(record)

    def add_rcl_publish_instance(
        self,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tid': tid,
                'rcl_publish_timestamp': timestamp,
                'publisher_handle': publisher_handle,
                'message': message,
            }
        )
        self.rcl_publish_instances.append(record)

    def add_dds_write_instance(
        self,
        tid: int,
        timestamp: int,
        message: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tid': tid,
                'dds_write_timestamp': timestamp,
                'message': message,
            }
        )
        self.dds_write_instances.append(record)

    def add_dds_bind_addr_to_addr(
        self,
        timestamp: int,
        addr_from: int,
        addr_to: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'dds_bind_addr_to_addr_timestamp': timestamp,
                'addr_from': addr_from,
                'addr_to': addr_to,
            }
        )
        self.dds_bind_addr_to_addr.append(record)

    def add_dds_bind_addr_to_stamp(
        self,
        tid: int,
        timestamp: int,
        addr: int,
        source_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tid': tid,
                'dds_bind_addr_to_stamp_timestamp': timestamp,
                'addr': addr,
                'source_timestamp': source_timestamp,
            }
        )
        self.dds_bind_addr_to_stamp.append(record)

    def add_on_data_available_instance(
        self,
        timestamp: int,
        source_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'on_data_available_timestamp': timestamp,
                'source_timestamp': source_timestamp,
            }
        )
        self.on_data_available_instances.append(record)

    def add_message_construct_instance(
        self, timestamp: int, original_message: int, constructed_message: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'message_construct_timestamp': timestamp,
                'original_message': original_message,
                'constructed_message': constructed_message,
            }
        )
        self.message_construct_instances.append(record)

    def add_dispatch_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
        source_timestamp: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'dispatch_subscription_callback_timestamp': timestamp,
                'callback_object': callback_object,
                'message': message,
                'source_timestamp': source_timestamp,
                'message_timestamp': message_timestamp,
            }
        )
        self.dispatch_subscription_callback_instances.append(record)

    def add_sim_time(
        self,
        timestamp: int,
        sim_time: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'system_time': timestamp,
                'sim_time': sim_time
            }
        )
        self.sim_time.append(record)

    def add_rmw_implementation(self, rmw_impl: str):
        self._rmw_impl.append({'rmw_impl': rmw_impl})

    def add_dispatch_intra_process_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'dispatch_intra_process_subscription_callback_timestamp': timestamp,
                'callback_object': callback_object,
                'message': message,
                'message_timestamp': message_timestamp
            }
        )
        self.dispatch_intra_process_subscription_callback_instances.append(
            record)

    def add_tilde_subscribe(
        self,
        timestamp: int,
        subscription: int,
        tilde_message_id: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tilde_subscribe_timestamp': timestamp,
                'subscription': subscription,
                'tilde_message_id': tilde_message_id
            }
        )
        self.tilde_subscribe.append(record)

    def add_tilde_publish(
        self,
        timestamp: int,
        publisher: int,
        subscription_id: int,
        tilde_message_id: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'tilde_publish_timestamp': timestamp,
                'publisher': publisher,
                'subscription_id': subscription_id,
                'tilde_message_id': tilde_message_id,
            }
        )
        self.tilde_publish.append(record)

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

    def _finalize(self) -> None:
        self.contexts = pd.DataFrame.from_dict(self._contexts)
        if self._contexts:
            self.contexts.set_index('context_handle', inplace=True, drop=True)
        self.nodes = pd.DataFrame.from_dict(self._nodes)
        if self._nodes:
            self.nodes.set_index('node_handle', inplace=True, drop=True)
        self.publishers = pd.DataFrame.from_dict(self._publishers)
        if self._publishers:
            self.publishers.set_index(
                'publisher_handle', inplace=True, drop=True)
        self.subscriptions = pd.DataFrame.from_dict(self._subscriptions)
        if self._subscriptions:
            self.subscriptions.set_index(
                'subscription_handle', inplace=True, drop=True)
        self.subscription_objects = pd.DataFrame.from_dict(
            self._subscription_objects)
        if self._subscription_objects:
            self.subscription_objects.set_index(
                'subscription', inplace=True, drop=True)
        self.services = pd.DataFrame.from_dict(self._services)
        if self._services:
            self.services.set_index('service_handle', inplace=True, drop=True)
        self.clients = pd.DataFrame.from_dict(self._clients)
        if self._clients:
            self.clients.set_index('client_handle', inplace=True, drop=True)
        self.timers = pd.DataFrame.from_dict(self._timers)
        if self._timers:
            self.timers.set_index('timer_handle', inplace=True, drop=True)
        self.timer_node_links = pd.DataFrame.from_dict(self._timer_node_links)
        if self._timer_node_links:
            self.timer_node_links.set_index(
                'timer_handle', inplace=True, drop=True)
        self.callback_objects = pd.DataFrame.from_dict(self._callback_objects)
        if self._callback_objects:
            self.callback_objects.set_index(
                'reference', inplace=True, drop=True)
        self.callback_symbols = pd.DataFrame.from_dict(self._callback_symbols)
        if self._callback_symbols:
            self.callback_symbols.set_index(
                'callback_object', inplace=True, drop=True)
        self.lifecycle_state_machines = pd.DataFrame.from_dict(
            self._lifecycle_state_machines)
        if self._lifecycle_state_machines:
            self.lifecycle_state_machines.set_index(
                'state_machine_handle', inplace=True, drop=True
            )
        self.lifecycle_transitions = pd.DataFrame.from_dict(
            self._lifecycle_transitions)
        self.executors = pd.DataFrame.from_dict(self._executors)
        if self._executors:
            self.executors.set_index(
                'executor_addr', inplace=True, drop=True
            )
        self.executors_static = pd.DataFrame.from_dict(self._executors_static)
        if self._executors_static:
            self.executors_static.set_index(
                'executor_addr', inplace=True, drop=True
            )
        self.callback_groups = pd.DataFrame.from_dict(self._callback_groups)
        if self._callback_groups:
            self.callback_groups.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.callback_groups_static = pd.DataFrame.from_dict(self._callback_groups_static)
        if self._callback_groups_static:
            self.callback_groups_static.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.callback_group_timer = pd.DataFrame.from_dict(self._callback_group_timer)
        if self._callback_group_timer:
            self.callback_group_timer.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.callback_group_subscription = pd.DataFrame.from_dict(
            self._callback_group_subscription)
        if self._callback_group_subscription:
            self.callback_group_subscription.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.callback_group_service = pd.DataFrame.from_dict(self._callback_group_service)
        if self._callback_group_service:
            self.callback_group_service.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.callback_group_client = pd.DataFrame.from_dict(self._callback_group_client)
        if self._callback_group_client:
            self.callback_group_client.set_index(
                'callback_group_addr', inplace=True, drop=True
            )
        self.tilde_subscriptions = pd.DataFrame.from_dict(self._tilde_subscriptions)
        if self._tilde_subscriptions:
            self.tilde_subscriptions.set_index(
                'subscription', inplace=True, drop=True
            )
        self.tilde_publishers = pd.DataFrame.from_dict(self._tilde_publishers)
        if self._tilde_publishers:
            self.tilde_publishers.set_index(
                'publisher', inplace=True, drop=True
            )
        self.tilde_subscribe_added = pd.DataFrame.from_dict(self._tilde_subscribe_added)
        if self._tilde_subscribe_added:
            self.tilde_subscribe_added.set_index(
                'subscription_id', inplace=True, drop=True
            )

        self.rmw_impl = pd.DataFrame.from_dict(self._rmw_impl)

    def print_data(self) -> None:
        print('====================ROS 2 DATA MODEL===================')
        print('Contexts:')
        print(self.contexts.to_string())
        print()
        print('Nodes:')
        print(self.nodes.to_string())
        print()
        print('Publishers:')
        print(self.publishers.to_string())
        print()
        print('Subscriptions:')
        print(self.subscriptions.to_string())
        print()
        print('Subscription objects:')
        print(self.subscription_objects.to_string())
        print()
        print('Services:')
        print(self.services.to_string())
        print()
        print('Clients:')
        print(self.clients.to_string())
        print()
        print('Timers:')
        print(self.timers.to_string())
        print()
        print('Timer-node links:')
        print(self.timer_node_links.to_string())
        print()
        print('Callback objects:')
        print(self.callback_objects.to_string())
        print()
        print('Callback symbols:')
        print(self.callback_symbols.to_string())
        print()
        print('Callback instances:')
        print(self.callback_instances.to_string())
        print()
        print('Lifecycle state machines:')
        print(self.lifecycle_state_machines.to_string())
        print()
        print('Lifecycle transitions:')
        print(self.lifecycle_transitions.to_string())
        print('==================================================')
