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

"""Module for ROS 2 data model."""

import pandas as pd

from tracetools_analysis.data_model import DataModel, DataModelIntermediateStorage
from trace_analysis.record.record import Record, Records


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

        # Events (multiple instances, may not have a meaningful index)
        self.lifecycle_transitions = Records()
        self.callback_start_instances = Records()
        self.callback_end_instances = Records()
        self.dds_write_instances = Records()
        self.dds_bind_addr_to_stamp = Records()
        self.dds_bind_addr_to_addr = Records()
        self.on_data_available_instances = Records()
        self.rclcpp_intra_publish_instances = Records()
        self.rclcpp_publish_instances = Records()
        self.rcl_publish_instances = Records()
        self.dispatch_subscription_callback_instances = Records()
        self.dispatch_intra_process_subscription_callback_instances = Records()
        self.message_construct_instances = Records()

    def add_context(self, context_handle, timestamp, pid, version) -> None:
        record = Record(
            {
                "context_handle": context_handle,
                "timestamp": timestamp,
                "pid": pid,
                "version": version,
            }
        )
        self._contexts.append(record)

    def add_node(self, node_handle, timestamp, tid, rmw_handle, name, namespace) -> None:
        record = Record(
            {
                "node_handle": node_handle,
                "timestamp": timestamp,
                "tid": tid,
                "rmw_handle": rmw_handle,
                "namespace": namespace,
                "name": name,
            }
        )
        self._nodes.append(record)

    def add_publisher(self, handle, timestamp, node_handle, rmw_handle, topic_name, depth) -> None:
        record = Record(
            {
                "publisher_handle": handle,
                "timestamp": timestamp,
                "node_handle": node_handle,
                "rmw_handle": rmw_handle,
                "topic_name": topic_name,
                "depth": depth,
            }
        )
        self._publishers.append(record)

    def add_rcl_subscription(
        self, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
        record = Record(
            {
                "subscription_handle": handle,
                "timestamp": timestamp,
                "node_handle": node_handle,
                "rmw_handle": rmw_handle,
                "topic_name": topic_name,
                "depth": depth,
            }
        )
        self._subscriptions.append(record)

    def add_rclcpp_subscription(
        self, subscription_pointer, timestamp, subscription_handle
    ) -> None:
        record = Record(
            {
                "subscription": subscription_pointer,
                "timestamp": timestamp,
                "subscription_handle": subscription_handle,
            }
        )
        self._subscription_objects.append(record)

    def add_service(self, handle, timestamp, node_handle, rmw_handle, service_name) -> None:
        record = Record(
            {
                "service_handle": timestamp,
                "timestamp": timestamp,
                "node_handle": node_handle,
                "rmw_handle": rmw_handle,
                "service_name": service_name,
            }
        )
        self._services.append(record)

    def add_client(self, handle, timestamp, node_handle, rmw_handle, service_name) -> None:
        record = Record(
            {
                "client_handle": handle,
                "timestamp": timestamp,
                "node_handle": node_handle,
                "rmw_handle": rmw_handle,
                "service_name": service_name,
            }
        )
        self._clients.append(record)

    def add_timer(self, handle, timestamp, period, tid) -> None:
        record = Record(
            {
                "timer_handle": handle,
                "timestamp": timestamp,
                "period": period,
                "tid": tid,
            }
        )
        self._timers.append(record)

    def add_timer_node_link(self, handle, timestamp, node_handle) -> None:
        record = Record(
            {
                "timer_handle": handle,
                "timestamp": timestamp,
                "node_handle": node_handle,
            }
        )
        self._timer_node_links.append(record)

    def add_callback_object(self, reference, timestamp, callback_object) -> None:
        record = Record(
            {
                "reference": reference,
                "timestamp": timestamp,
                "callback_object": callback_object,
            }
        )
        self._callback_objects.append(record)

    def add_callback_symbol(self, callback_object, timestamp, symbol) -> None:
        record = Record(
            {
                "callback_object": callback_object,
                "timestamp": timestamp,
                "symbol": symbol,
            }
        )
        self._callback_symbols.append(record)

    def add_lifecycle_state_machine(self, handle, node_handle) -> None:
        record = Record(
            {
                "state_machine_handle": handle,
                "node_handle": node_handle,
            }
        )
        self._lifecycle_state_machines.append(record)

    def add_lifecycle_state_transition(
        self, state_machine_handle, start_label, goal_label, timestamp
    ) -> None:
        record = Record(
            {
                "state_machine_handle": state_machine_handle,
                "start_label": start_label,
                "goal_label": goal_label,
                "timestamp": timestamp,
            }
        )
        self.lifecycle_transitions.append(record)

    def add_callback_start_instance(
        self, timestamp: int, callback: int, is_intra_process: bool
    ) -> None:
        record = Record(
            {
                "callback_start_timestamp": timestamp,
                "callback_object": callback,
                "is_intra_process": is_intra_process,
            }
        )
        self.callback_start_instances.append(record)

    def add_callback_end_instance(self, timestamp: int, callback: int) -> None:
        record = Record({"callback_end_timestamp": timestamp, "callback_object": callback})
        self.callback_end_instances.append(record)

    def add_rclcpp_intra_publish_instance(
        self,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = Record(
            {
                "rclcpp_intra_publish_timestamp": timestamp,
                "publisher_handle": publisher_handle,
                "message": message,
            }
        )
        self.rclcpp_intra_publish_instances.append(record)

    def add_rclcpp_publish_instance(
        self,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = Record(
            {
                "rclcpp_publish_timestamp": timestamp,
                "publisher_handle": publisher_handle,
                "message": message,
            }
        )
        self.rclcpp_publish_instances.append(record)

    def add_rcl_publish_instance(
        self,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = Record(
            {
                "rcl_publish_timestamp": timestamp,
                "publisher_handle": publisher_handle,
                "message": message,
            }
        )
        self.rcl_publish_instances.append(record)

    def add_dds_write_instance(
        self,
        timestamp: int,
        message: int,
    ) -> None:
        record = Record(
            {
                "dds_write_timestamp": timestamp,
                "message": message,
            }
        )
        self.dds_write_instances.append(record)

    def add_dds_bind_addr_to_addr(
        self,
        timestamp: int,
        addr_from: int,
        addr_to: int,
    ) -> None:
        record = Record(
            {
                "dds_bind_addr_to_addr_timestamp": timestamp,
                "addr_from": addr_from,
                "addr_to": addr_to,
            }
        )
        self.dds_bind_addr_to_addr.append(record)

    def add_dds_bind_addr_to_stamp(
        self,
        timestamp: int,
        addr: int,
        source_timestamp: int,
    ) -> None:
        record = Record(
            {
                "dds_bind_addr_to_stamp_timestamp": timestamp,
                "addr": addr,
                "source_timestamp": source_timestamp,
            }
        )
        self.dds_bind_addr_to_stamp.append(record)

    def add_on_data_available_instance(
        self,
        timestamp: int,
        source_timestamp: int,
    ) -> None:
        record = Record(
            {
                "on_data_available_timestamp": timestamp,
                "source_timestamp": source_timestamp,
            }
        )
        self.on_data_available_instances.append(record)

    def add_message_construct_instance(
        self, timestamp: int, original_message: int, constructed_message: int
    ) -> None:
        record = Record(
            {
                "message_construct_timestamp": timestamp,
                "original_message": original_message,
                "constructed_message": constructed_message,
            }
        )
        self.message_construct_instances.append(record)

    def add_dispatch_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
        source_timestamp: int,
    ) -> None:
        record = Record(
            {
                "dispatch_subscription_callback_timestamp": timestamp,
                "callback_object": callback_object,
                "message": message,
                "source_timestamp": source_timestamp,
            }
        )
        self.dispatch_subscription_callback_instances.append(record)

    def add_dispatch_intra_process_subscription_callback_instance(
        self,
        timestamp: int,
        callback_object: int,
        message: int,
    ) -> None:
        record = Record(
            {
                "dispatch_intra_process_subscription_callback_timestamp": timestamp,
                "callback_object": callback_object,
                "message": message,
            }
        )
        self.dispatch_intra_process_subscription_callback_instances.append(record)

    def _finalize(self) -> None:
        self.contexts = pd.DataFrame.from_dict([_.data for _ in self._contexts])
        if self._contexts:
            self.contexts.set_index("context_handle", inplace=True, drop=True)
        self.nodes = pd.DataFrame.from_dict([_.data for _ in self._nodes])
        if self._nodes:
            self.nodes.set_index("node_handle", inplace=True, drop=True)
        self.publishers = pd.DataFrame.from_dict([_.data for _ in self._publishers])
        if self._publishers:
            self.publishers.set_index("publisher_handle", inplace=True, drop=True)
        self.subscriptions = pd.DataFrame.from_dict([_.data for _ in self._subscriptions])
        if self._subscriptions:
            self.subscriptions.set_index("subscription_handle", inplace=True, drop=True)
        self.subscription_objects = pd.DataFrame.from_dict(
            [_.data for _ in self._subscription_objects]
        )
        if self._subscription_objects:
            self.subscription_objects.set_index("subscription", inplace=True, drop=True)
        self.services = pd.DataFrame.from_dict([_.data for _ in self._services])
        if self._services:
            self.services.set_index("service_handle", inplace=True, drop=True)
        self.clients = pd.DataFrame.from_dict([_.data for _ in self._clients])
        if self._clients:
            self.clients.set_index("client_handle", inplace=True, drop=True)
        self.timers = pd.DataFrame.from_dict([_.data for _ in self._timers])
        if self._timers:
            self.timers.set_index("timer_handle", inplace=True, drop=True)
        self.timer_node_links = pd.DataFrame.from_dict([_.data for _ in self._timer_node_links])
        if self._timer_node_links:
            self.timer_node_links.set_index("timer_handle", inplace=True, drop=True)
        self.callback_objects = pd.DataFrame.from_dict([_.data for _ in self._callback_objects])
        if self._callback_objects:
            self.callback_objects.set_index("reference", inplace=True, drop=True)
        self.callback_symbols = pd.DataFrame.from_dict([_.data for _ in self._callback_symbols])
        if self._callback_symbols:
            self.callback_symbols.set_index("callback_object", inplace=True, drop=True)
        self.lifecycle_state_machines = pd.DataFrame.from_dict(
            [_.data for _ in self._lifecycle_state_machines]
        )
        if self._lifecycle_state_machines:
            self.lifecycle_state_machines.set_index(
                "state_machine_handle", inplace=True, drop=True
            )

    def print_data(self) -> None:
        print("====================ROS 2 DATA MODEL===================")
        print("Contexts:")
        print(self.contexts.to_string())
        print()
        print("Nodes:")
        print(self.nodes.to_string())
        print()
        print("Publishers:")
        print(self.publishers.to_string())
        print()
        print("Subscriptions:")
        print(self.subscriptions.to_string())
        print()
        print("Subscription objects:")
        print(self.subscription_objects.to_string())
        print()
        print("Services:")
        print(self.services.to_string())
        print()
        print("Clients:")
        print(self.clients.to_string())
        print()
        print("Timers:")
        print(self.timers.to_string())
        print()
        print("Timer-node links:")
        print(self.timer_node_links.to_string())
        print()
        print("Callback objects:")
        print(self.callback_objects.to_string())
        print()
        print("Callback symbols:")
        print(self.callback_symbols.to_string())
        print()
        print("Callback instances:")
        print(self.callback_instances.to_string())
        print()
        print("Lifecycle state machines:")
        print(self.lifecycle_state_machines.to_string())
        print()
        print("Lifecycle transitions:")
        print(self.lifecycle_transitions.to_string())
        print("==================================================")
