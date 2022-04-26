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

from typing import Set, Tuple

from caret_analyze.record.column import Column, ColumnAttribute
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
        self._symbol_rename: DataModelIntermediateStorage = []

        self._tilde_subscriptions: DataModelIntermediateStorage = []
        self._tilde_publishers: DataModelIntermediateStorage = []
        self._tilde_subscribe_added: DataModelIntermediateStorage = []
        self._transform_broadcaster: DataModelIntermediateStorage = []
        self._transform_broadcaster_frames: DataModelIntermediateStorage = []
        self._construct_tf_buffer: DataModelIntermediateStorage = []
        self._init_bind_tf_buffer_core: DataModelIntermediateStorage = []
        self._tf_buffer_lookup_transforms: Set[Tuple[int, int, int]] = set()
        self._construct_node_hook: DataModelIntermediateStorage = []
        self._broadcaster_frame_id_compact: DataModelIntermediateStorage = []
        self._buffer_frame_id_compact: DataModelIntermediateStorage = []

        # Events (multiple instances, may not have a meaningful index)
        # string argument
        self._lifecycle_transitions: DataModelIntermediateStorage = []

        # Events (multiple instances, may not have a meaningful index)
        self.callback_start_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('callback_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('callback_object'),
                Column('is_intra_process')
            ]
        )
        self.callback_end_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('callback_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('callback_object')
            ]
        )
        self.dds_write_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('dds_write_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('message')
            ]
        )
        self.dds_bind_addr_to_stamp = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('dds_bind_addr_to_stamp_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('addr'),
                Column('source_timestamp')
            ]
        )
        self.dds_bind_addr_to_addr = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('dds_bind_addr_to_addr_timestamp'),
                Column('addr_from'),
                Column('addr_to')
            ]
        )
        self.on_data_available_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('on_data_available_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('source_timestamp')
            ]
        )
        self.rclcpp_intra_publish_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('rclcpp_intra_publish_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('publisher_handle'),
                Column('message'),
                Column('message_timestamp')
            ]
        )
        self.rclcpp_publish_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('rclcpp_publish_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('publisher_handle'),
                Column('message'),
                Column('message_timestamp')
            ]
        )
        self.rcl_publish_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('rcl_publish_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('publisher_handle'),
                Column('message')
            ]
        )
        self.dispatch_subscription_callback_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('dispatch_subscription_callback_timestamp'),
                Column('callback_object'),
                Column('message'),
                Column('source_timestamp'),
                Column('message_timestamp')
            ]
        )
        self.dispatch_intra_process_subscription_callback_instances = \
            RecordsFactory.create_instance(
                None,
                [
                    Column('pid'),
                    Column('tid'),
                    Column('dispatch_intra_process_subscription_callback_timestamp'),
                    Column('callback_object'),
                    Column('message'),
                    Column('message_timestamp')
                ]
            )
        self.message_construct_instances = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('message_construct_timestamp'),
                Column('original_message'),
                Column('constructed_message')
            ]
        )

        self.tilde_subscribe = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('tilde_subscribe_timestamp'),
                Column('subscription'),
                Column('tilde_message_id')
            ]
        )

        self.tilde_publish = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('tilde_publish_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('publisher'),
                Column('message_info_id'),
                Column('message_id')
            ]
        )
        self.sim_time = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('system_time', [ColumnAttribute.SYSTEM_TIME]),
                Column('sim_time')
            ]
        )
        self.timer_event = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('time_event_stamp', [ColumnAttribute.SYSTEM_TIME])
            ]
        )
        self.send_transform = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('send_transform_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('broadcaster'),
                Column('tf_timestamp'),
                Column('frame_id_compact'),
                Column('child_frame_id_compact'),
            ]
        )
        self.tf_lookup_transform_start = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('lookup_transform_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('tf_buffer_core'),
                Column('tf_lookup_target_time'),
                Column('frame_id_compact'),
                Column('child_frame_id_compact'),
            ]
        )
        self.tf_lookup_transform_end = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('lookup_transform_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('tf_buffer_core'),
            ]
        )
        self.tf_find_closest = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('find_closest_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('tf_buffer_core'),
                Column('frame_id_compact'),
                Column('child_frame_id_compact'),
                Column('stamp'),
            ]
        )
        self.tf_set_transform = RecordsFactory.create_instance(
            None,
            [
                Column('pid'),
                Column('tid'),
                Column('set_transform_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                Column('tf_buffer_core'),
                Column('tf_timestamp'),
                Column('frame_id_compact'),
                Column('child_frame_id_compact'),
            ]
        )

    def add_context(self, pid, tid, context_handle, timestamp, version) -> None:
        record = {
            'context_handle': context_handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'version': version,  # Comment out to align with Dict[str: int64_t]
        }
        self._contexts.append(record)

    def add_node(self, pid, tid, node_handle, timestamp, rmw_handle, name, namespace) -> None:
        record = {
            'node_handle': node_handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'rmw_handle': rmw_handle,
            'namespace': namespace,
            'name': name,
        }
        self._nodes.append(record)

    def add_publisher(self, pid, tid, handle, timestamp, node_handle, rmw_handle, topic_name, depth) -> None:
        record = {
            'publisher_handle': handle,
            'timestamp': timestamp,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'pid': pid,
            'tid': tid,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._publishers.append(record)

    def add_rcl_subscription(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
        record = {
            'subscription_handle': handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'topic_name': topic_name,
            'depth': depth,
        }
        self._subscriptions.append(record)

    def add_rclcpp_subscription(
        self, pid, tid, subscription_pointer, timestamp, subscription_handle
    ) -> None:
        record = {
            'subscription': subscription_pointer,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'subscription_handle': subscription_handle,
        }
        self._subscription_objects.append(record)

    def add_service(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, service_name
    ) -> None:
        record = {
            'service_handle': handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'service_name': service_name,
        }
        self._services.append(record)

    def add_client(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, service_name
    ) -> None:
        record = {
            'client_handle': handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'node_handle': node_handle,
            'rmw_handle': rmw_handle,
            'service_name': service_name,
        }
        self._clients.append(record)

    def add_timer(
        self, pid, tid, handle, timestamp, period
    ) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'period': period,
            'pid': pid,
            'tid': tid,
        }
        self._timers.append(record)

    def add_tilde_subscribe_added(
        self, pid, tid, subscription_id, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'subscription_id': subscription_id,
            'node_name': node_name,
            'topic_name': topic_name,
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp
        }
        self._tilde_subscribe_added.append(record)

    def add_timer_node_link(self, pid, tid, handle, timestamp, node_handle) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'node_handle': node_handle,
        }
        self._timer_node_links.append(record)

    def add_callback_object(self, pid, tid, reference, timestamp, callback_object) -> None:
        record = {
            'reference': reference,
            'timestamp': timestamp,
            'callback_object': callback_object,
            'pid': pid,
            'tid': tid,
        }
        self._callback_objects.append(record)

    def add_callback_symbol(self, pid, tid, callback_object, timestamp, symbol) -> None:
        record = {
            'callback_object': callback_object,
            'timestamp': timestamp,
            'symbol': symbol,
            'pid': pid,
            'tid': tid,
        }
        self._callback_symbols.append(record)

    def add_lifecycle_state_machine(self, pid, tid, handle, node_handle) -> None:
        record = {
            'state_machine_handle': handle,
            'node_handle': node_handle,
            'pid': pid,
            'tid': tid,
        }
        self._lifecycle_state_machines.append(record)

    def add_lifecycle_state_transition(
        self, pid, tid, state_machine_handle, start_label, goal_label, timestamp
    ) -> None:
        record = {
            'state_machine_handle': state_machine_handle,
            'start_label': start_label,
            'goal_label': goal_label,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
        }
        self._lifecycle_transitions.append(record)

    def add_tilde_subscription(
        self, pid, tid, subscription, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'subscription': subscription,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
        }
        self._tilde_subscriptions.append(record)

    def add_tilde_publisher(
        self, pid, tid, publisher, node_name, topic_name, timestamp
    ) -> None:
        record = {
            'publisher': publisher,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
        }
        self._tilde_publishers.append(record)

    def add_callback_start_instance(
        self, timestamp: int,
        pid: int,
        tid: int,
        callback: int,
        is_intra_process: bool,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'callback_start_timestamp': timestamp,
                'pid': pid,
                'tid': tid,
                'callback_object': callback,
                'is_intra_process': is_intra_process,
            }
        )
        self.callback_start_instances.append(record)

    def add_callback_end_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        callback: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'callback_end_timestamp': timestamp,
                'pid': pid,
                'tid': tid,
                'callback_object': callback
            }
        )
        self.callback_end_instances.append(record)

    def add_rclcpp_intra_publish_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
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
        pid: int,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
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
        pid: int,
        tid: int,
        timestamp: int,
        publisher_handle: int,
        message: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'rcl_publish_timestamp': timestamp,
                'publisher_handle': publisher_handle,
                'message': message,
            }
        )
        self.rcl_publish_instances.append(record)

    def add_dds_write_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        message: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'dds_write_timestamp': timestamp,
                'message': message,
            }
        )
        self.dds_write_instances.append(record)

    def add_dds_bind_addr_to_addr(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        addr_from: int,
        addr_to: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'dds_bind_addr_to_addr_timestamp': timestamp,
                'addr_from': addr_from,
                'addr_to': addr_to,
            }
        )
        self.dds_bind_addr_to_addr.append(record)

    def add_dds_bind_addr_to_stamp(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        addr: int,
        source_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'dds_bind_addr_to_stamp_timestamp': timestamp,
                'addr': addr,
                'source_timestamp': source_timestamp,
            }
        )
        self.dds_bind_addr_to_stamp.append(record)

    def add_on_data_available_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        source_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'on_data_available_timestamp': timestamp,
                'source_timestamp': source_timestamp,
            }
        )
        self.on_data_available_instances.append(record)

    def add_message_construct_instance(
        self, pid: int, tid: int, timestamp: int, original_message: int, constructed_message: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'message_construct_timestamp': timestamp,
                'original_message': original_message,
                'constructed_message': constructed_message,
            }
        )
        self.message_construct_instances.append(record)

    def add_dispatch_subscription_callback_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        callback_object: int,
        message: int,
        source_timestamp: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
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
        pid: int,
        tid: int,
        timestamp: int,
        sim_time: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'system_time': timestamp,
                'sim_time': sim_time
            }
        )
        self.sim_time.append(record)

    def add_rmw_implementation(self, rmw_impl: str):
        self._rmw_impl.append({'rmw_impl': rmw_impl})

    def add_symbol_rename(
        self, pid: int, tid: int, timestamp: int, symbol_from: str, symbol_to: str
    ):
        self._symbol_rename.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'symbol_from': symbol_from,
                'symbol_to': symbol_to
            }
        )

    def add_transform_broadcaster(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        broadcaster: int,
        publisher_handle: int,
    ) -> None:
        self._transform_broadcaster.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'transform_broadcaster': broadcaster,
                'publisher_handle': publisher_handle
            }
        )

    def add_transform_broadcaster_frames(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        transform_broadcaster: int,
        frame_id: str,
        child_frame_id: str
    ) -> None:
        self._transform_broadcaster_frames.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'transform_broadcaster': transform_broadcaster,
                'frame_id': frame_id,
                'child_frame_id': child_frame_id
            }
        )

    def add_construct_tf_buffer(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        tf_buffer: int,
        tf_buffer_core: int,
        clock: int
    ) -> None:
        self._construct_tf_buffer.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_buffer': tf_buffer,
                'tf_buffer_core': tf_buffer_core,
                'clock': clock
            }
        )

    def add_init_bind_tf_buffer_core(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        tf_buffer_core: int,
        callback: int
    ) -> None:
        self._init_bind_tf_buffer_core.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_buffer_core': tf_buffer_core,
                'callback': callback
            }
        )

    def add_construct_node_hook(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        node_handle: int,
        clock: int
    ) -> None:
        self._construct_node_hook.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'node_handle': node_handle,
                'clock': clock
            }
        )

    def add_broadcaster_frame_id_compact(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        broadcaster: int,
        frame_id: str,
        frame_id_compact: int
    ) -> None:
        self._broadcaster_frame_id_compact.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_broadcaster': broadcaster,
                'frame_id': frame_id,
                'frame_id_compact': frame_id_compact,
            }
        )

    def add_buffer_frame_id_compact(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        frame_id: str,
        frame_id_compact: int
    ) -> None:
        self._buffer_frame_id_compact.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'frame_id': frame_id,
                'frame_id_compact': frame_id_compact,
            }
        )

    def add_dispatch_intra_process_subscription_callback_instance(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        callback_object: int,
        message: int,
        message_timestamp: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
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
        pid: int,
        tid: int,
        timestamp: int,
        subscription: int,
        tilde_message_id: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'tilde_subscribe_timestamp': timestamp,
                'subscription': subscription,
                'tilde_message_id': tilde_message_id
            }
        )
        self.tilde_subscribe.append(record)

    def add_tilde_publish(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        publisher: int,
        message_info_id: int,
        message_id: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'tilde_publish_timestamp': timestamp,
                'publisher': publisher,
                'message_info_id': message_info_id,
                'message_id': message_id,
            }
        )
        self.tilde_publish.append(record)

    def add_send_transform(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        broadcaster: int,
        tf_stamp: int,
        frame_id_compact: int,
        child_frame_id_compact: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'send_transform_timestamp': timestamp,
                'broadcaster': broadcaster,
                'tf_timestamp': tf_stamp,
                'frame_id_compact': frame_id_compact,
                'child_frame_id_compact': child_frame_id_compact,
            }
        )
        self.send_transform.append(record)

    def add_tf_lookup_transform_start(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        target_time: int,
        frame_id_compact: int,
        child_frame_id_compact: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'lookup_transform_start_timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'tf_lookup_target_time': target_time,
                'frame_id_compact': frame_id_compact,
                'child_frame_id_compact': child_frame_id_compact,
            }
        )

        self._tf_buffer_lookup_transforms.add(
            (buffer_core, frame_id_compact, child_frame_id_compact)
        )
        self.tf_lookup_transform_start.append(record)

    def add_tf_lookup_transform_end(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'lookup_transform_end_timestamp': timestamp,
                'tf_buffer_core': buffer_core,
            }
        )
        self.tf_lookup_transform_end.append(record)

    def add_tf_find_closest(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        frame_id_compact: int,
        child_frame_id_compact: int,
        stamp: int,
        frame_id_compact_: int,
        child_frame_id_compact_: int,
        stamp_: int
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'find_closest_timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'frame_id_compact': frame_id_compact,
                'child_frame_id_compact': child_frame_id_compact,
                'stamp': stamp,
            }
        )
        self.tf_find_closest.append(record)
        if frame_id_compact_ != 0 and frame_id_compact_ != 0:
            record = RecordFactory.create_instance(
                {
                    'find_closest_timestamp': timestamp,
                    'tf_buffer_core': buffer_core,
                    'frame_id_compact': frame_id_compact_,
                    'child_frame_id_compact': child_frame_id_compact_,
                    'stamp': stamp_,
                }
            )
            self.tf_find_closest.append(record)

    def add_tf_set_transform(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        stamp: int,
        frame_id_compact: int,
        child_frame_id_compact: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'set_transform_timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'tf_timestamp': stamp,
                'frame_id_compact': frame_id_compact,
                'child_frame_id_compact': child_frame_id_compact,
            }
        )
        self.tf_set_transform.append(record)

    def add_executor(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'executor_type_name': executor_type_name,
        }
        self._executors.append(record)

    def add_executor_static(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        entities_collector_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'entities_collector_addr': entities_collector_addr,
            'executor_type_name': executor_type_name,
        }
        self._executors_static.append(record)

    def add_callback_group(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        }
        self._callback_groups.append(record)

    def add_callback_group_static_executor(
        self,
        pid: int,
        tid: int,
        entities_collector_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'entities_collector_addr': entities_collector_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        }
        self._callback_groups_static.append(record)

    def callback_group_add_timer(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        timer_handle: int
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'timer_handle': timer_handle,
        }
        self._callback_group_timer.append(record)

    def callback_group_add_subscription(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        subscription_handle: int
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'subscription_handle': subscription_handle,
        }
        self._callback_group_subscription.append(record)

    def callback_group_add_service(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        service_handle: int
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'service_handle': service_handle,
        }
        self._callback_group_service.append(record)

    def callback_group_add_client(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        client_handle: int
    ) -> None:
        record = {
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'client_handle': client_handle,
        }
        self._callback_group_client.append(record)

    def _finalize(self) -> None:
        self.contexts = pd.DataFrame.from_dict(self._contexts)
        self.nodes = pd.DataFrame.from_dict(self._nodes)
        self.publishers = pd.DataFrame.from_dict(self._publishers)
        self.subscriptions = pd.DataFrame.from_dict(self._subscriptions)
        self.subscription_objects = pd.DataFrame.from_dict(self._subscription_objects)
        self.services = pd.DataFrame.from_dict(self._services)
        self.clients = pd.DataFrame.from_dict(self._clients)
        self.timers = pd.DataFrame.from_dict(self._timers)
        self.timer_node_links = pd.DataFrame.from_dict(self._timer_node_links)
        self.callback_objects = pd.DataFrame.from_dict(self._callback_objects)
        self.construct_tf_buffer = pd.DataFrame.from_dict(self._construct_tf_buffer)
        self.init_bind_tf_buffer_core = pd.DataFrame.from_dict(self._init_bind_tf_buffer_core)

        tf_buffer_lookup_transforms_dict = [
                {'tf_buffer_core': t[0], 'frame_id_compact': t[1], 'child_frame_id_compact': t[2]}
                for t
                in self._tf_buffer_lookup_transforms
            ]
        self.tf_buffer_lookup_transforms = pd.DataFrame.from_dict(
            tf_buffer_lookup_transforms_dict
        )
        self.construct_node_hook = pd.DataFrame.from_dict(self._construct_node_hook)
        self.broadcaster_frame_id_compact = pd.DataFrame.from_dict(
            self._broadcaster_frame_id_compact
        )
        self.buffer_frame_id_compact = pd.DataFrame.from_dict(
            self._buffer_frame_id_compact)
        self.symbol_rename = pd.DataFrame.from_dict(self._symbol_rename)
        symbol_map = {
            symbol['symbol_from']: symbol['symbol_to'] for symbol in self._symbol_rename
        }
        for callback_symbol in self._callback_symbols:
            while callback_symbol['symbol'] in symbol_map:
                callback_symbol['symbol'] = symbol_map[callback_symbol['symbol']]
        self.callback_symbols = pd.DataFrame.from_dict(self._callback_symbols)

        self.transform_broadcaster = pd.DataFrame.from_dict(self._transform_broadcaster)
        self.transform_broadcaster_frames = pd.DataFrame.from_dict(
            self._transform_broadcaster_frames)

        self.lifecycle_state_machines = pd.DataFrame.from_dict(self._lifecycle_state_machines)
        self.lifecycle_transitions = pd.DataFrame.from_dict(self._lifecycle_transitions)
        self.executors = pd.DataFrame.from_dict(self._executors)
        self.executors_static = pd.DataFrame.from_dict(self._executors_static)
        self.callback_groups = pd.DataFrame.from_dict(self._callback_groups)
        self.callback_groups_static = pd.DataFrame.from_dict(self._callback_groups_static)
        self.callback_group_timer = pd.DataFrame.from_dict(self._callback_group_timer)
        self.callback_group_subscription = pd.DataFrame.from_dict(self._callback_group_subscription)
        self.callback_group_service = pd.DataFrame.from_dict(self._callback_group_service)
        self.callback_group_client = pd.DataFrame.from_dict(self._callback_group_client)
        self.tilde_subscriptions = pd.DataFrame.from_dict(self._tilde_subscriptions)
        self.tilde_publishers = pd.DataFrame.from_dict(self._tilde_publishers)
        self.tilde_subscribe_added = pd.DataFrame.from_dict(self._tilde_subscribe_added)

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
