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

from caret_analyze.record.column import ColumnAttribute, ColumnValue
from caret_analyze.record.record_factory import RecordFactory, RecordsFactory
from caret_analyze.infra.lttng.data_frame import TracePointData

from tracetools_analysis.data_model import DataModel


class Ros2DataModel(DataModel):
    """
    Container to model pre-processed ROS 2 data for analysis.

    This aims to represent the data in a ROS 2-aware way.
    """

    def __init__(self) -> None:
        """Create a Ros2DataModel."""
        super().__init__()
        # Objects (one-time events, usually when something is created)
        self.rcl_init = TracePointData([
            'pid', 'tid', 'timestamp', 'context_handle', 'version'
        ])
        self.rcl_node_init = TracePointData([
            'pid', 'tid', 'timestamp', 'node_handle', 'namespace', 'name', 'rmw_handle'
        ])
        self.rcl_publisher_init = TracePointData([
            'pid', 'tid', 'timestamp', 'publisher_handle',
            'node_handle', 'rmw_handle', 'topic_name', 'depth'
        ])
        self.rcl_subscription_init = TracePointData([
            'pid', 'tid', 'timestamp', 'subscription_handle',
            'node_handle', 'rmw_handle', 'topic_name', 'depth'
        ])
        self.rclcpp_subscription_init = TracePointData([
            'pid', 'tid', 'timestamp', 'subscription', 'subscription_handle'
        ])
        self.rcl_service_init = TracePointData([
            'service_handle', 'timestamp', 'pid', 'tid',
            'node_handle', 'rmw_handle', 'service_name',
        ])
        self.rcl_client_init = TracePointData([
            'client_handle', 'timestamp', 'pid', 'tid', 'node_handle',
            'rmw_handle', 'service_name',
        ])
        self.rcl_timer_init = TracePointData([
            'pid', 'tid', 'timestamp', 'timer_handle', 'period'
        ])
        self.rclcpp_timer_link_node = TracePointData([
            'pid', 'tid', 'timestamp', 'timer_handle', 'node_handle'
        ])
        self.rclcpp_subscription_callback_added = TracePointData([
            'subscription',
            'timestamp',
            'pid',
            'tid',
            'callback_object',
        ])
        self.rclcpp_service_callback_added = TracePointData([
            'service_handle', 'timestamp', 'pid', 'tid', 'callback_object',
        ])
        self.rclcpp_timer_callback_added = TracePointData([
            'timestamp',
            'pid',
            'tid',
            'timer_handle',
            'callback_object',
        ])
        self.rclcpp_callback_register = TracePointData([
            'callback_object',
            'timestamp',
            'symbol',
            'pid',
            'tid',
        ])
        self.rcl_lifecycle_state_machine_init = TracePointData([
            'state_machine_handle',
            'node_handle',
            'pid',
            'tid',
        ])
        self.construct_executor = TracePointData([
            'pid', 'tid', 'timestamp', 'executor_addr', 'executor_type_name'
        ])
        self.construct_static_executor = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'executor_addr',
            'entities_collector_addr',
            'executor_type_name',
        ])
        self.add_callback_group = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'executor_addr',
            'callback_group_addr',
            'group_type_name'

        ])
        self.add_callback_group_static_executor = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'entities_collector_addr',
            'callback_group_addr',
            'group_type_name',
        ])
        self.callback_group_add_timer = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'callback_group_addr',
            'timer_handle',
        ])
        self.callback_group_add_subscription = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'callback_group_addr',
            'subscription_handle',
        ])
        self.callback_group_add_service = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'callback_group_addr',
            'service_handle',
        ])
        self.callback_group_add_client = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'callback_group_addr',
            'client_handle',
        ])
        self.rmw_implementation = TracePointData([
            'pid', 'tid', 'rmw_impl'
        ])
        self.symbol_rename = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'symbol_from',
            'symbol_to',
        ])
        self.tilde_subscription_init = TracePointData([
            'subscription',
            'node_name',
            'topic_name',
            'timestamp',
            'pid',
            'tid',
        ])
        self.tilde_publisher_init = TracePointData([
            'publisher',
            'node_name',
            'topic_name',
            'timestamp',
            'pid',
            'tid',
        ])
        self.tilde_subscribe_added = TracePointData([
            'subscription_id',
            'node_name',
            'topic_name',
            'pid',
            'tid',
            'timestamp',
        ])
        self.transform_broadcaster = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'transform_broadcaster',
            'publisher_handle',
        ])
        self.transform_broadcaster_frames = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'transform_broadcaster',
            'frame_id',
            'child_frame_id',
        ])
        self.construct_tf_buffer = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'tf_buffer',
            'tf_buffer_core',
            'clock',
        ])
        self.init_bind_tf_buffer_core = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'tf_buffer_core',
            'callback',
        ])

        self.construct_node_hook = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'node_handle',
            'clock'
        ])
        self.init_tf_broadcaster_frame_id_compact = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'tf_broadcaster',
            'frame_id',
            'frame_id_compact',
        ])
        self.init_tf_buffer_frame_id_compact = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'tf_buffer_core',
            'frame_id',
            'frame_id_compact',
        ])
        self.tf_buffer_lookup_transform = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'tf_buffer_core',
            'frame_id_compact',
            'child_frame_id_compact',
        ])
        self.construct_ipm = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'ipm',
        ])
        self.ipm_add_publisher = TracePointData([
            'pid',
            'tid',
            'ipm',
            'timestamp',
            'publisher_handle',
            'pub_id',
        ])
        self.ipm_add_subscription = TracePointData([
            'pid',
            'tid',
            'ipm',
            'timestamp',
            'subscription_handle',
            'sub_id',
        ])
        self.ipm_insert_sub_id_for_pub = TracePointData([
            'pid',
            'tid',
            'ipm',
            'timestamp',
            'sub_id',
            'pub_id',
            'use_take_shared_method'
        ])
        self.construct_ring_buffer = TracePointData([
            'pid',
            'tid',
            'timestamp',
            'buffer',
            'capacity',
        ])

        # not supported
        # self.lifecycle_transitions = DataFrame([
        # ])

        # Events (multiple instances, may not have a meaningful index)
        self.callback_start = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_start_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.TAKE_MSG,
                    ColumnAttribute.NODE_IO
                    ]),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process')
            ]
        )
        self.callback_end = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('callback_object')
            ]
        )
        self.dds_write = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('dds_write_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.OPTIONAL,
                ]),
                ColumnValue('message')
            ]
        )
        self.dds_bind_addr_to_stamp = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('dds_bind_addr_to_stamp_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('addr'),
                ColumnValue('source_timestamp')
            ]
        )
        self.dds_bind_addr_to_addr = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('dds_bind_addr_to_addr_timestamp'),
                ColumnValue('addr_from'),
                ColumnValue('addr_to')
            ]
        )
        self.on_data_available = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('on_data_available_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('source_timestamp')
            ]
        )
        self.rclcpp_intra_publish = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('rclcpp_intra_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.NODE_IO,
                    ColumnAttribute.SEND_MSG,
                ]),
                ColumnValue('publisher_handle'),
                ColumnValue('message'),
                ColumnValue('message_timestamp')
            ]
        )
        self.rclcpp_publish = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('rclcpp_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.NODE_IO,
                    ColumnAttribute.SEND_MSG,
                    ]),
                ColumnValue('publisher_handle'),
                ColumnValue('message'),
                ColumnValue('message_timestamp')
            ]
        )
        self.rcl_publish = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('rcl_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.OPTIONAL
                ]),
                ColumnValue('publisher_handle'),
                ColumnValue('message')
            ]
        )
        self.dispatch_subscription_callback = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('dispatch_subscription_callback_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('message'),
                ColumnValue('source_timestamp'),
                ColumnValue('message_timestamp')
            ]
        )
        self.dispatch_intra_process_subscription_callback = \
            RecordsFactory.create_instance(
                None,
                [
                    ColumnValue('pid'),
                    ColumnValue('tid'),
                    ColumnValue('dispatch_intra_process_subscription_callback_timestamp'),
                    ColumnValue('callback_object'),
                    ColumnValue('message'),
                    ColumnValue('message_timestamp')
                ]
            )
        self.message_construct = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('message_construct_timestamp'),
                ColumnValue('original_message'),
                ColumnValue('constructed_message')
            ]
        )

        self.tilde_subscribe = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('tilde_subscribe_timestamp'),
                ColumnValue('subscription'),
                ColumnValue('tilde_message_id')
            ]
        )

        self.tilde_publish = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('tilde_publish_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('publisher'),
                ColumnValue('message_info_id'),
                ColumnValue('message_id')
            ]
        )
        self.sim_time = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('system_time', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('sim_time')
            ]
        )
        self.timer_event = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('time_event_stamp', [ColumnAttribute.SYSTEM_TIME])
            ]
        )
        self.send_transform = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('send_transform_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('broadcaster'),
                ColumnValue('tf_timestamp'),
                ColumnValue('frame_id_compact'),
                ColumnValue('child_frame_id_compact'),
            ]
        )
        self.tf_lookup_transform_start = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('lookup_transform_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('tf_buffer_core'),
                ColumnValue('tf_lookup_target_time'),
                ColumnValue('frame_id_compact'),
                ColumnValue('child_frame_id_compact'),
            ]
        )
        self.tf_lookup_transform_end = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('lookup_transform_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('tf_buffer_core'),
            ]
        )
        self.tf_buffer_find_closest = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('find_closest_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('tf_buffer_core'),
                ColumnValue('frame_id_compact'),
                ColumnValue('child_frame_id_compact'),
                ColumnValue('stamp'),
            ]
        )
        self.tf_set_transform = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('set_transform_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('tf_buffer_core'),
                ColumnValue('tf_timestamp'),
                ColumnValue('frame_id_compact'),
                ColumnValue('child_frame_id_compact'),
            ]
        )
        self.ring_buffer_enqueue = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('enqueue_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('buffer'),
                ColumnValue('message'),
                ColumnValue('size'),
                ColumnValue('is_full'),
            ]
        )
        self.ring_buffer_dequeue = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('dequeue_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('buffer'),
                ColumnValue('size'),
                ColumnValue('message'),
            ]
        )
        self.ring_buffer_clear = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('clear_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('buffer'),
            ]
        )

    def add_rcl_init(self, pid, tid, context_handle, timestamp, version) -> None:
        record = {
            'context_handle': context_handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'version': version,  # Comment out to align with Dict[str: int64_t]
        }
        self.rcl_init.append(record)

    def add_rcl_node_init(
        self, pid, tid, node_handle, timestamp, rmw_handle, name, namespace
    ) -> None:
        record = {
            'node_handle': node_handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'rmw_handle': rmw_handle,
            'namespace': namespace,
            'name': name,
        }
        self.rcl_node_init.append(record)

    def add_rcl_publisher_init(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
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
        self.rcl_publisher_init.append(record)

    def add_rcl_subscription_init(
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
        self.rcl_subscription_init.append(record)

    def add_rclcpp_subscription_init(
        self, pid, tid, subscription_pointer, timestamp, subscription_handle
    ) -> None:
        record = {
            'subscription': subscription_pointer,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'subscription_handle': subscription_handle,
        }
        self.rclcpp_subscription_init.append(record)

    def add_rclcpp_subscription_callback_added(
        self, pid, tid, subscription_pointer, timestamp, callback_object
    ) -> None:
        record = {
            'subscription': subscription_pointer,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'callback_object': callback_object,
        }
        self.rclcpp_subscription_callback_added.append(record)

    def add_rcl_service_init(
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
        self.rcl_service_init.append(record)

    def add_rclcpp_service_callback_added(
        self, pid, tid, service_handle, timestamp, callback_object
    ) -> None:
        record = {
            'service_handle': service_handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'callback_object': callback_object,
        }
        self.rclcpp_service_callback_added.append(record)

    def add_rcl_client_init(
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
        self.rcl_client_init.append(record)

    def add_rcl_timer_init(
        self, pid, tid, handle, timestamp, period
    ) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'period': period,
            'pid': pid,
            'tid': tid,
        }
        self.rcl_timer_init.append(record)

    def add_rclcpp_timer_callback_added(
        self, pid, tid, timer_handle, timestamp, callback_object
    ) -> None:
        record = {
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'timer_handle': timer_handle,
            'callback_object': callback_object,
        }
        self.rclcpp_timer_callback_added.append(record)

    def add_rclcpp_timer_link_node(self, pid, tid, handle, timestamp, node_handle) -> None:
        record = {
            'timer_handle': handle,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
            'node_handle': node_handle,
        }
        self.rclcpp_timer_link_node.append(record)

    # def add_rclcpp_callback_register(
    #     self, pid, tid, callback_group_pointer, timestamp, callback_group_handle
    # ) -> None:
    #     record = {
    #         'callback_group': callback_group_pointer,
    #         'timestamp': timestamp,
    #         'pid': pid,
    #         'tid': tid,
    #         'callback_group_handle': callback_group_handle,
    #     }
    #     self.rclcpp_callback_register.append(record)

    def add_callback_start(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        callback: int,
        is_intra_process: bool,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'callback_start_timestamp': timestamp,
                'callback_object': callback,
                'is_intra_process': is_intra_process,
            }
        )
        self.callback_start.append(record)

    def add_callback_end(
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
        self.callback_end.append(record)

    def add_rcl_lifecycle_state_machine_init(self, pid, tid, handle, node_handle) -> None:
        record = {
            'state_machine_handle': handle,
            'node_handle': node_handle,
            'pid': pid,
            'tid': tid,
        }
        self.rcl_lifecycle_state_machine_init.append(record)

    def add_rcl_lifecycle_transition(
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
        self.rcl_lifecycle_transition.append(record)

    def add_rclcpp_publish(
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
        self.rclcpp_publish.append(record)

    def add_message_construct(
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
        self.message_construct.append(record)

    def add_rclcpp_intra_publish(
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
        self.rclcpp_intra_publish.append(record)

    def add_dispatch_subscription_callback(
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
        self.dispatch_subscription_callback.append(record)

    def add_dispatch_intra_process_subscription_callback(
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
        self.dispatch_intra_process_subscription_callback.append(record)

    def add_on_data_available(
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
        self.on_data_available.append(record)

    def add_rcl_publish(
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
        self.rcl_publish.append(record)

    def add_dds_write(
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
        self.dds_write.append(record)

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

    def add_rmw_implementation(self, pid: int, tid: int, rmw_impl: str):
        self.rmw_implementation.append({
            'rmw_impl': rmw_impl,
            'pid': pid,
            'tid': tid,
        })

    def add_add_callback_group(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        self.add_callback_group.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        })

    def add_add_callback_group_static_executor(
        self,
        pid: int,
        tid: int,
        entities_collector_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        self.add_callback_group_static_executor.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'entities_collector_addr': entities_collector_addr,
            'callback_group_addr': callback_group_addr,
            'group_type_name': group_type_name
        })

    def add_construct_executor(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        self.construct_executor.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'executor_type_name': executor_type_name,
        })

    def add_construct_static_executor(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        entities_collector_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        self.construct_static_executor.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'executor_addr': executor_addr,
            'entities_collector_addr': entities_collector_addr,
            'executor_type_name': executor_type_name,
        })

    def add_callback_group_add_timer(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        timer_handle: int
    ) -> None:
        self.callback_group_add_timer.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'timer_handle': timer_handle,
        })

    def add_callback_group_add_subscription(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        subscription_handle: int
    ) -> None:
        self.callback_group_add_subscription.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'subscription_handle': subscription_handle,
        })

    def add_callback_group_add_service(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        service_handle: int
    ) -> None:
        self.callback_group_add_service.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'service_handle': service_handle,
        })

    def add_callback_group_add_client(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        client_handle: int
    ) -> None:
        self.callback_group_add_client.append({
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp,
            'callback_group_addr': callback_group_addr,
            'client_handle': client_handle,
        })

    def add_tilde_subscription_init(
        self, pid, tid, subscription, node_name, topic_name, timestamp
    ) -> None:
        self.tilde_subscription_init.append({
            'subscription': subscription,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
        })

    def add_tilde_publisher_init(
        self, pid, tid, publisher, node_name, topic_name, timestamp
    ) -> None:
        self.tilde_publisher_init.append({
            'publisher': publisher,
            'node_name': node_name,
            'topic_name': topic_name,
            'timestamp': timestamp,
            'pid': pid,
            'tid': tid,
        })

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

    def add_tilde_subscribe_added(
        self, pid, tid, subscription_id, node_name, topic_name, timestamp
    ) -> None:
        self.tilde_subscribe_added.append({
            'subscription_id': subscription_id,
            'node_name': node_name,
            'topic_name': topic_name,
            'pid': pid,
            'tid': tid,
            'timestamp': timestamp
        })

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

    def add_symbol_rename(
        self, pid: int, tid: int, timestamp: int, symbol_from: str, symbol_to: str
    ):
        self.symbol_rename.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'symbol_from': symbol_from,
                'symbol_to': symbol_to
            }
        )

    def add_init_bind_transform_broadcaster(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        broadcaster: int,
        publisher_handle: int,
    ) -> None:
        self.transform_broadcaster.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'transform_broadcaster': broadcaster,
                'publisher_handle': publisher_handle
            }
        )

    def add_init_bind_transform_broadcaster_frames(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        transform_broadcaster: int,
        frame_id: str,
        child_frame_id: str
    ) -> None:
        self.transform_broadcaster_frames.append(
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
        self.construct_tf_buffer.append(
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
        self.init_bind_tf_buffer_core.append(
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
        self.construct_node_hook.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'node_handle': node_handle,
                'clock': clock
            }
        )

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

    def add_init_tf_broadcaster_frame_id_compact(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        broadcaster: int,
        frame_id: str,
        frame_id_compact: int
    ) -> None:
        self.init_tf_broadcaster_frame_id_compact.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_broadcaster': broadcaster,
                'frame_id': frame_id,
                'frame_id_compact': frame_id_compact,
            }
        )

    def add_init_tf_buffer_frame_id_compact(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        frame_id: str,
        frame_id_compact: int
    ) -> None:
        self.init_tf_buffer_frame_id_compact.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'frame_id': frame_id,
                'frame_id_compact': frame_id_compact,
            }
        )

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

    # def add_callback_object(self, pid, tid, reference, timestamp, callback_object) -> None:
    #     record = {
    #         'reference': reference,
    #         'timestamp': timestamp,
    #         'callback_object': callback_object,
    #         'pid': pid,
    #         'tid': tid,
    #     }
    #     self.callback_objects.append(record)

    def add_rclcpp_callback_register(
        self, pid, tid, callback_object, timestamp, symbol
    ) -> None:
        record = {
            'callback_object': callback_object,
            'timestamp': timestamp,
            'symbol': symbol,
            'pid': pid,
            'tid': tid,
        }
        self.rclcpp_callback_register.append(record)

    def add_tf_buffer_find_closest(
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
        self.tf_buffer_find_closest.append(record)
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
            self.tf_buffer_find_closest.append(record)

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

    def add_init_tf_buffer_lookup_transform(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        frame_id_compact: int,
        child_frame_id_compact: int,
    ) -> None:

        self.tf_buffer_lookup_transform.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'frame_id_compact': frame_id_compact,
                'child_frame_id_compact': child_frame_id_compact,
            }
        )

    def add_construct_ipm(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        ipm: int,
    ) -> None:
        self.construct_ipm.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'ipm': ipm,
            }
        )

    def add_ipm_add_publisher(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        ipm: int,
        publisher_handle: int,
        pub_id: int,
    ) -> None:
        self.ipm_add_publisher.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'ipm': ipm,
                'publisher_handle': publisher_handle,
                'pub_id': pub_id,
            }
        )

    def add_ipm_add_subscription(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        ipm: int,
        subscription_handle: int,
        sub_id: int,
    ) -> None:
        self.ipm_add_subscription.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'ipm': ipm,
                'subscription_handle': subscription_handle,
                'sub_id': sub_id,
            }
        )

    def add_ipm_insert_sub_id_for_pub(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        ipm: int,
        sub_id: int,
        pub_id: int,
        use_take_shared_method: int
    ) -> None:
        self.ipm_insert_sub_id_for_pub.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'ipm': ipm,
                'sub_id': sub_id,
                'pub_id': pub_id,
                'use_take_shared_method': use_take_shared_method,
            }
        )

    def add_construct_ring_buffer(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer: int,
        capacity: int
    ) -> None:
        self.construct_ring_buffer.append(
            {
                'pid': pid,
                'tid': tid,
                'timestamp': timestamp,
                'buffer': buffer,
                'capacity': capacity,
            }
        )

    def add_ring_buffer_enqueue(
        self,
        pid: int,
        tid: int,
        enqueue_timestamp: int,
        buffer: int,
        message: int,
        size: int,
        is_full: int
    ) -> None:
        self.ring_buffer_enqueue.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'enqueue_timestamp': enqueue_timestamp,
                    'buffer': buffer,
                    'message': message,
                    'size': size,
                    'is_full': is_full,
                }
            )
        )

    def add_ring_buffer_dequeue(
        self,
        pid: int,
        tid: int,
        dequeue_timestamp: int,
        buffer: int,
        message: int,
        size: int,
    ) -> None:
        self.ring_buffer_dequeue.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'dequeue_timestamp': dequeue_timestamp,
                    'buffer': buffer,
                    'message': message,
                    'size': size,
                }
            )
        )

    def add_ring_buffer_clear(
        self,
        pid: int,
        tid: int,
        clear_timestamp: int,
        buffer: int,
    ) -> None:
        self.ring_buffer_clear.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'clear_timestamp': clear_timestamp,
                    'buffer': buffer,
                }
            )
        )

    def _finalize(self) -> None:
        self.rcl_init.finalize()
        self.rcl_node_init.finalize()
        self.rcl_publisher_init.finalize()
        self.rcl_subscription_init.finalize()
        self.rclcpp_subscription_init.finalize()
        self.rclcpp_subscription_callback_added.finalize()
        self.rcl_service_init.finalize()
        self.rclcpp_service_callback_added.finalize()
        self.rcl_client_init.finalize()
        self.rcl_timer_init.finalize()
        self.rclcpp_timer_callback_added.finalize()
        self.rclcpp_timer_link_node.finalize()

        self.construct_tf_buffer.finalize()
        self.init_bind_tf_buffer_core.finalize()
        self.tf_buffer_lookup_transform.finalize()
        self.construct_node_hook.finalize()
        self.symbol_rename.finalize()
        symbol_map = {
            symbol['symbol_from']: symbol['symbol_to']
            for _, symbol
            in self.symbol_rename.df.iterrows()
        }
        self.rclcpp_callback_register.replace('symbol', symbol_map)
        self.rclcpp_callback_register.finalize()
        self.init_tf_broadcaster_frame_id_compact.finalize()
        self.transform_broadcaster.finalize()
        self.transform_broadcaster_frames.finalize()
        self.init_tf_buffer_frame_id_compact.finalize()
        # self.lifecycle_state_machines.finalize()
        # self.lifecycle_transitions.finalize()
        self.construct_executor.finalize()
        self.construct_static_executor.finalize()
        self.add_callback_group.finalize()
        self.add_callback_group_static_executor.finalize()
        self.callback_group_add_timer.finalize()
        self.callback_group_add_subscription.finalize()
        self.callback_group_add_service.finalize()
        self.callback_group_add_client.finalize()
        self.tilde_subscription_init.finalize()
        self.tilde_publisher_init.finalize()
        self.tilde_subscribe_added.finalize()
        self.rmw_implementation.finalize()
        self.construct_ipm.finalize()
        self.ipm_add_publisher.finalize()
        self.ipm_add_subscription.finalize()
        self.ipm_insert_sub_id_for_pub.finalize()
        self.construct_ring_buffer.finalize()

    def print_data(self) -> None:
        raise NotImplementedError('')
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
        print(self.rclcpp_callback_register.to_string())
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
