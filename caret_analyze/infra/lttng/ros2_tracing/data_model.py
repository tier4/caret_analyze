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

from typing import Optional

from caret_analyze.record.column import ColumnAttribute, ColumnMapper, ColumnValue
from caret_analyze.record.record_factory import RecordFactory, RecordsFactory

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
        self.rcl_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('context_handle'),
                ColumnValue('version', mapper=ColumnMapper())
            ]
        )
        self.rcl_node_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('node_handle'),
                ColumnValue('namespace', mapper=ColumnMapper()),
                ColumnValue('name', mapper=ColumnMapper()),
                ColumnValue('rmw_handle'),
            ]
        )
        self.rcl_publisher_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('publisher_handle'),
                ColumnValue('node_handle'),
                ColumnValue('rmw_handle'),
                ColumnValue('topic_name', mapper=ColumnMapper()),
                ColumnValue('depth')
            ]
        )
        self.rcl_subscription_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('subscription_handle'),
                ColumnValue('node_handle'),
                ColumnValue('rmw_handle'),
                ColumnValue('topic_name', mapper=ColumnMapper()),
                ColumnValue('depth'),
            ]
        )
        self.rclcpp_subscription_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('subscription'),
                ColumnValue('subscription_handle'),
            ]
        )
        self.rcl_service_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('service_handle'),
                ColumnValue('timestamp'),
                ColumnValue('node_handle'),
                ColumnValue('rmw_handle'),
                ColumnValue('service_name', mapper=ColumnMapper()),
            ]
        )
        self.rcl_client_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('client_handle'),
                ColumnValue('timestamp'),
                ColumnValue('node_handle'),
                ColumnValue('rmw_handle'),
                ColumnValue('service_name', mapper=ColumnMapper()),
            ]
        )
        self.rcl_timer_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('timer_handle'),
                ColumnValue('period'),
            ]
        )
        self.rclcpp_timer_link_node = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('timer_handle'),
                ColumnValue('node_handle'),
            ]
        )
        self.rclcpp_subscription_callback_added = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('subscription'),
                ColumnValue('timestamp'),
                ColumnValue('callback_object'),
            ]
        )
        self.rclcpp_service_callback_added = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('service_handle'),
                ColumnValue('timestamp'),
                ColumnValue('callback_object'),
            ]
        )
        self.rclcpp_timer_callback_added = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('timer_handle'),
                ColumnValue('callback_object'),
            ]
        )
        self.rclcpp_callback_register = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_object'),
                ColumnValue('timestamp'),
                ColumnValue('symbol', mapper=ColumnMapper()),
            ]
        )
        self.rcl_lifecycle_state_machine_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('state_machine_handle'),
                ColumnValue('node_handle'),
            ]
        )
        self.construct_executor = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('executor_addr'),
                ColumnValue('executor_type_name', mapper=ColumnMapper()),
            ]
        )
        self.construct_static_executor = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('executor_addr'),
                ColumnValue('entities_collector_addr'),
                ColumnValue('executor_type_name', mapper=ColumnMapper()),
            ]
        )
        self.add_callback_group = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('executor_addr'),
                ColumnValue('callback_group_addr'),
                ColumnValue('group_type_name', mapper=ColumnMapper()),
            ]
        )
        self.add_callback_group_static_executor = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('entities_collector_addr'),
                ColumnValue('callback_group_addr'),
                ColumnValue('group_type_name', mapper=ColumnMapper()),
            ]
        )
        self.callback_group_add_timer = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('callback_group_addr'),
                ColumnValue('timer_handle'),
            ]
        )
        self.callback_group_add_subscription = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('callback_group_addr'),
                ColumnValue('subscription_handle'),
            ]
        )
        self.callback_group_add_service = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('callback_group_addr'),
                ColumnValue('service_handle'),
            ]
        )
        self.callback_group_add_client = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('callback_group_addr'),
                ColumnValue('client_handle'),
            ]
        )
        self.rmw_implementation = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('rmw_impl', mapper=ColumnMapper()),
            ]
        )
        self.symbol_rename = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('symbol_from', mapper=ColumnMapper()),
                ColumnValue('symbol_to', mapper=ColumnMapper()),
            ]
        )
        self.tilde_subscription_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('subscription'),
                ColumnValue('node_name', mapper=ColumnMapper()),
                ColumnValue('topic_name', mapper=ColumnMapper()),
                ColumnValue('timestamp'),
            ]
        )
        self.tilde_publisher_init = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('publisher'),
                ColumnValue('node_name', mapper=ColumnMapper()),
                ColumnValue('topic_name', mapper=ColumnMapper()),
                ColumnValue('timestamp'),
            ]
        )
        self.tilde_subscribe_added = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('subscription_id'),
                ColumnValue('node_name', mapper=ColumnMapper()),
                ColumnValue('topic_name', mapper=ColumnMapper()),
                ColumnValue('timestamp'),
            ]
        )
        self.transform_broadcaster = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('transform_broadcaster'),
                ColumnValue('publisher_handle'),
            ]
        )
        self.transform_broadcaster_frames = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('transform_broadcaster'),
                ColumnValue('frame_id', mapper=ColumnMapper()),
                ColumnValue('child_frame_id', mapper=ColumnMapper()),
            ]
        )
        self.construct_tf_buffer = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_buffer'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('clock'),
            ]
        )
        self.init_bind_tf_buffer_core = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('callback'),
            ]
        )

        self.construct_node_hook = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('node_handle'),
                ColumnValue('clock'),
            ]
        )
        self.init_tf_broadcaster_frame_id_compact = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_broadcaster'),
                ColumnValue('frame_id', mapper=ColumnMapper()),
                ColumnValue('frame_id_compact'),
            ]
        )
        self.init_tf_buffer_frame_id_compact = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('frame_id', mapper=ColumnMapper()),
                ColumnValue('frame_id_compact'),
            ]
        )
        self.tf_buffer_set_transform = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('frame_id', mapper=ColumnMapper()),
                ColumnValue('child_frame_id', mapper=ColumnMapper()),
            ]
        )
        self.tf_buffer_lookup_transform = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('target_frame_id', mapper=ColumnMapper()),
                ColumnValue('source_frame_id', mapper=ColumnMapper()),
            ]
        )
        self.construct_ipm = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('ipm'),
            ]
        )
        self.ipm_add_publisher = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('ipm'),
                ColumnValue('timestamp'),
                ColumnValue('publisher_handle'),
                ColumnValue('pub_id'),
            ]
        )
        self.ipm_add_subscription = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('ipm'),
                ColumnValue('timestamp'),
                ColumnValue('subscription_handle'),
                ColumnValue('sub_id'),
            ]
        )
        self.ipm_insert_sub_id_for_pub = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('ipm'),
                ColumnValue('timestamp'),
                ColumnValue('sub_id'),
                ColumnValue('pub_id'),
                ColumnValue('use_take_shared_method'),
            ]
        )
        self.construct_ring_buffer = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('timestamp'),
                ColumnValue('buffer'),
                ColumnValue('capacity'),
            ]
        )
        self._raw_offset: Optional[int] = None

        # for v0.2 compatibility
        self.callback_start = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_start_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
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
                ColumnValue('rclcpp_inter_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
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
        self.tf_lookup_transform_start = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('lookup_transform_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('tf_buffer_core'),
                ColumnValue('tf_lookup_target_time'),
                ColumnValue('target_frame_id_compact'),
                ColumnValue('source_frame_id_compact'),
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
        self.message_construct = RecordsFactory.create_instance(
            None, [
                ColumnValue('pid'),
                ColumnValue('message_construct_timestamp'),
                ColumnValue('original_message'),
                ColumnValue('constructed_message')
            ]
        )

        self.lifecycle_transitions = RecordsFactory.create_instance(
            None, [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('state_machine'),
                ColumnValue('start_label', mapper=ColumnMapper()),
                ColumnValue('goal_label', mapper=ColumnMapper()),
            ]
        )

        # Events (multiple instances, may not have a meaningful index)
        self.intra_callback_duration = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_object'),
                ColumnValue('callback_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('callback_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('message_timestamp', []),
            ]
        )
        self.inter_callback_duration = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('callback_object'),
                ColumnValue('callback_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('callback_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('source_timestamp', []),
                ColumnValue('message_timestamp', []),
            ]
        )
        self.inter_publish = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('publisher_handle'),
                ColumnValue('rclcpp_inter_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                ]),
                ColumnValue('rcl_publish_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.OPTIONAL
                ]),
                ColumnValue('dds_write_timestamp', [
                    ColumnAttribute.SYSTEM_TIME,
                    ColumnAttribute.OPTIONAL,
                ]),
                ColumnValue('source_timestamp'),
                ColumnValue('message_timestamp')
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
        self.tf_lookup_transform = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('pid'),
                ColumnValue('tid'),
                ColumnValue('tf_buffer_core'),
                ColumnValue('lookup_transform_start_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('lookup_transform_end_timestamp', [ColumnAttribute.SYSTEM_TIME]),
                ColumnValue('target_frame_id_compact'),
                ColumnValue('source_frame_id_compact'),
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
                ColumnValue('index'),
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
                ColumnValue('index'),
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

    def set_offset(self, timestamp: int, timestamp_raw: int) -> None:
        self._raw_offset = timestamp - timestamp_raw

    def _to_system_time(self, raw_timestamp: int) -> int:
        assert self._raw_offset is not None
        return raw_timestamp + self._raw_offset

    def add_rcl_init(self, pid, tid, context_handle, timestamp, version) -> None:
        version_idx = len(self.rcl_init)
        mapper = self.rcl_init.columns.get('version').mapper
        assert mapper is not None
        mapper.add(version_idx, version)
        self.rcl_init.append(
            RecordFactory.create_instance(
                {
                    'context_handle': context_handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'version': version_idx,
                }
            )
        )

    def add_rcl_node_init(
        self, pid, tid, node_handle, timestamp, rmw_handle, name, namespace
    ) -> None:
        record_idx = len(self.rcl_node_init)
        ns_mapper = self.rcl_node_init.columns.get('namespace').mapper
        name_mapper = self.rcl_node_init.columns.get('name').mapper
        assert ns_mapper is not None and name_mapper is not None
        ns_mapper.add(record_idx, namespace)
        name_mapper.add(record_idx, name)

        self.rcl_node_init.append(
            RecordFactory.create_instance(
                {
                    'node_handle': node_handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'rmw_handle': rmw_handle,
                    'namespace': record_idx,
                    'name': record_idx,
                }
            )
        )

    def add_rcl_publisher_init(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
        topic_name_idx = len(self.rcl_publisher_init)
        mapper = self.rcl_publisher_init.columns.get('topic_name').mapper
        assert mapper is not None
        mapper.add(topic_name_idx, topic_name)
        self.rcl_publisher_init.append(
            RecordFactory.create_instance(
                {
                    'publisher_handle': handle,
                    'timestamp': timestamp,
                    'node_handle': node_handle,
                    'rmw_handle': rmw_handle,
                    'pid': pid,
                    'tid': tid,
                    'topic_name': topic_name_idx,
                    'depth': depth,
                }
            )
        )

    def add_rcl_subscription_init(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, topic_name, depth
    ) -> None:
        record_idx = len(self.rcl_subscription_init)
        mapper = self.rcl_subscription_init.columns.get('topic_name').mapper
        assert mapper is not None
        mapper.add(record_idx, topic_name)

        self.rcl_subscription_init.append(
            RecordFactory.create_instance(
                {
                    'subscription_handle': handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'node_handle': node_handle,
                    'rmw_handle': rmw_handle,
                    'topic_name': record_idx,
                    'depth': depth,
                }
            )
        )

    def add_rclcpp_subscription_init(
        self, pid, tid, subscription_pointer, timestamp, subscription_handle
    ) -> None:
        self.rclcpp_subscription_init.append(
            RecordFactory.create_instance(
                {
                    'subscription': subscription_pointer,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'subscription_handle': subscription_handle,
                }
            )
        )

    def add_rclcpp_subscription_callback_added(
        self, pid, tid, subscription_pointer, timestamp, callback_object
    ) -> None:
        self.rclcpp_subscription_callback_added.append(
            RecordFactory.create_instance(
                {
                    'subscription': subscription_pointer,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'callback_object': callback_object,
                }
            )
        )

    def add_rcl_service_init(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, service_name
    ) -> None:
        record_idx = len(self.rcl_service_init)
        mapper = self.rcl_service_init.columns.get('service_name').mapper
        assert mapper is not None
        mapper.add(record_idx, service_name)

        self.rcl_service_init.append(
            RecordFactory.create_instance(
                {
                    'service_handle': handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'node_handle': node_handle,
                    'rmw_handle': rmw_handle,
                    'service_name': record_idx,
                }
            )
        )

    def add_rclcpp_service_callback_added(
        self, pid, tid, service_handle, timestamp, callback_object
    ) -> None:
        self.rclcpp_service_callback_added.append(
            RecordFactory.create_instance(
                {
                    'service_handle': service_handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'callback_object': callback_object,
                }
            )
        )

    def add_rcl_client_init(
        self, pid, tid, handle, timestamp, node_handle, rmw_handle, service_name
    ) -> None:
        self.rcl_client_init.append(
            RecordFactory.create_instance(
                {
                    'client_handle': handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'node_handle': node_handle,
                    'rmw_handle': rmw_handle,
                    'service_name': service_name,
                }
            )
        )

    def add_rcl_timer_init(
        self, pid, tid, handle, timestamp, period
    ) -> None:
        self.rcl_timer_init.append(
            RecordFactory.create_instance(
                {
                    'timer_handle': handle,
                    'timestamp': timestamp,
                    'period': period,
                    'pid': pid,
                    'tid': tid,
                }
            )
        )

    def add_rclcpp_timer_callback_added(
        self, pid, tid, timer_handle, timestamp, callback_object
    ) -> None:
        self.rclcpp_timer_callback_added.append(
            RecordFactory.create_instance(
                {
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'timer_handle': timer_handle,
                    'callback_object': callback_object,
                }
            )
        )

    def add_rclcpp_timer_link_node(self, pid, tid, handle, timestamp, node_handle) -> None:
        self.rclcpp_timer_link_node.append(
            RecordFactory.create_instance(
                {
                    'timer_handle': handle,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                    'node_handle': node_handle,
                }
            )
        )

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

    def add_intra_callback_duration(
        self,
        pid: int,
        tid: int,
        callback_end_timestamp: int,
        callback: int,
        callback_start_timestamp_raw: int,
        callback_end_timestamp_raw: int,
        message_timestamp: int,
    ) -> None:
        self.set_offset(callback_end_timestamp, callback_end_timestamp_raw)

        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'callback_object': callback,
                'callback_start_timestamp': self._to_system_time(callback_start_timestamp_raw),
                'callback_end_timestamp': callback_end_timestamp,
                'message_timestamp': message_timestamp
            }
        )
        self.intra_callback_duration.append(record)

    def add_inter_callback_duration(
        self,
        pid: int,
        tid: int,
        callback_end_timestamp: int,
        callback: int,
        callback_start_timestamp_raw: int,
        callback_end_timestamp_raw: int,
        source_timestamp: int,
        message_timestamp: int,
    ) -> None:
        self.set_offset(callback_end_timestamp, callback_end_timestamp_raw)

        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'callback_object': callback,
                'callback_start_timestamp': self._to_system_time(callback_start_timestamp_raw),
                'callback_end_timestamp': callback_end_timestamp,
                'source_timestamp': source_timestamp,
                'message_timestamp': message_timestamp
            }
        )

        self.inter_callback_duration.append(record)

    def add_inter_publish(
        self,
        pid: int,
        tid: int,
        publisher_handle: int,
        rclcpp_publish_timestamp_raw: int,
        rcl_publish_timestamp_raw: Optional[int],
        dds_write_timestamp_raw: Optional[int],
        source_timestamp: int,
        message_timestamp: int,
    ) -> None:
        if self._raw_offset is None:
            return

        d = {
            'pid': pid,
            'tid': tid,
            'publisher_handle': publisher_handle,
            'rclcpp_inter_publish_timestamp':
                self._to_system_time(rclcpp_publish_timestamp_raw),
            'source_timestamp': source_timestamp,
            'message_timestamp': message_timestamp,
        }
        if rcl_publish_timestamp_raw is not None:
            d['rcl_publish_timestamp'] = self._to_system_time(rcl_publish_timestamp_raw)

        if dds_write_timestamp_raw is not None:
            d['dds_write_timestamp'] = self._to_system_time(dds_write_timestamp_raw)

        record = RecordFactory.create_instance(d)

        self.inter_publish.append(record)

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
        pass
        # TODO(hsgwa): implement this
        # record = {
        #     'state_machine_handle': state_machine_handle,
        #     'start_label': start_label,
        #     'goal_label': goal_label,
        #     'timestamp': timestamp,
        #     'pid': pid,
        #     'tid': tid,
        # }
        # self.rcl_lifecycle_transition.append(record)

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
                'rclcpp_inter_publish_timestamp': timestamp,
                'publisher_handle': publisher_handle,
                'message': message,
                'message_timestamp': message_timestamp,
            }
        )
        self.rclcpp_publish.append(record)

    def add_message_construct(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        original_message: int,
        constructed_message: int,
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
        rmw_impl_index = len(self.rmw_implementation)
        mapper = self.rmw_implementation.columns.get('rmw_impl').mapper
        assert mapper is not None
        mapper.add(rmw_impl_index, rmw_impl)

        self.rmw_implementation.append(
            RecordFactory.create_instance(
                {
                    'rmw_impl': rmw_impl_index,
                    'pid': pid,
                    'tid': tid,
                }
            )
        )

    def add_add_callback_group(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        record_idx = len(self.add_callback_group)
        mapper = self.add_callback_group.columns.get('group_type_name').mapper
        assert mapper is not None
        mapper.add(record_idx, group_type_name)

        self.add_callback_group.append(
            RecordFactory.create_instance(

                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'executor_addr': executor_addr,
                    'callback_group_addr': callback_group_addr,
                    'group_type_name': record_idx
                }
            )
        )

    def add_add_callback_group_static_executor(
        self,
        pid: int,
        tid: int,
        entities_collector_addr: int,
        timestamp: int,
        callback_group_addr: int,
        group_type_name: str
    ) -> None:
        self.add_callback_group_static_executor.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'entities_collector_addr': entities_collector_addr,
                    'callback_group_addr': callback_group_addr,
                    'group_type_name': group_type_name
                }
            )
        )

    def add_construct_executor(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        executor_type_name_idx = len(self.construct_executor)
        mapper = self.construct_executor.columns.get('executor_type_name').mapper
        assert mapper is not None
        mapper.add(executor_type_name_idx, executor_type_name)
        self.construct_executor.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'executor_addr': executor_addr,
                    'executor_type_name': executor_type_name_idx,
                }
            )
        )

    def add_construct_static_executor(
        self,
        pid: int,
        tid: int,
        executor_addr: int,
        entities_collector_addr: int,
        timestamp: int,
        executor_type_name: str
    ) -> None:
        record_idx = len(self.construct_static_executor)
        mapper = self.construct_static_executor.columns.get('frame_id').mapper
        assert mapper is not None

        mapper.add(record_idx, executor_type_name)

        self.construct_static_executor.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'executor_addr': executor_addr,
                    'entities_collector_addr': entities_collector_addr,
                    'executor_type_name': record_idx,
                }
            )
        )

    def add_callback_group_add_timer(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        timer_handle: int
    ) -> None:
        self.callback_group_add_timer.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'callback_group_addr': callback_group_addr,
                    'timer_handle': timer_handle,
                }
            )
        )

    def add_callback_group_add_subscription(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        subscription_handle: int
    ) -> None:
        self.callback_group_add_subscription.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'callback_group_addr': callback_group_addr,
                    'subscription_handle': subscription_handle,
                }
            )
        )

    def add_callback_group_add_service(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        service_handle: int
    ) -> None:
        self.callback_group_add_service.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'callback_group_addr': callback_group_addr,
                    'service_handle': service_handle,
                }
            )
        )

    def add_callback_group_add_client(
        self,
        pid: int,
        tid: int,
        callback_group_addr: int,
        timestamp: int,
        client_handle: int
    ) -> None:
        self.callback_group_add_client.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'callback_group_addr': callback_group_addr,
                    'client_handle': client_handle,
                }
            )
        )

    def add_tilde_subscription_init(
        self, pid, tid, subscription, node_name, topic_name, timestamp
    ) -> None:
        self.tilde_subscription_init.append(
            RecordFactory.create_instance(
                {
                    'subscription': subscription,
                    'node_name': node_name,
                    'topic_name': topic_name,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                }
            )
        )

    def add_tilde_publisher_init(
        self, pid, tid, publisher, node_name, topic_name, timestamp
    ) -> None:
        self.tilde_publisher_init.append(
            RecordFactory.create_instance(
                {
                    'publisher': publisher,
                    'node_name': node_name,
                    'topic_name': topic_name,
                    'timestamp': timestamp,
                    'pid': pid,
                    'tid': tid,
                }
            )
        )

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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'symbol_from': symbol_from,
                    'symbol_to': symbol_to,
                }
            )
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
            RecordFactory.create_instance
            (
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'transform_broadcaster': broadcaster,
                    'publisher_handle': publisher_handle
                }
            )
        )

    def add_init_bind_tf_broadcaster_send_transform(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        transform_broadcaster: int,
        frame_id: str,
        child_frame_id: str
    ) -> None:
        record_idx = len(self.transform_broadcaster_frames)
        mapper = self.transform_broadcaster_frames.columns.get('frame_id').mapper
        mapper_child = self.transform_broadcaster_frames.columns.get('child_frame_id').mapper
        assert mapper is not None and mapper_child is not None

        mapper.add(record_idx, frame_id)
        mapper_child.add(record_idx, child_frame_id)

        self.transform_broadcaster_frames.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'transform_broadcaster': transform_broadcaster,
                    'frame_id': record_idx,
                    'child_frame_id': record_idx,
                }
            )
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_buffer': tf_buffer,
                    'tf_buffer_core': tf_buffer_core,
                    'clock': clock
                }
            )
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_buffer_core': tf_buffer_core,
                    'callback': callback,
                }
            )
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'node_handle': node_handle,
                    'clock': clock
                }
            )
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
        record_idx = len(self.init_tf_broadcaster_frame_id_compact)
        mapper = self.init_tf_broadcaster_frame_id_compact.columns.get('frame_id').mapper
        assert mapper is not None
        mapper.add(record_idx, frame_id)
        self.init_tf_broadcaster_frame_id_compact.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_broadcaster': broadcaster,
                    'frame_id': record_idx,
                    'frame_id_compact': frame_id_compact,
                }
            )
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
        record_idx = len(self.init_tf_buffer_frame_id_compact)
        mapper = self.init_tf_buffer_frame_id_compact.columns.get('frame_id').mapper
        assert mapper is not None
        mapper.add(record_idx, frame_id)
        self.init_tf_buffer_frame_id_compact.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_buffer_core': buffer_core,
                    'frame_id': record_idx,
                    'frame_id_compact': frame_id_compact,
                }
            )
        )

    def add_tf_lookup_transform(
        self,
        pid: int,
        tid: int,
        buffer_core: int,
        lookup_transform_timestamp_start_raw: int,
        lookup_transform_timestamp_end: int,
        target_frame_id_compact: int,
        source_frame_id_compact: int,
    ) -> None:
        if self._raw_offset is None:
            return

        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'tf_buffer_core': buffer_core,
                'lookup_transform_start_timestamp':
                    self._to_system_time(lookup_transform_timestamp_start_raw),
                'lookup_transform_end_timestamp': lookup_transform_timestamp_end,
                'target_frame_id_compact': target_frame_id_compact,
                'source_frame_id_compact': source_frame_id_compact,
            }
        )

        self.tf_lookup_transform.append(record)

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
        record_idx = len(self.rclcpp_callback_register)
        mapper = self.rclcpp_callback_register.columns.get('symbol').mapper
        assert mapper is not None
        mapper.add(record_idx, symbol)

        self.rclcpp_callback_register.append(
            RecordFactory.create_instance(
                {
                    'callback_object': callback_object,
                    'timestamp': timestamp,
                    'symbol': record_idx,
                    'pid': pid,
                    'tid': tid,
                }
            )
        )

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
        target_frame_id: str,
        source_frame_id: str,
    ) -> None:
        record_idx = len(self.tf_buffer_lookup_transform)
        mapper = self.tf_buffer_lookup_transform.columns.get('target_frame_id').mapper
        mapper_source = self.tf_buffer_lookup_transform.columns.get('source_frame_id').mapper
        assert mapper is not None and mapper_source is not None

        mapper.add(record_idx, target_frame_id)
        mapper_source.add(record_idx, source_frame_id)

        self.tf_buffer_lookup_transform.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_buffer_core': buffer_core,
                    'target_frame_id': record_idx,
                    'source_frame_id': record_idx,
                }
            )
        )

    def add_init_tf_buffer_set_transform(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        frame_id: str,
        child_frame_id: str,
    ) -> None:
        record_idx = len(self.tf_buffer_set_transform)
        mapper = self.tf_buffer_set_transform.columns.get('frame_id').mapper
        mapper_child = self.tf_buffer_set_transform.columns.get('child_frame_id').mapper
        assert mapper is not None and mapper_child is not None

        mapper.add(record_idx, frame_id)
        mapper_child.add(record_idx, child_frame_id)

        self.tf_buffer_set_transform.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'tf_buffer_core': buffer_core,
                    'frame_id': record_idx,
                    'child_frame_id': record_idx,
                }
            )
        )

    def add_construct_ipm(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        ipm: int,
    ) -> None:
        self.construct_ipm.append(
            RecordFactory.create_instance
            (
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'ipm': ipm,
                }
            )
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'ipm': ipm,
                    'publisher_handle': publisher_handle,
                    'pub_id': pub_id,
                }
            )
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'ipm': ipm,
                    'subscription_handle': subscription_handle,
                    'sub_id': sub_id,
                }
            )
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
            RecordFactory.create_instance(
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
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'timestamp': timestamp,
                    'buffer': buffer,
                    'capacity': capacity,
                }
            )
        )

    def add_ring_buffer_enqueue(
        self,
        pid: int,
        tid: int,
        enqueue_timestamp: int,
        buffer: int,
        index: int,
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
                    'index': index,
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
        index: int,
        size: int,
    ) -> None:
        self.ring_buffer_dequeue.append(
            RecordFactory.create_instance(
                {
                    'pid': pid,
                    'tid': tid,
                    'dequeue_timestamp': dequeue_timestamp,
                    'buffer': buffer,
                    'index': index,
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

    def add_tf_lookup_transform_start(
        self,
        pid: int,
        tid: int,
        timestamp: int,
        buffer_core: int,
        target_time: int,
        target_frame_id_compact: int,
        source_frame_id_compact: int,
    ) -> None:
        record = RecordFactory.create_instance(
            {
                'pid': pid,
                'tid': tid,
                'lookup_transform_start_timestamp': timestamp,
                'tf_buffer_core': buffer_core,
                'tf_lookup_target_time': target_time,
                'target_frame_id_compact': target_frame_id_compact,
                'source_frame_id_compact': source_frame_id_compact,
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

    def _finalize(self) -> None:
        symbol_map = {
            symbol['symbol_from']: symbol['symbol_to']
            for _, symbol
            in self.symbol_rename.to_dataframe().iterrows()
        }
        while True:
            old_column_names = tuple(self.rclcpp_callback_register.column_names)
            self.rclcpp_callback_register.columns.rename(symbol_map)
            new_column_names = tuple(self.rclcpp_callback_register.column_names)
            if old_column_names == new_column_names:
                break

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
