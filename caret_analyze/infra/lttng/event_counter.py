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

from __future__ import annotations

from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .lttng_info import LttngInfo
from .ros2_tracing.data_model import Ros2DataModel
from .ros2_tracing.processor import Ros2Handler
from .value_objects import (
    CallbackGroupValueLttng,
    ClientCallbackValueLttng,
    NodeValueLttng,
    PublisherValueLttng,
    ServiceCallbackValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from ...exceptions import InvalidArgumentError, InvalidTraceFormatError, Error
from ...record import RecordsInterface

logger = getLogger(__name__)


class Category:

    def __init__(self):
        self.node_name = None
        self.topic_name = None
        self.tracepoint = None


ServerKey = Tuple[int, int]


class CategoryServer:

    keys = {
        'node_handle',
        'timer_handle',
        'publisher_handle',
        'subscription_handle',
        'service_handle',
        'client_handle',
        'callback_object',
        'callback_group',
        'tf_buffer_core',
        'tf_broadcaster',
    }

    def __init__(
        self,
        info: LttngInfo
    ) -> None:
        nodes = info.get_nodes()

        self._nodes: Dict[ServerKey, NodeValueLttng] = {}
        self._clients: Dict[ServerKey, ClientCallbackValueLttng] = {}
        self._publishers: Dict[ServerKey, PublisherValueLttng] = {}
        self._subscriptions: Dict[ServerKey, SubscriptionCallbackValueLttng] = {}
        self._services: Dict[ServerKey, ServiceCallbackValueLttng] = {}
        self._timers: Dict[ServerKey, TimerCallbackValueLttng] = {}
        self._cbgs: Dict[ServerKey, CallbackGroupValueLttng] = {}
        self._tf_br: Dict[ServerKey, TransformBroadcasterValueLttng] = {}
        self._tf_buff: Dict[ServerKey, TransformBufferValueLttng] = {}

        for node in nodes:
            self._nodes[node.pid, node.node_handle] = node
            try:
                self._cbgs.update({
                    (cbg.pid, cbg.callback_group_addr): cbg
                    for cbg
                    in info.get_callback_groups(node)
                })
                self._clients.update({
                    (clt.pid, clt.client_handle): clt
                    for clt
                    in info.get_client_callbacks(node)
                })
                self._publishers.update({
                    (pub.pid, pub.publisher_handle): pub for pub in info.get_publishers(node)
                })
                self._subscriptions.update({
                    (sub.pid, sub.subscription_handle): sub
                    for sub
                    in info.get_subscription_callbacks(node)
                })
                self._services.update({
                    (srv.pid, srv.service_handle): srv
                    for srv
                    in info.get_service_callbacks(node)
                })
                self._timers.update({
                    (tmr.pid, tmr.timer_handle): tmr
                    for tmr
                    in info.get_timer_callbacks(node)
                })

                bf = info.get_tf_broadcaster(node)
                if bf is not None:
                    self._tf_br[bf.pid, bf.broadcaster_handler] = bf
                buf = info.get_tf_buffer(node)
                if buf is not None:
                    self._tf_buff[buf.pid, buf.buffer_handler] = buf
            except Error as e:
                logger.warning('Failed to get node info: %s', e)

        self._timer_callbacks: Dict[ServerKey, TimerCallbackValueLttng] = {
            (timer.pid, timer.callback_object): timer
            for timer
            in self._timers.values()
        }
        self._subscription_callbacks: Dict[ServerKey, SubscriptionCallbackValueLttng] = {
            (sub.pid, sub.callback_object): sub
            for sub
            in self._subscriptions.values()
        }
        self._service_callbacks: Dict[ServerKey, ServiceCallbackValueLttng] = {
            (srv.pid, srv.callback_object): srv
            for srv
            in self._services.values()
        }

    def get(
        self,
        pid: int,
        key_value: int
    ) -> Category:
        info = Category()

        key = (pid, key_value)

        if key in self._nodes:
            node = self._nodes[key]
            info.node_name = node.node_name
        elif key in self._timers:
            timer = self._timers[key]
            info.node_name = timer.node_name
        elif key in self._publishers:
            publisher = self._publishers[key]
            info.node_name = publisher.node_name
            info.topic_name = publisher.topic_name
        elif key in self._subscriptions:
            subscription = self._subscriptions[key]
            info.node_name = subscription.node_name
            info.topic_name = subscription.subscribe_topic_name
        elif key in self._services:
            service = self._services[key]
            info.node_name = service.node_name
        elif key in self._clients:
            client = self._clients[key]
            info.node_name = client.node_name
        elif key in self._timer_callbacks:
            timer = self._timer_callbacks[key]
            info.node_name = timer.node_name
        elif key in self._service_callbacks:
            service = self._service_callbacks[key]
            info.node_name = service.node_name
        elif key in self._subscription_callbacks:
            subscription = self._subscription_callbacks[key]
            info.node_name = subscription.node_name
            info.topic_name = subscription.subscribe_topic_name
        elif key in self._clients:
            client = self._clients[key]
            info.node_name = client.node_name
        elif key in self._cbgs:
            cbg = self._cbgs[key]
            info.node_name = cbg.node_name
        elif key in self._tf_buff:
            info.topic_name = '/tf'
        elif key in self._tf_br:
            tf_br = self._tf_br[key]
            info.node_name = tf_br.node_name
            info.topic_name = '/tf'

        return info


class CountRecord:
    def __init__(
        self,
        trace_point: str,
        info: Category,
        size: int
    ) -> None:
        self._trace_point = trace_point
        self._info = info
        self._size = size

    def as_dict(self) -> Dict[str, Union[str, int]]:
        return {
            'trace_point': self._trace_point,
            'topic_name': self._info.topic_name,
            'node_name': self._info.node_name,
            'size': self._size,
        }


class CountRule:

    def __init__(
        self,
        trace_point: str,
        data: RecordsInterface,
        data_key: Optional[str],
    ) -> None:
        self.trace_point = trace_point
        self.records = data
        self.record_key = data_key


class EventCounter:

    def __init__(
        self,
        data: Ros2DataModel,
        info: LttngInfo,
    ) -> None:
        self._allowed_keys = {'trace_point', 'node_name', 'topic_name'}

        counter = CategoryServer(info)
        count_rules = self.get_count_rules(data)
        counts = EventCounter.count_tracepoints(count_rules, counter)
        count_dicts = [record.as_dict() for record in counts]
        self._count_df = pd.DataFrame.from_dict(count_dicts)

    def get_count(self, groupby: List[str]) -> pd.DataFrame:
        if len(set(groupby) - self._allowed_keys) > 0:
            raise InvalidArgumentError(
                f'invalid groupby: {groupby}. {self._allowed_keys} are allowed.')

        grouped_df = self._count_df.groupby(groupby).sum([['size']])
        count_df = grouped_df.sort_values('size', ascending=False)
        return count_df

    @staticmethod
    def get_count_rules(data: Ros2DataModel) -> List[CountRule]:
        count_rules = [
            # initialization tracepoints
            CountRule('ros2:construct_ring_buffer',
                      data.construct_ring_buffer,
                      None),
            CountRule('ros2:rcl_client_init',
                      data.rcl_client_init,
                      'client_handle'),
            CountRule('ros2:rcl_init',
                      data.rcl_init,
                      None),
            CountRule('ros2:rcl_lifecycle_state_machine_init',
                      data.rcl_lifecycle_state_machine_init,
                      'node_handle'),
            CountRule('ros2:rcl_lifecycle_transition',
                      data.lifecycle_transitions,
                      None),
            CountRule('ros2:rcl_node_init',
                      data.rcl_node_init,
                      'node_handle'),
            CountRule('ros2:rcl_publisher_init',
                      data.rcl_publisher_init,
                      'publisher_handle'),
            CountRule('ros2:rcl_service_init',
                      data.rcl_service_init,
                      'service_handle'),
            CountRule('ros2:rcl_subscription_init',
                      data.rcl_subscription_init,
                      'subscription_handle'),
            CountRule('ros2:rcl_timer_init',
                      data.rcl_timer_init,
                      'timer_handle'),
            CountRule('ros2:rclcpp_callback_register',
                      data.rclcpp_callback_register,
                      'callback_object'),
            CountRule('ros2:rclcpp_service_callback_added',
                      data.rclcpp_service_callback_added,
                      'callback_object'),
            CountRule('ros2:rclcpp_subscription_callback_added',
                      data.rclcpp_subscription_callback_added,
                      'callback_object'),
            CountRule('ros2:rclcpp_subscription_init',
                      data.rclcpp_subscription_init,
                      'subscription_handle'),
            CountRule('ros2:rclcpp_timer_callback_added',
                      data.rclcpp_timer_callback_added,
                      'timer_handle'),
            CountRule('ros2:rclcpp_timer_link_node',
                      data.rclcpp_timer_link_node,
                      'timer_handle'),
            CountRule('ros2_caret:add_callback_group',
                      data.add_callback_group,
                      None),
            CountRule('ros2_caret:add_callback_group_static_executor',
                      data.add_callback_group_static_executor,
                      None),
            CountRule('ros2_caret:callback_group_add_client',
                      data.callback_group_add_client,
                      'client_handle'),
            CountRule('ros2_caret:callback_group_add_service',
                      data.callback_group_add_service,
                      'service_handle'),
            CountRule('ros2_caret:callback_group_add_subscription',
                      data.callback_group_add_subscription,
                      'subscription_handle'),
            CountRule('ros2_caret:callback_group_add_timer',
                      data.callback_group_add_timer,
                      'timer_handle'),
            CountRule('ros2_caret:construct_executor',
                      data.construct_executor,
                      None),
            CountRule('ros2_caret:construct_ipm',
                      data.construct_ipm,
                      None),
            CountRule('ros2_caret:construct_node_hook',
                      data.construct_node_hook,
                      'node_handle'),
            CountRule('ros2_caret:construct_static_executor',
                      data.construct_static_executor,
                      None),
            CountRule('ros2_caret:construct_tf_buffer',
                      data.construct_tf_buffer,
                      'tf_buffer_core'),
            CountRule('ros2_caret:init_bind_tf_broadcaster_send_transform',
                      data.transform_broadcaster_frames,
                      'transform_broadcaster'),
            CountRule('ros2_caret:init_bind_tf_buffer_core',
                      data.init_bind_tf_buffer_core,
                      'tf_buffer_core'),
            CountRule('ros2_caret:init_bind_transform_broadcaster',
                      data.transform_broadcaster,
                      'publisher_handle'),
            CountRule('ros2_caret:init_tf_broadcaster_frame_id_compact',
                      data.init_tf_buffer_frame_id_compact,
                      'tf_broadcaster'),
            CountRule('ros2_caret:init_tf_buffer_frame_id_compact',
                      data.init_tf_buffer_frame_id_compact,
                      'tf_buffer_core'),
            CountRule('ros2_caret:init_tf_buffer_lookup_transform',
                      data.tf_buffer_lookup_transform,
                      'tf_buffer_core'),
            CountRule('ros2_caret:init_tf_buffer_set_transform',
                      data.tf_buffer_set_transform,
                      'tf_buffer_core'),
            CountRule('ros2_caret:ipm_add_publisher',
                      data.ipm_add_publisher,
                      'publisher_handle'),
            CountRule('ros2_caret:ipm_add_subscription',
                      data.ipm_add_subscription,
                      'subscription_handle'),
            CountRule('ros2_caret:ipm_insert_sub_id_for_pub',
                      data.ipm_insert_sub_id_for_pub,
                      None),
            CountRule('ros2_caret:rmw_implementation',
                      data.rmw_implementation,
                      None),
            CountRule('ros2_caret:symbol_rename',
                      data.symbol_rename,
                      None),
            CountRule('ros2_caret:tilde_publisher_init',
                      data.tilde_publisher_init,
                      'publisher'),
            CountRule('ros2_caret:tilde_subscribe_added',
                      data.tilde_subscribe_added,
                      'subscription'),
            CountRule('ros2_caret:tilde_subscription_init',
                      data.tilde_subscription_init,
                      'subscription'),
            CountRule('ros2_caret:rcl_init_caret',
                      data.rcl_init_caret,
                      'pid'),

            # measurement tracepoints
            CountRule('ros2:callback_end',
                      data.callback_end,
                      'callback_object'),
            CountRule('ros2:callback_start',
                      data.callback_start,
                      'callback_object'),
            CountRule('ros2:dispatch_intra_process_subscription_callback',
                      data.dispatch_intra_process_subscription_callback,
                      'callback_object'),
            CountRule('ros2:dispatch_subscription_callback',
                      data.dispatch_subscription_callback,
                      'callback_object'),
            CountRule('ros2:message_construct',
                      data.message_construct,
                      None),
            CountRule('ros2:rcl_publish',
                      data.rcl_publish,
                      'publisher_handle'),
            CountRule('ros2:rclcpp_intra_publish',
                      data.rclcpp_intra_publish,
                      'publisher_handle'),
            CountRule('ros2:rclcpp_publish',
                      data.rclcpp_publish,
                      'publisher_handle'),
            CountRule('ros2:ring_buffer_clear',
                      data.ring_buffer_clear,
                      None),
            CountRule('ros2:ring_buffer_dequeue',
                      data.ring_buffer_dequeue,
                      None),
            CountRule('ros2:ring_buffer_enqueue',
                      data.ring_buffer_enqueue,
                      None),
            CountRule('ros2_caret:dds_bind_addr_to_addr',
                      data.dds_bind_addr_to_addr,
                      None),
            CountRule('ros2_caret:dds_bind_addr_to_stamp',
                      data.dds_bind_addr_to_stamp,
                      None),
            CountRule('ros2_caret:dds_write',
                      data.dds_write,
                      None),
            CountRule('ros2_caret:inter_callback_duration',
                      data.inter_callback_duration,
                      'callback_object'),
            CountRule('ros2_caret:inter_publish',
                      data.inter_publish,
                      'publisher_handle'),
            CountRule('ros2_caret:intra_callback_duration',
                      data.intra_callback_duration,
                      'callback_object'),
            CountRule('ros2_caret:on_data_available',
                      data.on_data_available,
                      None),
            CountRule('ros2_caret:send_transform',
                      data.send_transform,
                      'broadcaster'),
            CountRule('ros2_caret:sim_time',
                      data.sim_time,
                      None),
            CountRule('ros2_caret:tf_buffer_find_closest',
                      data.tf_buffer_find_closest,
                      'tf_buffer_core'),
            CountRule('ros2_caret:tf_lookup_transform',
                      data.tf_lookup_transform,
                      'tf_buffer_core'),
            CountRule('ros2_caret:tf_lookup_transform_start',
                      data.tf_lookup_transform_start,
                      'tf_buffer_core'),
            CountRule('ros2_caret:tf_lookup_transform_end',
                      data.tf_lookup_transform_end,
                      'tf_buffer_core'),
            CountRule('ros2_caret:tf_lookup_transform',
                      data.tf_lookup_transform,
                      'tf_buffer_core'),
            CountRule('ros2_caret:tilde_publish',
                      data.tilde_publish,
                      'publisher'),
            CountRule('ros2_caret:tilde_subscribe',
                      data.tilde_subscribe,
                      'subscription'),
            CountRule('ros2_caret:tf_set_transform',
                      data.tf_set_transform,
                      'tf_buffer_core'),
            CountRule('ros2:rclcpp_publisher_init',
                      data.rclcpp_publisher_init,
                      'publisher_handle'),
        ]

        # validate count_rules
        handler = Ros2Handler()
        count_rule_tracepoints = {rule.trace_point for rule in count_rules}
        missing = handler.tracepoints - count_rule_tracepoints
        if len(missing) > 0:
            assert False, 'Missing trace points: {}'.format(missing)
        over_set = count_rule_tracepoints - handler.tracepoints
        if len(over_set) > 0:
            assert False, 'Over-set trace points: {}'.format(over_set)

        return count_rules

    @staticmethod
    def count_tracepoints(
        count_rules: Sequence[CountRule],
        server: CategoryServer,
    ) -> List[CountRecord]:
        l: List[CountRecord] = []

        for rule in count_rules:
            records = rule.records
            trace_point = rule.trace_point
            record_key = rule.record_key

            if record_key is None:
                record = CountRecord(trace_point, Category(), len(records))
                l.append(record)
                continue

            for keys, group in records.groupby(['pid', record_key]).items():
                pid = keys[0]
                key_value = keys[1]
                record = CountRecord(trace_point, server.get(pid, key_value), len(group))
                l.append(record)

        return l
