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

from .column_names import COLUMN_NAME as CN
from .lttng_info import LttngInfo
from .ros2_tracing import Ros2DataModel, Ros2Handler
from .trace_points import TRACE_POINT as TP
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
from ...exceptions import Error, InvalidArgumentError
from ...record import RecordsInterface

logger = getLogger(__name__)

UNKNOWN = 'unkown'


class Category:

    def __init__(self):
        # PANDAS treats the None value differently, so enter a string by default.
        self.node_name = UNKNOWN
        self.topic_name = UNKNOWN
        self.trace_point = UNKNOWN


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
                    (pub.pid, pub.publisher_handle): pub
                    for pub
                    in info.get_publishers(node)
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
        addr: int
    ) -> Category:
        info = Category()

        key = (pid, addr)

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
        self._allowed_keys = ['trace_point', 'node_name', 'topic_name']

        counter = CategoryServer(info)
        count_rules = self.get_count_rules(data)
        counts = self.count_tracepoints(count_rules, counter)
        count_dicts = [record.as_dict() for record in counts]
        self._count_df = pd.DataFrame.from_dict(count_dicts)

    def get_count(self, groupby: Optional[Sequence[str]] = None) -> pd.DataFrame:
        groupby_ = self._allowed_keys if groupby is None else list(groupby)
        if len(set(groupby_) - set(self._allowed_keys)) > 0:
            raise InvalidArgumentError(
                f'invalid groupby: {groupby_}. {self._allowed_keys} are allowed.')

        if len(self._count_df) == 0:
            return pd.DataFrame()
        grouped_df = self._count_df.groupby(groupby_).sum([['size']])
        count_df = grouped_df.sort_values('size', ascending=False)
        count_df.reset_index(inplace=True)
        count_df = count_df.reindex(columns=groupby_ + ['size'])
        return count_df

    @staticmethod
    def get_count_rules(data: Ros2DataModel) -> List[CountRule]:
        count_rules = [
            # initialization tracepoints
            CountRule(TP.CONSTRUCT_RING_BUFFER,
                      data.construct_ring_buffer,
                      None),
            CountRule(TP.RCL_CLIENT_INIT,
                      data.rcl_client_init,
                      CN.CLIENT_HANDLE),
            CountRule(TP.RCL_INIT,
                      data.rcl_init,
                      UNKNOWN),
            CountRule(TP.RCL_LIFECYCLE_STATE_MACHINE_INIT,
                      data.rcl_lifecycle_state_machine_init,
                      CN.NODE_HANDLE),
            CountRule(TP.RCL_LIFECYCLE_TRANSITION,
                      data.lifecycle_transitions,
                      UNKNOWN),
            CountRule(TP.RCL_NODE_INIT,
                      data.rcl_node_init,
                      CN.NODE_HANDLE),
            CountRule(TP.RCL_PUBLISHER_INIT,
                      data.rcl_publisher_init,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.RCL_SERVICE_INIT,
                      data.rcl_service_init,
                      CN.SERVICE_HANDLE),
            CountRule(TP.RCL_SUBSCRIPTION_INIT,
                      data.rcl_subscription_init,
                      CN.SUBSCRIPTION_HANDLE),
            CountRule(TP.RCL_TIMER_INIT,
                      data.rcl_timer_init,
                      CN.TIMER_HANDLE),
            CountRule(TP.RCLCPP_CALLBACK_REGISTER,
                      data.rclcpp_callback_register,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.RCLCPP_SERVICE_CALLBACK_ADDED,
                      data.rclcpp_service_callback_added,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.RCLCPP_SUBSCRIPTION_INIT,
                      data.rclcpp_subscription_callback_added,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.RCLCPP_SUBSCRIPTION_CALLBACK_ADDED,
                      data.rclcpp_subscription_init,
                      CN.SUBSCRIPTION_HANDLE),
            CountRule(TP.RCLCPP_TIMER_CALLBACK_ADDED,
                      data.rclcpp_timer_callback_added,
                      CN.TIMER_HANDLE),
            CountRule(TP.RCLCPP_TIMER_LINK_NODE,
                      data.rclcpp_timer_link_node,
                      CN.TIMER_HANDLE),
            CountRule(TP.ADD_CALLBACK_GROUP,
                      data.add_callback_group,
                      None),
            CountRule(TP.ADD_CALLBACK_GROUP_STATIC_EXECUTOR,
                      data.add_callback_group_static_executor,
                      None),
            CountRule(TP.CALLBACK_GROUP_ADD_CLIENT,
                      data.callback_group_add_client,
                      CN.CLIENT_HANDLE),
            CountRule(TP.CALLBACK_GROUP_ADD_SERVICE,
                      data.callback_group_add_service,
                      CN.SERVICE_HANDLE),
            CountRule(TP.CALLBACK_GROUP_ADD_SUBSCRIPTION,
                      data.callback_group_add_subscription,
                      CN.SUBSCRIPTION_HANDLE),
            CountRule(TP.CALLBACK_GROUP_ADD_TIMER,
                      data.callback_group_add_timer,
                      CN.TIMER_HANDLE),
            CountRule(TP.CONSTRUCT_EXECUTOR,
                      data.construct_executor,
                      None),
            CountRule(TP.CONSTRUCT_IPM,
                      data.construct_ipm,
                      None),
            CountRule(TP.CONSTRUCT_NODE_HOOK,
                      data.construct_node_hook,
                      CN.NODE_HANDLE),
            CountRule(TP.CONSTRUCT_STATIC_EXECUTOR,
                      data.construct_static_executor,
                      None),
            CountRule(TP.CONSTRUCT_TF_BUFFER,
                      data.construct_tf_buffer,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.INIT_BIND_TF_BROADCASTER_SEND_TRANSFORM,
                      data.transform_broadcaster_frames,
                      CN.TRANSFORM_BROADCASTER),
            CountRule(TP.INIT_BIND_TF_BUFFER_CORE,
                      data.init_bind_tf_buffer_core,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.INIT_BIND_TRANSFORM_BROADCASTER,
                      data.transform_broadcaster,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.INIT_TF_BROADCASTER_FRAME_ID_COMPACT,
                      data.init_tf_buffer_frame_id_compact,
                      CN.TF_BROADCASTER),
            CountRule(TP.INIT_TF_BUFFER_FRAME_ID_COMPACT,
                      data.init_tf_buffer_frame_id_compact,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.INIT_TF_BUFFER_LOOKUP_TRANSFORM,
                      data.tf_buffer_lookup_transform,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.INIT_TF_BUFFER_SET_TRANSFORM,
                      data.tf_buffer_set_transform,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.IPM_ADD_PUBLISHER,
                      data.ipm_add_publisher,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.IPM_ADD_SUBSCRIPTION,
                      data.ipm_add_subscription,
                      CN.SUBSCRIPTION_HANDLE),
            CountRule(TP.IPM_INSERT_SUB_ID_FOR_PUB,
                      data.ipm_insert_sub_id_for_pub,
                      None),
            CountRule(TP.RMW_IMPLEMENTATION,
                      data.rmw_implementation,
                      None),
            CountRule(TP.SYMBOL_RENAME,
                      data.symbol_rename,
                      None),
            CountRule(TP.TILDE_PUBLISHER_INIT,
                      data.tilde_publisher_init,
                      CN.PUBLISHER),
            CountRule(TP.TILDE_SUBSCRIBE_ADDED,
                      data.tilde_subscribe_added,
                      CN.SUBSCRIPTION),
            CountRule(TP.TILDE_SUBSCRIPTION_INIT,
                      data.tilde_subscription_init,
                      CN.SUBSCRIPTION),
            CountRule(TP.RCL_INIT_CARET,
                      data.rcl_init_caret,
                      CN.PID),

            # measurement tracepoints
            CountRule(TP.CALLBACK_END,
                      data.callback_end,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.CALLBACK_START,
                      data.callback_start,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK,
                      data.dispatch_intra_process_subscription_callback,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.DISPATCH_SUBSCRIPTION_CALLBACK,
                      data.dispatch_subscription_callback,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.MESSAGE_CONSTRUCT,
                      data.message_construct,
                      None),
            CountRule(TP.RCL_PUBLISH,
                      data.rcl_publish,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.RCLCPP_INTRA_PUBLISH,
                      data.rclcpp_intra_publish,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.RCLCPP_PUBLISH,
                      data.rclcpp_publish,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.RING_BUFFER_CLEAR,
                      data.ring_buffer_clear,
                      None),
            CountRule(TP.RING_BUFFER_DEQUEUE,
                      data.ring_buffer_dequeue,
                      None),
            CountRule(TP.RING_BUFFER_ENQUEUE,
                      data.ring_buffer_enqueue,
                      None),
            CountRule(TP.DDS_BIND_ADDR_TO_ADDR,
                      data.dds_bind_addr_to_addr,
                      None),
            CountRule(TP.DDS_BIND_ADDR_TO_STAMP,
                      data.dds_bind_addr_to_stamp,
                      None),
            CountRule(TP.DDS_WRITE,
                      data.dds_write,
                      None),
            CountRule(TP.INTER_CALLBACK_DURATION,
                      data.inter_callback_duration,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.INTER_PUBLISH,
                      data.inter_publish,
                      CN.PUBLISHER_HANDLE),
            CountRule(TP.INTRA_CALLBACK_DURATION,
                      data.intra_callback_duration,
                      CN.CALLBACK_OBJECT),
            CountRule(TP.ON_DATA_AVAILABLE,
                      data.on_data_available,
                      None),
            CountRule(TP.SEND_TRANSFORM,
                      data.send_transform,
                      CN.BROADCASTER),
            CountRule(TP.SIM_TIME,
                      data.sim_time,
                      None),
            CountRule(TP.TF_BUFFER_FIND_CLOSEST,
                      data.tf_buffer_find_closest,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.TF_LOOKUP_TRANSFORM,
                      data.tf_lookup_transform,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.TF_LOOKUP_TRANSFORM_START,
                      data.tf_lookup_transform_start,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.TF_LOOKUP_TRANSFORM_END,
                      data.tf_lookup_transform_end,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.TF_LOOKUP_TRANSFORM,
                      data.tf_lookup_transform,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.TILDE_PUBLISH,
                      data.tilde_publish,
                      CN.PUBLISHER),
            CountRule(TP.TILDE_SUBSCRIBE,
                      data.tilde_subscribe,
                      CN.SUBSCRIPTION),
            CountRule(TP.TF_SET_TRANSFORM,
                      data.tf_set_transform,
                      CN.TF_BUFFER_CORE),
            CountRule(TP.RCLCPP_PUBLISHER_INIT,
                      data.rclcpp_publisher_init,
                      CN.PUBLISHER_HANDLE),
        ]

        handler_tracepoints = set(Ros2Handler.get_tracepoints())
        # validate count_rules
        count_rule_tracepoints = {rule.trace_point for rule in count_rules}
        missing = handler_tracepoints - count_rule_tracepoints
        if len(missing) > 0:
            assert False, 'Missing trace points: {}'.format(missing)
        over_set = count_rule_tracepoints - handler_tracepoints
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
                if len(records) == 0:
                    continue
                record = CountRecord(trace_point, Category(), len(records))
                l.append(record)
                continue

            for keys, group in records.groupby([CN.PID, record_key]).items():
                pid = keys[0]
                key_value = keys[1]
                record = CountRecord(trace_point, server.get(pid, key_value), len(group))
                l.append(record)

        return l
