# Copyright 2021 TIER IV, Inc.
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

import pandas as pd

from .ros2_tracing.data_model import Ros2DataModel
from ...exceptions import InvalidArgumentError, InvalidTraceFormatError

logger = getLogger(__name__)


class EventCounter:

    def __init__(self, data: Ros2DataModel, *, validate=True):
        self._allowed_keys = {'trace_point', 'node_name', 'topic_name'}
        self._count_df = self._build_count_df(data)
        self._has_intra_process = self._check_intra_process_communication(data)
        self._has_original_rclcpp_publish = self._check_original_rclcpp_publish(data)
        self._distribution = self._get_distribution(data)
        if validate:
            self._validate()

    def get_count(self, groupby: list[str]) -> pd.DataFrame:
        if len(set(groupby) - self._allowed_keys) > 0:
            raise InvalidArgumentError(
                f'invalid groupby: {groupby}. {self._allowed_keys} are allowed.')

        grouped_df = self._count_df.groupby(groupby).sum([['size']])
        count_df = grouped_df.sort_values('size', ascending=False)
        return count_df

    def _validate(self):
        # The record containing ignored_topics can records trace_points_added_by_ld_preload
        # and trace_points_added_by_fork_rclcpp.
        # Exclude these records as they interfere with validation.
        record_df = self.get_count(['trace_point', 'topic_name']).reset_index()
        ignored_topics = ['caret/start_record', '/caret/end_record']
        ignored_df = record_df[~record_df['topic_name'].isin(ignored_topics)]

        count_df = ignored_df.groupby('trace_point').sum([['size']])
        count_df_recorded = count_df[count_df['size'] > 0]
        recorded_trace_points = list(count_df_recorded.index)

        trace_points_added_by_ld_preload = {
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
            'ros2_caret:tilde_subscribe_added',
            'ros2_caret:dds_write',
            'ros2_caret:dds_bind_addr_to_stamp',
            'ros2_caret:dds_bind_addr_to_addr',
            'ros2_caret:tilde_publish',
            'ros2_caret:sim_time',
            'ros2_caret:on_data_available_data',
            'ros2_caret:rmw_implementation'
        }

        dds_trace_points_added_by_ld_preload = {
            'ros2_caret:dds_bind_addr_to_stamp',
            'ros2_caret:dds_bind_addr_to_addr',
        }

        trace_points_added_by_fork_rclcpp_for_intra_process = {
            'ros2:message_construct',
            'ros2:rclcpp_intra_publish',
            'ros2:dispatch_intra_process_subscription_callback',
        }

        trace_points_added_by_fork_rclcpp_for_inter_process = {
            'ros2:dispatch_subscription_callback',
        }

        if len(set(recorded_trace_points) & dds_trace_points_added_by_ld_preload) == 0:
            raise InvalidTraceFormatError(
                'Failed to find dds trace point added by LD_PRELOAD. '
                'Measurement results will not be correct. '
                'Function symbols used for hooks may be hidden in the DDS layer.'
            )

        if len(set(recorded_trace_points) & trace_points_added_by_ld_preload) == 0:
            raise InvalidTraceFormatError(
                'Failed to find trace point added by LD_PRELOAD. '
                'Measurement results will not be correct. '
                'The measurement may have been performed without setting LD_PRELOAD.'
            )

        # For after iron distributions
        # No need to check trace points added by fork-rclcpp
        if self._distribution[0] >= 'i':
            return

        has_forked_inter_process_trace_points = len(
            set(recorded_trace_points) & trace_points_added_by_fork_rclcpp_for_inter_process) != 0
        has_forked_intra_process_trace_points = len(
            set(recorded_trace_points) & trace_points_added_by_fork_rclcpp_for_intra_process) != 0

        # In caret, rmw_take can be used as a substitute for dispatch_subscription_callback
        has_rmw_take_trace_points = len(
            set(recorded_trace_points) & {'ros2:rmw_take'}) != 0

        if self._has_original_rclcpp_publish:
            logger.warning(
                'This trace data has trace point from a package built without caret-rclcpp. '
                'Such package cannot be analyzed successfully.')

        if (not has_forked_inter_process_trace_points
                and not has_forked_intra_process_trace_points):
            # In this case, the measured application uses only inter process communication,
            # so the trace data can be analyzed using rmw_take trace points.
            if has_rmw_take_trace_points and not self._has_intra_process:
                return
            msg = 'Failed to find trace points added by caret-rclcpp. '
            if self._has_intra_process:
                msg += "If the application doesn't have any intra communication, "
                msg += 'please ignore this message. '
            msg += 'To check whether binary are built with caret-rclcpp, '
            msg += 'run CARET CLI : ros2 caret check_caret_rclcpp.'

            # trace points added to rclcpp may not be recorded, depending on the implementation.
            # Here, only warnings are given.
            logger.warning(msg)

    def _check_intra_process_communication(self, data: Ros2DataModel) -> bool:
        return sum(data.callback_start_instances.get_column_series('is_intra_process')) != 0

    def _check_original_rclcpp_publish(self, data: Ros2DataModel) -> bool:
        return 0 in data.rclcpp_publish_instances.get_column_series('publisher_handle')

    def _get_distribution(self, data: Ros2DataModel) -> str:
        caret_init_df = data.caret_init.df
        distributions = list(caret_init_df['distribution'].unique())
        if len(distributions) > 1:
            logger.info('Multiple ros distributions are found.')

        if len(distributions) == 0:
            return 'NOTFOUND'

        return distributions[0]

    @staticmethod
    def _build_count_df(data: Ros2DataModel) -> pd.DataFrame:
        # TODO(hsgwa): Definitions on tracepoint types are scattered. Refactor required.
        trace_point_and_df = {
            'ros2:rcl_init': data.contexts.df,
            'ros2:rcl_node_init': data.nodes.df,
            'ros2:rcl_publisher_init': data.publishers.df,
            'ros2:rcl_subscription_init': data.subscriptions.df,
            'ros2:rclcpp_subscription_init': data.subscription_objects.df,
            'ros2:rclcpp_subscription_callback_added': data.callback_objects.df,
            'ros2:rcl_service_init': data.services.df,
            'ros2:rclcpp_service_callback_added': data.callback_objects.df,
            'ros2:rcl_client_init': data.clients.df,
            'ros2:rcl_timer_init': data.timers.df,
            'ros2:rclcpp_timer_callback_added': data.callback_objects.df,
            'ros2:rclcpp_timer_link_node': data.timer_node_links.df,
            'ros2:rclcpp_callback_register': data.callback_symbols.df,
            'ros2:rcl_lifecycle_state_machine_init': data.lifecycle_state_machines.df,
            'ros2_caret:add_callback_group': data.callback_groups.df,
            'ros2_caret:add_callback_group_static_executor': data.callback_groups_static.df,
            'ros2_caret:callback_group_to_executor_entity_collector':
                data.callback_group_to_executor_entity_collector.df,
            'ros2_caret:executor_entity_collector_to_executor':
                data.executor_entity_collector_to_executor.df,
            'ros2_caret:construct_executor': data.executors.df,
            'ros2_caret:construct_static_executor': data.executors_static.df,
            'ros2_caret:callback_group_add_timer': data.callback_group_timer.df,
            'ros2_caret:callback_group_add_subscription': data.callback_group_subscription.df,
            'ros2_caret:callback_group_add_service': data.callback_group_service.df,
            'ros2_caret:callback_group_add_client': data.callback_group_client.df,
            'ros2_caret:tilde_subscription_init': data.tilde_subscriptions.df,
            'ros2_caret:tilde_publisher_init': data.tilde_publishers.df,
            'ros2_caret:tilde_subscribe_added': data.tilde_subscribe_added.df,
            'ros2:rcl_lifecycle_transition': data.lifecycle_transitions.df,
            'ros2:rclcpp_buffer_to_ipb': data.buffer_to_ipbs.df,
            'ros2:rclcpp_ipb_to_subscription': data.ipb_to_subscriptions.df,
            'ros2:rclcpp_construct_ring_buffer': data.ring_buffers.df,
            'ros2_caret:rmw_implementation': data.rmw_impl.df,

            'ros2:callback_start': data.callback_start_instances.to_dataframe(),
            'ros2:callback_end': data.callback_end_instances.to_dataframe(),
            'ros2:rclcpp_publish': data.rclcpp_publish_instances.to_dataframe(),
            'ros2:rclcpp_intra_publish': data.rclcpp_intra_publish_instances.to_dataframe(),
            'ros2:rclcpp_ring_buffer_enqueue':
                data.rclcpp_ring_buffer_enqueue_instances.to_dataframe(),
            'ros2:rclcpp_ring_buffer_dequeue':
                data.rclcpp_ring_buffer_dequeue_instances.to_dataframe(),
            'ros2:message_construct': data.message_construct_instances.to_dataframe(),
            'ros2:dispatch_subscription_callback':
                data.dispatch_subscription_callback_instances.to_dataframe(),
            'ros2:rmw_take':
                data.rmw_take_instances.to_dataframe(),
            'ros2:dispatch_intra_process_subscription_callback':
                data.dispatch_intra_process_subscription_callback_instances.to_dataframe(),
            'ros2:rcl_publish': data.rcl_publish_instances.to_dataframe(),
            'ros2_caret:dds_write': data.dds_write_instances.to_dataframe(),
            'ros2_caret:dds_bind_addr_to_stamp': data.dds_bind_addr_to_stamp.to_dataframe(),
            'ros2_caret:dds_bind_addr_to_addr': data.dds_bind_addr_to_addr.to_dataframe(),
            'ros2_caret:tilde_publish': data.tilde_publish.to_dataframe(),
            'ros2_caret:tilde_subscribe': data.tilde_subscribe.to_dataframe(),
            'ros2_caret:sim_time': data.sim_time.to_dataframe(),
            'ros2_caret:on_data_available': data.on_data_available_instances.to_dataframe(),
            'ros2_caret:caret_init': data.caret_init.df,
        }
        #  'ros2_caret:rmw_implementation': ,

        sub_handle_to_topic_name: dict[int, str] = {}
        sub_handle_to_node_name: dict[int, str] = {}
        pub_handle_to_topic_name: dict[int, str] = {}
        pub_handle_to_node_name: dict[int, str] = {}
        node_handle_to_node_name: dict[int, str] = {}
        timer_handle_to_node_name: dict[int, str] = {}
        sub_cb_to_node_name: dict[int, str] = {}
        timer_cb_to_node_name: dict[int, str] = {}
        sub_cb_to_topic_name: dict[int, str] = {}
        sub_to_topic_name: dict[int, str] = {}
        sub_to_node_name: dict[int, str] = {}
        rmw_handle_to_node_name: dict[int, str] = {}
        rmw_handle_to_topic_name: dict[int, str] = {}

        def ns_and_node_name(ns: str, name: str) -> str:
            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        for handler, row in data.nodes.df.iterrows():
            node_handle_to_node_name[handler] = ns_and_node_name(row['namespace'], row['name'])

        for handler, row in data.publishers.df.iterrows():
            pub_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')
            pub_handle_to_topic_name[handler] = row['topic_name']

        for handler, row in data.subscriptions.df.iterrows():
            sub_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')
            sub_handle_to_topic_name[handler] = row['topic_name']
            rmw_handle_to_node_name[row['rmw_handle']] = \
                node_handle_to_node_name.get(row['node_handle'], '-')
            rmw_handle_to_topic_name[row['rmw_handle']] = row['topic_name']

        for handler, row in data.timer_node_links.df.iterrows():
            timer_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')

        for sub, row in data.subscription_objects.df.iterrows():
            sub_handle = row['subscription_handle']
            sub_to_topic_name[sub] = sub_handle_to_topic_name.get(sub_handle, '-')
            sub_to_node_name[sub] = sub_handle_to_node_name.get(sub_handle, '-')

        for handler, row in data.callback_objects.df.iterrows():
            if handler in sub_to_topic_name:
                sub_cb_to_node_name[row['callback_object']] = sub_to_node_name.get(handler, '-')
                sub_cb_to_topic_name[row['callback_object']] = sub_to_topic_name.get(handler, '-')
            elif handler in timer_handle_to_node_name:
                timer_cb_to_node_name[row['callback_object']] = \
                    timer_handle_to_node_name.get(handler, '-')

        tilde_pub_to_topic_name: dict[int, str] = {}
        tilde_pub_to_node_name: dict[int, str] = {}
        for handler, row in data.tilde_publishers.df.iterrows():
            tilde_pub_to_node_name[handler] = row['node_name']
            tilde_pub_to_topic_name[handler] = row['topic_name']

        tilde_sub_to_topic_name: dict[int, str] = {}
        tilde_sub_to_node_name: dict[int, str] = {}
        for handler, row in data.tilde_subscriptions.df.iterrows():
            tilde_sub_to_node_name[handler] = row['node_name']
            tilde_sub_to_topic_name[handler] = row['topic_name']

        count_dict = []
        group_keys = [
            'callback_object', 'publisher_handle', 'subscription_handle',
            'tilde_publisher', 'tilde_subscription', 'rmw_subscription_handle'
        ]
        for trace_point, df in trace_point_and_df.items():
            df = df.reset_index()

            if len(df) == 0:
                count_dict.append(
                    {
                        'node_name': '-',
                        'topic_name': '-',
                        'size': 0,
                        'trace_point': trace_point
                    }
                )
                continue
            if 'callback_object' not in df.columns:
                df['callback_object'] = '-'
            if 'publisher_handle' not in df.columns:
                df['publisher_handle'] = '-'
            if 'subscription_handle' not in df.columns:
                df['subscription_handle'] = '-'
            if 'rmw_subscription_handle' not in df.columns:
                df['rmw_subscription_handle'] = '-'

            if trace_point in ['ros2_caret:tilde_publish', 'ros2_caret:tilde_publisher_init']:
                df['tilde_publisher'] = df['publisher']
            else:
                df['tilde_publisher'] = '-'

            if trace_point in ['ros2_caret:tilde_subscribe', 'ros2_caret:tilde_subscription_init']:
                df['tilde_subscription'] = df['subscription']
            else:
                df['tilde_subscription'] = '-'

            for key, group in df.groupby(group_keys):
                node_name = '-'
                topic_name = '-'

                if key[0] in timer_cb_to_node_name:
                    node_name = timer_cb_to_node_name.get(key[0], '-')
                elif key[0] in sub_cb_to_node_name or key[0] \
                        in sub_cb_to_topic_name:
                    node_name = sub_cb_to_node_name.get(key[0], '-')
                    topic_name = sub_cb_to_topic_name.get(key[0], '-')
                elif key[1] in pub_handle_to_topic_name or \
                        key[1] in pub_handle_to_node_name:
                    topic_name = pub_handle_to_topic_name.get(key[1], '-')
                    node_name = pub_handle_to_node_name.get(key[1], '-')
                elif key[2] in sub_handle_to_node_name or \
                        key[2] in sub_handle_to_topic_name:
                    topic_name = sub_handle_to_topic_name.get(key[2], '-')
                    node_name = sub_handle_to_node_name.get(key[2], '-')
                elif key[3] in tilde_pub_to_node_name or \
                        key[3] in tilde_pub_to_topic_name:
                    topic_name = tilde_pub_to_topic_name.get(key[3], '-')
                    node_name = tilde_pub_to_node_name.get(key[3], '-')
                elif key[4] in tilde_sub_to_node_name or \
                        key[4] in tilde_sub_to_topic_name:
                    topic_name = tilde_sub_to_topic_name.get(key[4], '-')
                    node_name = tilde_sub_to_node_name.get(key[4], '-')
                elif key[5] in rmw_handle_to_node_name or \
                        key[5] in rmw_handle_to_topic_name:
                    topic_name = rmw_handle_to_topic_name.get(key[5], '-')
                    node_name = rmw_handle_to_node_name.get(key[5], '-')

                count_dict.append(
                    {
                        'node_name': node_name,
                        'topic_name': topic_name,
                        'size': len(group),
                        'trace_point': trace_point
                    }
                )

        return pd.DataFrame.from_dict(count_dict)
