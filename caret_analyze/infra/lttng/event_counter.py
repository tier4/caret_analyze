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
from typing import Dict, List

import pandas as pd

from .ros2_tracing.data_model import Ros2DataModel
from ...exceptions import InvalidArgumentError, InvalidTraceFormatError

logger = getLogger(__name__)


class EventCounter:

    def __init__(self, data: Ros2DataModel, *, validation=True):
        self._allowed_keys = {'trace_point', 'node_name', 'topic_name'}
        # self._count_df = self._build_count_df(data)

        # if validation:
            # self._validate()

    def get_count(self, groupby: List[str]) -> pd.DataFrame:
        if len(set(groupby) - self._allowed_keys) > 0:
            raise InvalidArgumentError(
                f'invalid groupby: {groupby}. {self._allowed_keys} are allowed.')

        grouped_df = self._count_df.groupby(groupby).sum([['size']])
        count_df = grouped_df.sort_values('size', ascending=False)
        return count_df

    def _validate(self):
        count_df = self.get_count(['trace_point'])
        count_df_recorded = count_df[count_df['size'] > 0]
        recorded_trace_points = list(count_df_recorded.index)

        trace_points_added_byld_preload = {
            'ros2_caret:add_callback_group',
            'ros2_caret:add_callback_group_static_executor',
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

        trace_points_added_by_fork_rclcpp = {
            'ros2:message_construct',
            'ros2:rclcpp_intra_publish',
            'ros2:dispatch_subscription_callback',
            'ros2:dispatch_intra_process_subscription_callback',
        }

        if len(set(recorded_trace_points) & trace_points_added_byld_preload) == 0:
            raise InvalidTraceFormatError(
                'Failed to found trace point added by LD_PRELOAD. '
                'Measurement results will not be correct. '
                'The measurement may have been performed without setting LD_PRELOAD.')

        if len(set(recorded_trace_points) & trace_points_added_by_fork_rclcpp) == 0:
            raise InvalidTraceFormatError(
                'Failed to found trace point added by forked rclcpp. '
                'Measurement results will not be correct. '
                'The binary may have been compiled without using fork-rclcpp.')

    @staticmethod
    def _build_count_df(data: Ros2DataModel) -> pd.DataFrame:
        trace_point_and_df = {
            'ros2:rcl_init': data.rcl_init.df,
            'ros2:rcl_node_init': data.rcl_node_init.df,
            'ros2:rcl_publisher_init': data.rcl_publisher_init.df,
            'ros2:rcl_subscription_init': data.rcl_subscription_init.df,
            'ros2:rclcpp_subscription_init': data.rclcpp_subscription_init.df,
            'ros2:rclcpp_subscription_callback_added':
                data.rclcpp_subscription_callback_added.df,
            'ros2:rcl_service_init': data.rcl_service_init.df,
            'ros2:rclcpp_service_callback_added': data.rclcpp_service_callback_added.df,
            'ros2:rcl_client_init': data.rcl_client_init.df,
            'ros2:rcl_timer_init': data.rcl_timer_init.df,
            'ros2:rclcpp_timer_callback_added': data.rclcpp_timer_callback_added.df,
            'ros2:rclcpp_timer_link_node': data.rclcpp_timer_link_node.df,
            'ros2:rclcpp_callback_register': data.rclcpp_callback_register.df,
            # 'ros2:rcl_lifecycle_state_machine_init': data.lifecycle_state_machines,
            'ros2_caret:add_callback_group': data.add_callback_group.df,
            'ros2_caret:add_callback_group_static_executor':
                data.add_callback_group_static_executor.df,
            'ros2_caret:construct_executor': data.construct_executor.df,
            'ros2_caret:construct_static_executor': data.construct_static_executor.df,
            'ros2_caret:callback_group_add_timer': data.callback_group_add_timer.df,
            'ros2_caret:callback_group_add_subscription':
                data.callback_group_add_subscription.df,
            'ros2_caret:callback_group_add_service': data.callback_group_add_service.df,
            'ros2_caret:callback_group_add_client': data.callback_group_add_client.df,
            'ros2_caret:tilde_subscription_init': data.tilde_subscription_init.df,
            'ros2_caret:tilde_publisher_init': data.tilde_publisher_init.df,
            'ros2_caret:tilde_subscribe_added': data.tilde_subscribe_added.df,
            # 'ros2:rcl_lifecycle_transition': data.lifecycle_transitions,
            'ros2_caret:rmw_implementation': data.rmw_implementation.df,

            'ros2:callback_start': data.callback_start.to_dataframe(),
            'ros2:callback_end': data.callback_end.to_dataframe(),
            'ros2:rclcpp_publish': data.rclcpp_publish.to_dataframe(),
            'ros2:rclcpp_intra_publish': data.rclcpp_intra_publish.to_dataframe(),
            'ros2:message_construct': data.message_construct.to_dataframe(),
            'ros2:dispatch_subscription_callback':
                data.dispatch_subscription_callback.to_dataframe(),
            'ros2:dispatch_intra_process_subscription_callback':
                data.dispatch_intra_process_subscription_callback.to_dataframe(),
            'ros2:rcl_publish': data.rcl_publish.to_dataframe(),
            'ros2_caret:dds_write': data.dds_write.to_dataframe(),
            'ros2_caret:dds_bind_addr_to_stamp': data.dds_bind_addr_to_stamp.to_dataframe(),
            'ros2_caret:dds_bind_addr_to_addr': data.dds_bind_addr_to_addr.to_dataframe(),
            'ros2_caret:tilde_publish': data.tilde_publish.to_dataframe(),
            'ros2_caret:tilde_subscribe': data.tilde_subscribe.to_dataframe(),
            'ros2_caret:sim_time': data.sim_time.to_dataframe(),
            'ros2_caret:on_data_available': data.on_data_available.to_dataframe(),
        }
        #  'ros2_caret:rmw_implementation': ,

        sub_handle_to_topic_name: Dict[int, str] = {}
        sub_handle_to_node_name: Dict[int, str] = {}
        pub_handle_to_topic_name: Dict[int, str] = {}
        pub_handle_to_node_name: Dict[int, str] = {}
        node_handle_to_node_name: Dict[int, str] = {}
        timer_handle_to_node_name: Dict[int, str] = {}
        sub_cb_to_node_name: Dict[int, str] = {}
        timer_cb_to_node_name: Dict[int, str] = {}
        sub_cb_to_topic_name: Dict[int, str] = {}
        sub_to_topic_name: Dict[int, str] = {}
        sub_to_node_name: Dict[int, str] = {}

        def ns_and_node_name(ns: str, name: str) -> str:
            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        for handler, row in data.rcl_node_init.df.iterrows():
            node_handle_to_node_name[handler] = ns_and_node_name(row['namespace'], row['name'])

        for handler, row in data.rcl_publisher_init.df.iterrows():
            pub_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')
            pub_handle_to_topic_name[handler] = row['topic_name']

        for handler, row in data.rcl_subscription_init.df.iterrows():
            sub_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')
            sub_handle_to_topic_name[handler] = row['topic_name']

        for handler, row in data.rclcpp_timer_link_node.df.iterrows():
            timer_handle_to_node_name[handler] = \
                node_handle_to_node_name.get(row['node_handle'], '-')

        for sub, row in data.rclcpp_subscription_callback_added.df.iterrows():
            sub_handle = row['subscription_handle']
            sub_to_topic_name[sub] = sub_handle_to_topic_name.get(sub_handle, '-')
            sub_to_node_name[sub] = sub_handle_to_node_name.get(sub_handle, '-')

        for handler, row in data.rclcpp_callback_register.df.iterrows():
            if handler in sub_to_topic_name:
                sub_cb_to_node_name[row['callback_object']] = sub_to_node_name.get(handler, '-')
                sub_cb_to_topic_name[row['callback_object']] = sub_to_topic_name.get(handler, '-')
            elif handler in timer_handle_to_node_name:
                timer_cb_to_node_name[row['callback_object']] = \
                    timer_handle_to_node_name.get(handler, '-')

        tilde_pub_to_topic_name: Dict[int, str] = {}
        tilde_pub_to_node_name: Dict[int, str] = {}
        for handler, row in data.tilde_publisher_init.df.iterrows():
            tilde_pub_to_node_name[handler] = row['node_name']
            tilde_pub_to_topic_name[handler] = row['topic_name']

        tilde_sub_to_topic_name: Dict[int, str] = {}
        tilde_sub_to_node_name: Dict[int, str] = {}
        for handler, row in data.tilde_subscription_init.df.iterrows():
            tilde_sub_to_node_name[handler] = row['node_name']
            tilde_sub_to_topic_name[handler] = row['topic_name']

        count_dict = []
        group_keys = [
            'callback_object', 'publisher_handle', 'subscription_handle',
            'tilde_publisher', 'tilde_subscription'
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

                count_dict.append(
                    {
                        'node_name': node_name,
                        'topic_name': topic_name,
                        'size': len(group),
                        'trace_point': trace_point
                    }
                )

        return pd.DataFrame.from_dict(count_dict)
