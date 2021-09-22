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

from typing import List

import pandas as pd

from .util import Ros2DataModelUtil


class DataFrameFormatter:

    def __init__(self, data_util: Ros2DataModelUtil):
        self._data_util = data_util
        data_util.data.nodes = self.merge_namespace_and_name_coumn(
            data_util.data.nodes)

    def get_rmw_implementation(self) -> str:
        return self._data_util.data.rmw_implementation

    def merge_namespace_and_name_coumn(self, nodes: pd.DataFrame) -> pd.DataFrame:
        from copy import deepcopy

        def to_merged_name(ns: str, name: str):
            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        nodes_ = deepcopy(nodes)
        for i, row in nodes_.iterrows():
            merged_name = to_merged_name(row['namespace'], row['name'])
            nodes_.at[i, 'name'] = merged_name

        return nodes_.drop(['namespace'], axis=1)

    def get_callback_df(self):
        intra_callback_df = pd.DataFrame()
        inter_callback_df = pd.DataFrame()

        sub_df = self.get_subscription_info()
        for name, group in sub_df.groupby(['name', 'topic_name']):
            if len(group) == 2:
                intra_callback_df = intra_callback_df.append(group.iloc[0, :])
                inter_callback_df = inter_callback_df.append(group.iloc[1, :])
            else:
                inter_callback_df = inter_callback_df.append(group.iloc[0, :])

        intra_callback_df.reset_index(drop=True, inplace=True)
        inter_callback_df.reset_index(drop=True, inplace=True)

        return intra_callback_df, inter_callback_df

    def get_publisher_handles(self, topic_name, node_name=None):
        df = self.get_publisher_info()
        if node_name is None:
            df_target = df[df['topic_name'] == topic_name]
        else:
            df_target = df[(df['name'] == node_name) &
                           (df['topic_name'] == topic_name)]

        return df_target['publisher_handle'].values

    def get_timer_info(self) -> pd.DataFrame:
        columns = [
            'period_ns',
            'node_handle',
            'timer_handle',
            'callback_object',
            'symbol',
            'rmw_handle',
            'name',
            'namespace',
        ]

        if len(self._data_util.data.timers) == 0:
            return pd.DataFrame(columns=columns)

        has_timer_node_link = self._data_util.data.timers.index.isin(
            self._data_util.data.timer_node_links.index
        )

        timer_df = pd.merge(
            self._data_util.data.timers[has_timer_node_link]
            .rename(columns={'period': 'period_ns'})
            .drop(['timestamp', 'tid'], axis=1),
            self._data_util.data.timer_node_links.drop('timestamp', axis=1),
            left_index=True,
            right_index=True,
        )

        timer_df = pd.merge(
            timer_df,
            self._data_util.data.callback_objects.drop(['timestamp'], axis=1)
            .reset_index()
            .rename(columns={'reference': 'timer_handle'}),
            left_index=True,
            right_on='timer_handle',
        )

        timer_df = pd.merge(
            timer_df,
            self._get_callback_symbols(),
            left_on='callback_object',
            right_on='callback_object',
        )

        timer_df = pd.merge(
            timer_df,
            self._data_util.data.nodes.drop(['timestamp', 'tid'], axis=1),
            left_on='node_handle',
            right_on='node_handle',
        )

        return timer_df

    def get_node_names(self) -> List[str]:
        return self._data_util.data.nodes['name'].values

    def get_subscription_info(self) -> pd.DataFrame:
        columns = [
            'node_handle',
            'rmw_handle',
            'topic_name',
            'depth',
            'subscription_handle',
            'subscription_pointer',
            'callback_object',
            'name',
            'namespace',
            'symbol',
        ]

        if len(self._data_util.data.subscriptions) == 0:
            return pd.DataFrame(columns=columns)

        sub_df = self._data_util.data.subscriptions.drop(['timestamp'], axis=1)
        sub_df = pd.merge(
            sub_df,
            self._data_util.data.subscription_objects.drop(
                ['timestamp'], axis=1),
            left_index=True,
            right_on='subscription_handle',
        )

        sub_df = pd.merge(
            sub_df,
            self._data_util.data.callback_objects.drop(['timestamp'], axis=1),
            left_index=True,
            right_on='reference',
        )

        sub_df = pd.merge(
            sub_df,
            self._data_util.data.nodes.drop(
                ['timestamp', 'tid', 'rmw_handle'], axis=1),
            left_on='node_handle',
            right_index=True,
        )

        sub_df = pd.merge(
            sub_df,
            self._get_callback_symbols(),
            left_on='callback_object',
            right_on='callback_object',
        )

        return sub_df

    def get_publisher_info(self) -> pd.DataFrame:
        pub_df = pd.merge(
            self._data_util.data.nodes.drop(['timestamp', 'tid'], axis=1),
            self._data_util.data.publishers.drop(
                ['timestamp', 'rmw_handle'], axis=1
            ).reset_index(),
            left_on='node_handle',
            right_on='node_handle',
        )

        return pub_df

    def _get_callback_symbols(self) -> pd.DataFrame:
        d = self._data_util.get_callback_symbols()
        return pd.DataFrame.from_dict(
            {'callback_object': list(d.keys()), 'symbol': list(d.values())}
        )
