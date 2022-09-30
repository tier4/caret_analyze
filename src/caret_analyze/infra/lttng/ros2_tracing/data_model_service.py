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

from typing import List, Set

import pandas as pd

from .data_model import Ros2DataModel


class DataModelService:

    def __init__(self, data: Ros2DataModel) -> None:
        self._data = data

    def get_node_names(self, cbg_addr: int) -> List[str]:
        """
        Get node names from callback group address.

        Parameters
        ----------
        cbg_addr : int
            callback group address.

        Returns
        -------
        List[str]

        Notes
        -----
        Since there may be duplicate addresses in the trace data,
        this API returns a list of all possible node names.

        """
        node_names: Set[str] = set()
        try:
            node_names |= set(self._get_node_names_from_cbg_timer(cbg_addr))
        except KeyError:
            pass

        try:
            node_names |= set(self._get_node_names_from_cbg_sub(cbg_addr))
        except KeyError:
            pass

        try:
            node_names |= set(
                self._get_node_names_from_get_parameters_srv(cbg_addr))
        except KeyError:
            pass

        return sorted(node_names)

    def _get_node_names_from_cbg_timer(
        self,
        cbg_addr: int
    ) -> List[str]:
        # `.loc[[cbg_addr], :]` is intended to make the return value DataFrame
        # instead of Series.
        match_cbg_timer = self._data.callback_group_timer.loc[[cbg_addr], :]

        match_timer_handles = match_cbg_timer.loc[:, 'timer_handle'].to_list()
        match_timer_node_links = self._data.timer_node_links.loc[
            match_timer_handles, :
        ]

        match_node_handles = \
            match_timer_node_links.loc[:, 'node_handle'].to_list()
        return self._get_node_names_from_node_handles(match_node_handles)

    def _get_node_names_from_cbg_sub(
        self,
        cbg_addr: int
    ) -> List[str]:
        # `.loc[[cbg_addr], :]` is intended to make the return value DataFrame
        # instead of Series.
        match_cbg_sub = \
            self._data.callback_group_subscription.loc[[cbg_addr], :]

        match_sub_handles = \
            match_cbg_sub.loc[:, 'subscription_handle'].to_list()
        match_subscriptions = self._data.subscriptions.loc[
            match_sub_handles, :
        ]

        match_node_handles = \
            match_subscriptions.loc[:, 'node_handle'].to_list()
        return self._get_node_names_from_node_handles(match_node_handles)

    def _get_node_names_from_get_parameters_srv(
        self,
        cbg_addr: int
    ) -> List[str]:
        # `.loc[[cbg_addr], :]` is intended to make the return value DataFrame
        # instead of Series.
        match_cbg_srv = self._data.callback_group_service.loc[[cbg_addr], :]

        match_srv_handles = match_cbg_srv.loc[:, 'service_handle'].to_list()
        match_services = self._data.services.loc[
            match_srv_handles, :
        ]

        match_node_handles = match_services.loc[:, 'node_handle'].to_list()
        return self._get_node_names_from_node_handles(match_node_handles)

    def _get_node_names_from_node_handles(
        self,
        node_handles: List[int]
    ) -> List[str]:
        def ns_and_node_name(row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        match_nodes = self._data.nodes.loc[node_handles, :]

        node_names = [ns_and_node_name(row)
                      for _, row in match_nodes.iterrows()]
        return node_names
