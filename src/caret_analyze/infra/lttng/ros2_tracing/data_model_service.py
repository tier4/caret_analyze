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

from typing import List, Optional

from .data_model import Ros2DataModel
from ....exceptions import InvalidCtfDataError


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

        Raises
        ------
        InvalidCtfDataError
            Occurs when there is no node name associated with
            the given callback group address.

        Notes
        -----
        Since there may be duplicate addresses in the trace data,
        this API returns a list of all possible node names.

        """
        try:
            node_names = self._get_node_names_from_cbg_timer(cbg_addr)
        except KeyError:
            pass
        else:
            return node_names

        try:
            node_names = self._get_node_names_from_cbg_sub(cbg_addr)
        except KeyError:
            pass
        else:
            return node_names

        try:
            node_names = self._get_node_names_from_get_parameters_srv(cbg_addr)
        except KeyError:
            raise InvalidCtfDataError(
                'Failed to identify node name from callback group address.')
        else:
            return node_names

    def _get_node_names_from_cbg_timer(
        self,
        cbg_addr: int
    ) -> Optional[List[str]]:
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
    ) -> Optional[List[str]]:
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
    ) -> Optional[List[str]]:
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
        match_nodes = self._data.nodes.loc[node_handles, :]

        node_names = [row.namespace + '/' + row.name
                      for row in match_nodes.itertuples()]
        return node_names
