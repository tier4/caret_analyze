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

from typing import Optional

from .data_model import Ros2DataModel
from ....exceptions import InvalidCtfDataError


class DataModelService:

    def __init__(self, data: Ros2DataModel) -> None:
        self._data = data

    def get_node_name(self, cbg_addr: int) -> str:
        node_name = self._get_node_name_from_rcl_node_init(cbg_addr)
        if node_name:
            return node_name

        node_name = self._get_node_name_from_get_parameters_srv(cbg_addr)
        if node_name:
            return node_name

        raise InvalidCtfDataError(
            'Failed to identify node name from callback group address.')

    def _get_node_name_from_rcl_node_init(
        self,
        cbg_addr: int
    ) -> Optional[str]:
        node_name = self._get_node_name_from_cbg_timer(cbg_addr)
        if node_name:
            return node_name

        node_name = self._get_node_name_from_cbg_sub(cbg_addr)
        if node_name:
            return node_name

        return None

    def _get_node_name_from_cbg_timer(self, cbg_addr: int) -> Optional[str]:
        print('a')
        match_cbg_timer = self._data.callback_group_timer.query(
            f'callback_group_addr == {cbg_addr}'
        )
        if match_cbg_timer.empty:
            return None

        assert len(match_cbg_timer) == 1
        timer_handle = match_cbg_timer.iloc[0]['timer_handle']
        match_timer_node_links = self._data.timer_node_links.query(
            f'timer_handle == {timer_handle}'
        )
        if match_timer_node_links.empty:
            return None

        assert len(match_timer_node_links) == 1
        node_handle = match_timer_node_links.iloc[0]['node_handle']
        match_nodes = self._data.nodes.query(
            f'node_handle == {node_handle}'
        )
        if match_nodes.empty:
            return None

        assert len(match_nodes) == 1
        node_name = (match_nodes.iloc[0]['namespace']
                     + '/' + match_nodes.iloc[0]['name'])
        return node_name

    def _get_node_name_from_cbg_sub(self, cbg_addr: int) -> Optional[str]:
        match_cbg_sub = self._data.callback_group_subscription.query(
            f'callback_group_addr == {cbg_addr}'
        )
        if match_cbg_sub.empty:
            return None

        assert len(match_cbg_sub) == 1
        subscription_handle = match_cbg_sub.iloc[0]['subscription_handle']
        match_subscriptions = self._data.subscriptions.query(
            f'subscription_handle == {subscription_handle}'
        )
        if match_subscriptions.empty:
            return None

        assert len(match_subscriptions) == 1
        node_handle = match_subscriptions.iloc[0]['node_handle']
        match_nodes = self._data.nodes.query(
            f'node_handle == {node_handle}'
        )
        if match_nodes.empty:
            return None

        assert len(match_nodes) == 1
        node_name = (match_nodes.iloc[0]['namespace']
                     + '/' + match_nodes.iloc[0]['name'])
        return node_name

    def _get_node_name_from_get_parameters_srv(
        self,
        cbg_addr: int
    ) -> Optional[str]:
        match_cbg_srv = self._data.callback_group_service.query(
            f'callback_group_addr == {cbg_addr}'
        )
        if match_cbg_srv.empty:
            return None

        assert len(match_cbg_srv) == 1
        service_handle = match_cbg_srv.iloc[0]['service_handle']
        match_services = self._data.services.query(
            f'service_handle == {service_handle}'
        )
        if match_services.empty:
            return None

        assert len(match_services) == 1
        node_handle = match_services.iloc[0]['node_handle']
        match_nodes = self._data.nodes.query(
            f'node_handle == {node_handle}'
        )
        if match_nodes.empty:
            return None

        assert len(match_nodes) == 1
        node_name = (match_nodes.iloc[0]['namespace']
                     + '/' + match_nodes.iloc[0]['name'])
        return node_name
