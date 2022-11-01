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

from typing import List, Optional, Set, Tuple, Union

import pandas as pd

from .data_model import Ros2DataModel


class DataModelService:

    def __init__(self, data: Ros2DataModel) -> None:
        self._data = data

    def get_node_names_and_cb_symbols(
        self,
        cbg_addr: int
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        """
        Get node names and callback symbols from callback group address.

        Parameters
        ----------
        cbg_addr : int
            callback group address.

        Returns
        -------
        List[Tuple[Optional[str], Optional[str]]]
            node names and callback symbols.
            tuple structure: (node_name, callback_symbol)

        Notes
        -----
        Since there may be duplicate addresses in the trace data,
        this returns a list of all possible node names and callback symbols.

        """
        node_names_and_cb_symbols: Set[Tuple[Optional[str], Optional[str]]] = set()
        try:
            node_names_and_cb_symbols |= \
                set(self._get_node_names_and_cb_symbols_from_timer(cbg_addr))
        except KeyError:
            pass

        try:
            node_names_and_cb_symbols |= \
                set(self._get_node_names_and_cb_symbols_from_sub(cbg_addr))
        except KeyError:
            pass

        try:
            node_names_and_cb_symbols |= \
                set(self._get_node_names_and_cb_symbols_from_srv(cbg_addr))
        except KeyError:
            pass

        return sorted(
            node_names_and_cb_symbols,
            key=lambda x: (x[0] is None, x[1] is None, str(x[0]), str(x[1]))
        )

    def _get_node_names_and_cb_symbols_from_timer(
        self,
        cbg_addr: int
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        match_cbg_timer = self._ensure_dataframe(
            self._data.callback_group_timer.loc[cbg_addr, :])
        timer_handles = match_cbg_timer.loc[:, 'timer_handle'].to_list()

        node_names_and_cb_symbols: List[Tuple[Optional[str], Optional[str]]] = []
        for handle in timer_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.timer_node_links)
            callback_symbol = self._get_callback_symbol_from_handle(handle)
            if node_name or callback_symbol:
                node_names_and_cb_symbols.append((node_name, callback_symbol))

        return node_names_and_cb_symbols

    def _get_node_names_and_cb_symbols_from_sub(
        self,
        cbg_addr: int
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        match_cbg_sub = self._ensure_dataframe(
            self._data.callback_group_subscription.loc[cbg_addr, :])
        sub_handles = match_cbg_sub.loc[:, 'subscription_handle'].to_list()

        node_names_and_cb_symbols: List[Tuple[Optional[str], Optional[str]]] = []
        for handle in sub_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.subscriptions)
            callback_symbol = self._get_callback_symbol_from_handle(handle)
            if node_name or callback_symbol:
                node_names_and_cb_symbols.append((node_name, callback_symbol))

        return node_names_and_cb_symbols

    def _get_node_names_and_cb_symbols_from_srv(
        self,
        cbg_addr: int
    ) -> List[Tuple[Optional[str], Optional[str]]]:
        match_cbg_srv = self._ensure_dataframe(
            self._data.callback_group_service.loc[cbg_addr, :])
        srv_handles = match_cbg_srv.loc[:, 'service_handle'].to_list()

        node_names_and_cb_symbols: List[Tuple[Optional[str], Optional[str]]] = []
        for handle in srv_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.services)
            callback_symbol = self._get_callback_symbol_from_handle(handle)
            if node_name or callback_symbol:
                node_names_and_cb_symbols.append((node_name, callback_symbol))

        return node_names_and_cb_symbols

    def _get_node_name_from_handle(
        self,
        handle: int,
        middle_df: pd.DataFrame
    ) -> Optional[str]:
        def ns_and_node_name(row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        try:
            match_middle = self._ensure_dataframe(middle_df.loc[handle, :])
            node_handles = match_middle.loc[:, 'node_handle'].to_list()
            match_nodes = self._data.nodes.loc[node_handles, :]
            assert len(match_nodes) == 1
            return ns_and_node_name(match_nodes.iloc[0])
        except KeyError:
            return None

    def _get_callback_symbol_from_handle(
        self,
        handle: int
    ) -> Optional[str]:
        try:
            match_callback_objects = self._ensure_dataframe(
                self._data.callback_objects.loc[handle, :])
            callback_objects = match_callback_objects.loc[:, 'callback_object'].to_list()
            match_callback_symbols = self._data.callback_symbols.loc[callback_objects, :]
            assert len(match_callback_symbols) == 1
            return match_callback_symbols.iloc[0]['symbol']
        except KeyError:
            return None

    @staticmethod
    def _ensure_dataframe(
        dataframe_or_series: Union[pd.DataFrame, pd.Series]
    ) -> pd.DataFrame:
        """
        If input is Series, convert to DataFrame.

        Note
        ----
        In the pandas specification,
        when a conditional extraction is performed on DataFrame,
        if there is only one matching row, it is converted to Series.
        To address the above case, this converts Series to a one-row DataFrame.

        """
        if isinstance(dataframe_or_series, pd.Series):
            df = pd.DataFrame([dataframe_or_series])
        else:
            df = dataframe_or_series

        return df
