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

from functools import lru_cache
import itertools

from logging import getLogger, Logger

import pandas as pd

from .data_model import Ros2DataModel
from ....exceptions import InvalidArgumentError


logger = getLogger(__name__)


class DataModelService:

    def __init__(self, data: Ros2DataModel) -> None:
        self._data = data

    def get_node_names_and_cb_symbols(
        self,
        cbg_addr: int
    ) -> list[tuple[str | None, str | None]]:
        """
        Get node names and callback symbols from callback group address.

        Parameters
        ----------
        cbg_addr : int
            callback group address.

        Returns
        -------
        list[tuple[str | None, str | None]]
            node names and callback symbols.
            tuple structure: (node_name, callback_symbol)

        Notes
        -----
        Since there may be duplicate addresses in the trace data,
        this returns a list of all possible node names and callback symbols.

        """
        node_names_and_cb_symbols: set[tuple[str | None, str | None]] = set()
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
    ) -> list[tuple[str | None, str | None]]:
        match_cbg_timer = self._ensure_dataframe(
            self._data.callback_group_timer.df.loc[cbg_addr, :])
        timer_handles = match_cbg_timer.loc[:, 'timer_handle'].to_list()

        node_names_and_cb_symbols: list[tuple[str | None, str | None]] = []
        for handle in timer_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.timer_node_links.df)
            callback_symbol = self._get_callback_symbols_from_handle(handle)
            if node_name or callback_symbol != [None]:
                node_names_and_cb_symbols.extend(
                    list(itertools.product([node_name], callback_symbol)))

        return node_names_and_cb_symbols

    def _get_node_names_and_cb_symbols_from_sub(
        self,
        cbg_addr: int
    ) -> list[tuple[str | None, str | None]]:
        match_cbg_sub = self._ensure_dataframe(
            self._data.callback_group_subscription.df.loc[cbg_addr, :])
        sub_handles = match_cbg_sub.loc[:, 'subscription_handle'].to_list()

        node_names_and_cb_symbols: list[tuple[str | None, str | None]] = []
        for handle in sub_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.subscriptions.df)
            callback_symbol = self._get_callback_symbols_from_handle(handle)
            if node_name or callback_symbol != [None]:
                node_names_and_cb_symbols.extend(
                    list(itertools.product([node_name], callback_symbol)))

        return node_names_and_cb_symbols

    def _get_node_names_and_cb_symbols_from_srv(
        self,
        cbg_addr: int
    ) -> list[tuple[str | None, str | None]]:
        match_cbg_srv = self._ensure_dataframe(
            self._data.callback_group_service.df.loc[cbg_addr, :])
        srv_handles = match_cbg_srv.loc[:, 'service_handle'].to_list()

        node_names_and_cb_symbols: list[tuple[str | None, str | None]] = []
        for handle in srv_handles:
            node_name = self._get_node_name_from_handle(handle, self._data.services.df)
            callback_symbol = self._get_callback_symbols_from_handle(handle)
            if node_name or callback_symbol != [None]:
                node_names_and_cb_symbols.extend(
                    list(itertools.product([node_name], callback_symbol)))

        return node_names_and_cb_symbols

    @staticmethod
    @lru_cache(maxsize=10)
    def _log_once(logger_: Logger, message: str):
        logger_.warning(message)

    def _get_node_name_from_handle(
        self,
        handle: int,
        middle_df: pd.DataFrame
    ) -> str | None:
        def ns_and_node_name(row: pd.Series) -> str:
            ns: str = row['namespace']
            name: str = row['name']

            if ns[-1] == '/':
                return ns + name
            else:
                return ns + '/' + name

        try:
            match_middle = self._ensure_dataframe(middle_df.loc[handle, :])
            node_handles = list(set(match_middle.loc[:, 'node_handle'].to_list()))
            match_nodes = self._data.nodes.df.loc[node_handles, :]
            match_nodes.drop_duplicates(
                set(match_nodes) - {'tid'}, inplace=True, ignore_index=True)
            node_names = [ns_and_node_name(row) for _, row in match_nodes.iterrows()]
            if len(node_names) > 1:
                self._log_once(
                    logger,
                    'Failed to identify node name from node_handler.'
                    f'Several candidates were found: {node_names}',
                )
            return node_names[0]
        except KeyError:
            return None

    def _get_callback_symbols_from_handle(
        self,
        handle: int
    ) -> list[str | None]:
        try:
            match_callback_objects = self._ensure_dataframe(
                self._data.callback_objects.df.loc[handle, :])
            callback_objects = match_callback_objects.loc[:, 'callback_object'].to_list()
            match_callback_symbols = self._data.callback_symbols.df.loc[callback_objects, :]
            return [t.symbol for t in match_callback_symbols.itertuples()]
        except KeyError:
            return [None]

    def get_rmw_subscription_handle_from_callback_object(
        self,
        cb_addr: int
    ) -> int | None:
        """
        Get rmw subscription handle from callback object.

        Parameters
        ----------
        cb_addr : int
            callback address.

        Returns
        -------
        int | None
            rmw subscription handle.

        """
        try:
            sub = self._get_sub_from_callback_object(cb_addr)
            rmw_handle = None
            if sub is not None:
                sub_handle = self._get_sub_handle_from_sub(sub)
                if sub_handle is not None:
                    rmw_handle = self._get_rmw_handle_from_sub_handle(sub_handle)
            return rmw_handle
        except KeyError:
            return None

    def _get_sub_from_callback_object(
        self,
        cb_addr: int
    ) -> int | None:
        try:
            target_df = self._ensure_dataframe(
                self._data.callback_objects.clone().df.reset_index()[
                    ['reference', 'callback_object']
                ])
            sub = target_df[target_df['callback_object'] == cb_addr]['reference'].values
            if len(sub) == 1:
                return sub[0]
            elif len(sub) == 0:
                msg = f'There is no subscription  that corresponds to callback_addr: {cb_addr}.'
                raise InvalidArgumentError(msg)
            else:
                msg = f'Duplicated subscription : [{sub}] \
                    that corresponds to callback_addr: {cb_addr}'
                raise InvalidArgumentError(msg)
        except KeyError:
            return None

    def _get_sub_handle_from_sub(
        self,
        subscription: int
    ) -> int | None:
        try:
            target_df = self._ensure_dataframe(
                self._data.subscription_objects.clone().df.reset_index()[
                        ['subscription', 'subscription_handle']
                    ]
                )
            sub_handle = target_df[target_df['subscription'] == subscription][
                    'subscription_handle'
                ].values
            if len(sub_handle) == 1:
                return sub_handle[0]
            elif len(sub_handle) == 0:
                msg = f'There is no subscription_handle that \
                    corresponds to subscription : {subscription}.'
                raise InvalidArgumentError(msg)
            else:
                msg = f'Duplicated subscription_handle: [{sub_handle}] that \
                    corresponds to subscription : {subscription}.'
                raise InvalidArgumentError(msg)
        except KeyError:
            return None

    def _get_rmw_handle_from_sub_handle(
        self,
        subscription_handle: int
    ) -> int | None:
        try:
            target_df = self._ensure_dataframe(
                self._data.subscriptions.clone().df.reset_index()[
                    ['subscription_handle', 'rmw_handle']
                ])
            rmw_handle = target_df[
                target_df['subscription_handle'] == subscription_handle
                ]['rmw_handle'].values
            if len(rmw_handle) == 1:
                return rmw_handle[0]
            elif len(rmw_handle) == 0:
                msg = f'There is no rmw_handle that \
                    corresponds to subscription_handle: {subscription_handle}.'
                raise InvalidArgumentError(msg)
            else:
                msg = f'Duplicated rmw_handle: [{rmw_handle}] that \
                    corresponds to subscription_handle: {subscription_handle}.'
                raise InvalidArgumentError(msg)
        except KeyError:
            return None

    @staticmethod
    def _ensure_dataframe(
        dataframe_or_series: pd.DataFrame | pd.Series
    ) -> pd.DataFrame:
        """
        If input is Series, convert to DataFrame.

        Note:
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
