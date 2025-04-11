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

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence

import pandas as pd

from ..record import RecordsInterface
from ..runtime import CallbackBase, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path


class MetricsBase(metaclass=ABCMeta):
    """Metics base class."""

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes],
    ) -> None:
        self._target_objects = list(target_objects)

    @property
    def target_objects(self) -> list[TimeSeriesTypes]:
        """
        Get target objects.

        Returns
        -------
        list[TimeSeriesTypes]
            target object list.

        """
        return self._target_objects

    @abstractmethod
    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get data in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str, optional
            X axis value's type , by default 'system_time'.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    @abstractmethod
    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> list[RecordsInterface]:
        """
        Get timeseries records list.

        Parameters
        ----------
        xaxis_type : str, optional
            X axis value's type , by default 'system_time'.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    # TODO: Multi-column DataFrame are difficult for users to handle,
    #       so this function is unnecessary.
    @staticmethod
    def _get_ts_column_name(
        target_object: TimeSeriesTypes
    ) -> str:
        if isinstance(target_object, Publisher):
            callback_names = (f'{target_object.callback_names[0]}/'
                              if target_object.callback_names else '')
            ts_column_name = f'{callback_names}rclcpp_publish_timestamp'
        elif isinstance(target_object, Subscription):
            ts_column_name = f'{target_object.column_names[0]}'
        else:
            ts_column_name = target_object.column_names[0].split('/')[-1]

        return ts_column_name + ' [ns]'

    # TODO: Multi-column DataFrame are difficult for users to handle,
    #       so this function is unnecessary.
    @staticmethod
    def _add_top_level_column(
        target_df: pd.DataFrame,
        target_object: TimeSeriesTypes
    ) -> pd.DataFrame:
        if isinstance(target_object, CallbackBase):
            column_name = target_object.callback_name
        elif isinstance(target_object, Communication):
            column_name = (f'{target_object.publish_node_name}|'
                           f'{target_object.topic_name}|'
                           f'{target_object.subscribe_node_name}')
        elif isinstance(target_object, (Publisher, Subscription)):
            column_name = target_object.topic_name
        elif isinstance(target_object, Path):
            column_name = f'{target_object.column_names[0]} to {target_object.column_names[-1]}'

        return pd.concat([target_df], keys=[column_name], axis=1)
