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

from collections.abc import Sequence

import pandas as pd

from ..metrics_base import MetricsBase
from ...record import Latency, RecordsInterface
from ...runtime import CallbackBase, Communication


class LatencyTimeSeries(MetricsBase):
    """Class that provides latency timeseries data."""

    def __init__(
        self,
        target_objects: Sequence[CallbackBase | Communication],
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get latency timeseries data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        pd.DataFrame
            Multi-column latency DataFrame.

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        timeseries_records_list = self.to_timeseries_records_list(xaxis_type)
        all_df = pd.DataFrame()
        for to, latency_records in zip(self._target_objects, timeseries_records_list):
            latency_df = latency_records.to_dataframe()
            latency_df[latency_df.columns[-1]] *= 10**(-6)
            latency_df.rename(
                columns={
                    latency_df.columns[0]: f'{self._get_ts_column_name(to)}',
                    latency_df.columns[1]: 'latency [ms]',
                },
                inplace=True
            )
            # TODO: Multi-column DataFrame are difficult for users to handle,
            #       so it should be a single-column DataFrame.
            latency_df = self._add_top_level_column(latency_df, to)
            all_df = pd.concat([all_df, latency_df], axis=1)

        return all_df.sort_index(level=0, axis=1, sort_remaining=False)

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> list[RecordsInterface]:
        """
        Get latency records list of all target objects.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        list[RecordsInterface]
            Latency records list of all target objects.

        """
        timeseries_records_list: list[RecordsInterface] = [
            _.to_records() for _ in self._target_objects
        ]

        if xaxis_type == 'sim_time':
            timeseries_records_list = \
                self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        latency_timeseries_list: list[RecordsInterface] = []
        for records in timeseries_records_list:
            latency = Latency(records)
            latency_timeseries_list.append(latency.to_records())

        return latency_timeseries_list
