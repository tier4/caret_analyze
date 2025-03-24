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

from collections.abc import Sequence

import pandas as pd

from ..metrics_base import MetricsBase
from ..util import get_clock_converter
from ...common import ClockConverter
from ...record import RecordsInterface, ResponseTime
from ...runtime import Path


class ResponseTimeTimeSeries(MetricsBase):
    """Class that provides latency timeseries data."""

    def __init__(
        self,
        target_objects: Sequence[Path],
        case: str
    ) -> None:
        super().__init__(target_objects)
        self._case = case

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get Response time timeseries data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        pd.DataFrame
            Multi-column Response time DataFrame.

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        timeseries_records_list = self.to_timeseries_records_list(xaxis_type)
        all_df = pd.DataFrame()
        for to, response_records in zip(self._target_objects, timeseries_records_list):
            response_df = response_records.to_dataframe()
            response_df[response_df.columns[-1]] *= 10**(-6)
            response_df.rename(
                columns={
                    response_df.columns[0]: 'path_start_timestamp [ns]',
                    response_df.columns[1]: 'response_time [ms]',
                },
                inplace=True
            )
            # TODO: Multi-column DataFrame are difficult for users to handle,
            #       so it should be a single-column DataFrame.
            response_df = self._add_top_level_column(response_df, to)
            all_df = pd.concat([all_df, response_df], axis=1)

        return all_df.sort_index(level=0, axis=1, sort_remaining=False)

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> list[RecordsInterface]:
        """
        Get Response time records list of all target objects.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        list[RecordsInterface]
            Response time records list of all target objects.

        Raises
        ------
        ValueError
            - Case is not "all", "best", "worst", or "worst-with-external-latency".

        """
        timeseries_records_list: list[RecordsInterface] = [
            _.to_records() for _ in self._target_objects
        ]

        converter: ClockConverter | None = None
        if xaxis_type == 'sim_time':
            converter = get_clock_converter(self._target_objects)

        response_timeseries_list: list[RecordsInterface] = []
        for records in timeseries_records_list:
            response = ResponseTime(records)
            if self._case == 'all':
                response_timeseries_list.append(response.to_all_records(converter=converter))
            elif self._case == 'best':
                response_timeseries_list.append(response.to_best_case_records(converter=converter))
            elif self._case == 'worst':
                response_timeseries_list.append(response.to_worst_case_records(
                    converter=converter))
            elif self._case == 'worst-with-external-latency':
                response_timeseries_list.append(
                    response.to_worst_with_external_latency_case_records(converter=converter))
            else:
                raise ValueError('optional argument "case" must be following: \
                                 "all", "best", "worst", "worst-with-external-latency".')

        return response_timeseries_list
