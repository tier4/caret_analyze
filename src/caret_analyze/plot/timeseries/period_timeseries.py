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

from typing import List, Sequence, Union

import pandas as pd

from ..metrics_base import MetricsBase
from ...record import Period, RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class PeriodTimeSeries(MetricsBase):
    """Class that provides period timeseries data."""

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes]
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get period timeseries data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        Returns
        -------
        pd.DataFrame
            Multi-column period DataFrame.

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        timeseries_records_list = self.to_timeseries_records_list(xaxis_type)
        all_df = pd.DataFrame()
        for to, period_records in zip(self.target_objects, timeseries_records_list):
            period_df = period_records.to_dataframe()
            period_df[period_df.columns[-1]] *= 10**(-6)
            period_df.rename(
                columns={
                    period_df.columns[0]: f'{self._get_ts_column_name(to)}',
                    period_df.columns[1]: 'period [ms]',
                },
                inplace=True
            )
            # TODO: Multi-column DataFrame are difficult for users to handle,
            #       so it should be a single-column DataFrame.
            period_df = self._add_top_level_column(period_df, to)
            all_df = pd.concat([all_df, period_df], axis=1)

        return all_df.sort_index(level=0, axis=1, sort_remaining=False)

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> List[RecordsInterface]:
        """
        Get period records list of all target objects.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        List[RecordsInterface]
            Period records list of all target objects.

        """
        if isinstance(self._target_objects[0], Communication):
            columns = self._target_objects[0].to_records().columns
            start_column = columns[0]
            end_column = columns[1]

            def row_filter_communication(record) -> bool:
                """Return True only if communication is established."""
                comm_start_column = start_column
                comm_end_column = end_column
                if (record.data.get(comm_start_column) is not None
                        and record.data.get(comm_end_column) is not None):
                    return True
                else:
                    return False

        timeseries_records_list: List[RecordsInterface] = []
        for target_object in self._target_objects:
            period = Period(
                target_object.to_records(),
                row_filter=row_filter_communication
                if isinstance(target_object, Communication) else None
            )
            timeseries_records_list.append(period.to_records())

        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        return timeseries_records_list
