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

from typing import List, Optional, Sequence, Tuple, Union

import pandas as pd

from ..metrics_base import MetricsBase
from ...exceptions import InvalidArgumentError
from ...record import Frequency, RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class FrequencyTimeSeries(MetricsBase):
    """Class that provides frequency timeseries data."""

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes]
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get frequency timeseries data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        pd.DataFrame
            Multi-column frequency DataFrame.

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        timeseries_records_list = self.to_timeseries_records_list()
        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        all_df = pd.DataFrame()
        for to, frequency_records in zip(self.target_objects, timeseries_records_list):
            frequency_df = frequency_records.to_dataframe()
            frequency_df.rename(
                columns={
                    frequency_df.columns[0]: f'{self._get_ts_column_name(to)}',
                    frequency_df.columns[1]: 'frequency [Hz]',
                },
                inplace=True
            )
            # TODO: Multi-column DataFrame are difficult for users to handle,
            #       so it should be a single-column DataFrame.
            frequency_df = self._add_top_level_column(frequency_df, to)
            all_df = pd.concat([all_df, frequency_df], axis=1)

        return all_df.sort_index(level=0, axis=1, sort_remaining=False)

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> List[RecordsInterface]:
        """
        Get frequency records list of all target objects.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        List[RecordsInterface]
            Frequency records list of all target objects.

        """
        min_time, max_time = self._get_timestamp_range(self._target_objects)
        timeseries_records_list: List[RecordsInterface] = []
        for target_object in self._target_objects:
            frequency = Frequency(target_object.to_records())
            timeseries_records_list.append(frequency.to_records(
                base_timestamp=min_time, until_timestamp=max_time
            ))

        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        return timeseries_records_list

    # TODO: Migrate into record.
    @staticmethod
    def _get_timestamp_range(
        target_objects: Sequence[TimeSeriesTypes]
    ) -> Tuple[int, int]:
        def is_valid(timestamp: Optional[int]) -> bool:
            import numpy as np
            if timestamp is None:
                return False
            elif np.isnan(timestamp):
                return False
            else:
                return True

        first_timestamps = []
        last_timestamps = []
        for to in target_objects:
            df = to.to_dataframe()
            if len(df) == 0:
                continue
            if is_valid(first_timestamp := df.iloc[0, 0]):
                first_timestamps.append(first_timestamp)
            if is_valid(last_timestamp := df.iloc[-1, 0]):
                last_timestamps.append(last_timestamp)

        if len(first_timestamps) == 0 or len(last_timestamps) == 0:
            raise InvalidArgumentError('None of the objects have timestamps.')
        else:
            return min(first_timestamps), max(last_timestamps)
