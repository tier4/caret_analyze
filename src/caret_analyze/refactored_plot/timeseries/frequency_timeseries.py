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

from typing import List, Sequence, Tuple, Union

import pandas as pd

from ..metrics_base import MetricsBase
from ...record import Frequency, RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class FrequencyTimeSeries(MetricsBase):

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes]
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        timeseries_records_list = self.to_timeseries_records_list()
        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        all_df = pd.DataFrame()
        for frequency_records in timeseries_records_list:
            frequency_df = frequency_records.to_dataframe()
            frequency_df.rename(
                columns={
                    frequency_df.columns[0]: f'{frequency_df.columns[0]} [ns]',
                    frequency_df.columns[1]: 'frequency [Hz]',
                },
                inplace=True
            )
            all_df = pd.concat([all_df, frequency_df], axis=1)

        return all_df

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> List[RecordsInterface]:
        min_time, max_time = self._get_timestamp_range()
        timeseries_records_list: List[RecordsInterface] = []
        for target_object in self._target_objects:
            frequency = Frequency(target_object.to_records())
            timeseries_records_list.append(frequency.to_records(
                base_timestamp=min_time, until_timestamp=max_time
            ))

        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        return timeseries_records_list

    def _get_timestamp_range(
        self
    ) -> Tuple[int, int]:
        first_timestamps = []
        last_timestamps = []
        for to in self._target_objects:
            df = to.to_dataframe()
            if len(df) == 0:
                continue
            first_timestamps.append(df.iloc[0, 0])
            last_timestamps.append(df.iloc[-1, 0])

        # TODO: fix error when len(first_timestamp) == 0
        return min(first_timestamps), max(last_timestamps)
