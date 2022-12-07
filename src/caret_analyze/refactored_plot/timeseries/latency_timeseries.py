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
from ...record import Latency, RecordsInterface
from ...runtime import CallbackBase, Communication


class LatencyTimeSeries(MetricsBase):

    def __init__(
        self,
        target_objects: Sequence[Union[CallbackBase, Communication]],
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        timeseries_records_list = self.to_timeseries_records_list()
        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        all_df = pd.DataFrame()
        for latency_records in timeseries_records_list:
            latency_df = latency_records.to_dataframe()
            latency_df[latency_df.columns[-1]] *= 10**(-6)
            latency_df.rename(
                columns={
                    latency_df.columns[0]: f'{latency_df.columns[0]} [ns]',
                    latency_df.columns[1]: 'latency [ms]',
                },
                inplace=True
            )
            all_df = pd.concat([all_df, latency_df], axis=1)

        return all_df

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> List[RecordsInterface]:
        timeseries_records_list: List[RecordsInterface] = []
        for target_object in self._target_objects:
            latency = Latency(target_object.to_records())
            timeseries_records_list.append(latency.to_records())

        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        return timeseries_records_list
