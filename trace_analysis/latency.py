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

from abc import ABCMeta, abstractmethod

from typing import Tuple, Optional, List

import numpy as np
import pandas as pd

from trace_analysis.record import Records


class LatencyBase(metaclass=ABCMeta):
    @abstractmethod
    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        pass

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        records = self.to_records(remove_dropped, True)
        df = records.to_dataframe()

        if column_names is not None:
            return df[column_names]

        fully_recorded = df.dropna()
        err_msg = (
            "Failed to find a record with all columns measured."
            "All messages may have been lost in the process."
        )

        assert len(fully_recorded) > 0, err_msg

        sort_target_index = fully_recorded.index[0]
        df.sort_values(sort_target_index, axis=1, ascending=True, inplace=True)

        return df

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        df = self.to_dataframe(remove_dropped, column_names=column_names)
        assert len(df) > 0
        source_stamps_ns = np.array(df.iloc[:, 0].values)
        dest_stamps_ns = np.array(df.iloc[:, -1].values)
        t = source_stamps_ns

        latency_ns = dest_stamps_ns - source_stamps_ns
        return t, latency_ns

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        import math

        _, latency_ns = self.to_timeseries(remove_dropped=True, column_names=column_names)

        range_min = math.floor(min(latency_ns) / binsize_ns) * binsize_ns
        range_max = math.ceil(max(latency_ns) / binsize_ns) * binsize_ns
        bin_num = math.ceil((range_max - range_min) / binsize_ns)

        return np.histogram(latency_ns, bins=bin_num, range=(range_min, range_max))
