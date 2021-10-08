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

from abc import ABCMeta
from abc import abstractmethod
from typing import List, Tuple

import numpy as np
import pandas as pd

from .record import RecordsInterface


class LatencyBase(metaclass=ABCMeta):

    @abstractmethod
    def to_records(self) -> RecordsInterface:
        pass

    @property
    @abstractmethod
    def column_names(self) -> List[str]:
        pass

    def to_dataframe(self, remove_dropped=False, treat_drop_as_delay=False) -> pd.DataFrame:
        records = self.to_records()
        column_names = self.column_names

        if remove_dropped is False and treat_drop_as_delay:
            records.bind_drop_as_delay(column_names[0])

        df = records.to_dataframe()

        if remove_dropped:
            df.dropna(inplace=True)

        for missing_column in set(column_names) - set(df.columns):
            df[missing_column] = np.nan

        return df[column_names]

    def to_timeseries(
        self, remove_dropped=False, treat_drop_as_delay=False
    ) -> Tuple[np.array, np.array]:
        df = self.to_dataframe(remove_dropped, treat_drop_as_delay)
        msg = (
            'Failed to find any records that went through the path.'
            + 'There is a possibility that all records are lost.',
        )

        assert len(df) > 0, msg
        source_stamps_ns = np.array(df.iloc[:, 0].values)
        dest_stamps_ns = np.array(df.iloc[:, -1].values)
        t = source_stamps_ns

        latency_ns = dest_stamps_ns - source_stamps_ns
        return t, latency_ns

    def to_histogram(
        self, binsize_ns: int = 1000000, treat_drop_as_delay=False
    ) -> Tuple[np.array, np.array]:
        import math

        _, latency_ns = self.to_timeseries(True, treat_drop_as_delay=treat_drop_as_delay)

        range_min = math.floor(min(latency_ns) / binsize_ns) * binsize_ns
        range_max = math.ceil(max(latency_ns) / binsize_ns) * binsize_ns
        bin_num = math.ceil((range_max - range_min) / binsize_ns)

        return np.histogram(latency_ns, bins=bin_num, range=(range_min, range_max))
