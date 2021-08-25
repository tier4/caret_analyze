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

import pytest

from typing import Tuple, Set, List
import numpy as np

from trace_analysis.record import Record, Records
from trace_analysis.latency import LatencyBase


class LatencyBaseImpl(LatencyBase):
    def to_records(self):
        pass


class TestLatencyBase:
    @pytest.mark.parametrize(
        "records, df_len, columns",
        [
            (
                Records(
                    [
                        Record({"col0": 1, "col1": 5, "col2": 3}),
                    ]
                ),
                1,
                set(["col0", "col1", "col2"]),
            ),
            (
                Records(
                    [
                        Record({"col0": 1, "col2": 3}),
                        Record({"col1": 6, "col2": 4}),
                        Record({"col0": 2, "col1": 6, "col2": 4}),
                    ]
                ),
                3,
                set(["col0", "col1", "col2"]),
            ),
        ],
    )
    def test_to_dataframe(self, mocker, records: Records, df_len: int, columns: Set[str]):
        def custom_to_records(_, remove_dropped=False) -> Records:
            return records

        latency = LatencyBaseImpl()
        mocker.patch.object(latency, "to_records", custom_to_records)
        df = latency.to_dataframe()

        assert len(df) == df_len
        assert set(df.columns) == columns

        # Make sure the columns are in chronological order from left to right.
        for _, row in df.iterrows():
            for i in range(len(row) - 1):
                if np.isnan(row[i]) or np.isnan(row[i + 1]):
                    continue
                assert row[i] < row[i + 1]

    @pytest.mark.parametrize(
        "records",
        [
            (
                Records(
                    [
                        Record({"col0": 1}),
                        Record({"col1": 5}),
                    ]
                )
            )
        ],
    )
    def test_to_dataframe_failed_to_sort_columns(self, mocker, records: Records):
        def custom_to_records(remove_dropped=False, remove_runtime_info=False) -> Records:
            return records

        with pytest.raises(AssertionError):
            latency = LatencyBaseImpl()
            mocker.patch.object(latency, "to_records", custom_to_records)
            latency.to_dataframe()

    @pytest.mark.parametrize(
        "latencies, binsize_ns, bins_range",
        [
            ([5132, 12385], 1, [5132, 12385]),
            ([5132, 12385], 10, [5130, 12390]),
            ([5132, 12385], 100, [5100, 12400]),
            ([5132, 12385], 1000, [5000, 13000]),
        ],
    )
    def test_to_histogram(
        self, mocker, latencies: List[int], binsize_ns: int, bins_range: Tuple[int, int]
    ):
        def custom_to_timeseries(remove_dropped=False) -> Tuple[np.array, np.array]:
            return np.array([]), np.array(latencies)

        latency = LatencyBaseImpl()
        mocker.patch.object(latency, "to_timeseries", custom_to_timeseries)

        hist, bins = latency.to_histogram(binsize_ns=binsize_ns)
        assert min(bins) == bins_range[0] and max(bins) == bins_range[1]
        expect_length = (max(bins) - min(bins)) / binsize_ns + 1
        assert len(bins) == expect_length
