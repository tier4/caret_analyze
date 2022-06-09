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

# from typing import List, Set, Tuple

# from caret_analyze.latency import LatencyBase
# from caret_analyze.record import Record
# from caret_analyze.record import Records

# import numpy as np
# import pytest


# class LatencyBaseImpl(LatencyBase):

#     def __init__(self, records, columns):
#         self._records = records
#         self._columns = columns

#     def to_records(self):
#         return self._records

#     @property
#     def column_names(self) -> List[str]:
#         return self._columns


# class TestLatencyBase:

#     @pytest.mark.parametrize(
#         'records, df_len, columns',
#         [
#             (
#                 Records(
#                     [
#                         Record({'col0': 1, 'col1': 5, 'col2': 3}),
#                     ]
#                 ),
#                 1,
#                 {'col0', 'col1', 'col2'},
#             ),
#             (
#                 Records(
#                     [
#                         Record({'col0': 1, 'col2': 3}),
#                         Record({'col1': 6, 'col2': 4}),
#                         Record({'col0': 2, 'col1': 6, 'col2': 4}),
#                     ]
#                 ),
#                 3,
#                 {'col0', 'col1', 'col2'},
#             ),
#         ],
#     )
#     def test_to_dataframe(self, mocker, records: Records, df_len: int, columns: Set[str]):
#         latency = LatencyBaseImpl(records, list(columns))
#         df = latency.to_dataframe()

#         assert len(df) == df_len
#         assert set(df.columns) == columns

#         # Make sure the columns are in chronological order from left to right.
#         assert list(df.columns) == list(columns)

#     @pytest.mark.parametrize(
#         'latencies, binsize_ns, bins_range',
#         [
#             ([5132, 12385], 1, [5132, 12385]),
#             ([5132, 12385], 10, [5130, 12390]),
#             ([5132, 12385], 100, [5100, 12400]),
#             ([5132, 12385], 1000, [5000, 13000]),
#         ],
#     )
#     def test_to_histogram(
#         self, mocker, latencies: List[int], binsize_ns: int, bins_range: Tuple[int, int]
#     ):
#         def custom_to_timeseries(
#             remove_dropped: bool = False,
#             treat_drop_as_delay=False,
#             lstrip=0,
#             rstrip=0,
#             *,
#             shaper=None
#         ) -> Tuple[np.array, np.array]:
#             return np.array([]), np.array(latencies)

#         latency = LatencyBaseImpl(Records(), [])
#         mocker.patch.object(latency, 'to_timeseries', custom_to_timeseries)

#         hist, bins = latency.to_histogram(binsize_ns=binsize_ns)
#         assert min(bins) == bins_range[0] and max(bins) == bins_range[1]
#         expect_length = (max(bins) - min(bins)) / binsize_ns + 1
#         assert len(bins) == expect_length
