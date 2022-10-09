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

from typing import List, Optional, Tuple

from .column import ColumnValue
from .interface import RecordsInterface
from .record_factory import RecordFactory, RecordsFactory


class Frequency:

    def __init__(
        self,
        records: RecordsInterface,
        target_column: Optional[str] = None
    ) -> None:
        self._target_column = target_column or records.columns[0]
        self._target_series = [ts for ts in records.get_column_series(
                               self._target_column) if ts is not None]

    def to_records(
        self,
        interval_ns: int = 1000000000,
        base_timestamp: Optional[int] = None
    ) -> RecordsInterface:
        records = self._create_empty_records()
        if not self._target_series:
            return records

        timestamp_list, frequency_list = self._get_freq_with_timestamp(
            interval_ns,
            base_timestamp or self._target_series[0]
        )
        for ts, freq in zip(timestamp_list, frequency_list):
            records.append(RecordFactory.create_instance(
                {self._target_column: ts, 'frequency': freq}
            ))

        return records

    def _create_empty_records(
        self
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(columns=[
            ColumnValue(self._target_column), ColumnValue('frequency')
        ])

    def _get_freq_with_timestamp(
        self,
        interval_ns: int,
        base_timestamp: int
    ) -> Tuple[List[int], List[int]]:
        timestamp_list: List[int] = [base_timestamp]
        frequency_list: List[int] = [0]
        diff_base = base_timestamp

        for timestamp in self._target_series:
            if timestamp - diff_base < interval_ns:
                frequency_list[-1] += 1
            else:
                timestamp_list.append(base_timestamp +
                                      len(timestamp_list) * interval_ns)
                frequency_list.append(1)
                diff_base += interval_ns

        return timestamp_list, frequency_list
