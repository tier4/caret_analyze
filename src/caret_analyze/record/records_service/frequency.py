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

from __future__ import annotations

from collections.abc import Callable

from ..column import ColumnValue
from ..interface import RecordsInterface
from ..record_factory import RecordsFactory


class Frequency:

    def __init__(
        self,
        records: RecordsInterface,
        target_column: str | None = None,
        row_filter: Callable | None = None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate frequency.
        target_column : str | None, optional
            Column name of timestamps used in the calculation, by default None
            If None, the first column of records is selected.
        row_filter : Callable | None, optional
            Filter function to select rows to calculate period, by default None.
            Example:
            ```
            def row_filter(record: RecordInterface) -> bool:
                start_column = 'timestamp1'
                end_column = 'timestamp2'
                if (record.data.get(start_column) is not None
                        and record.data.get(end_column) is not None):
                    return True
                else:
                    return False
            ```

        """
        self._target_column = target_column or records.columns[0]
        self._target_timestamps: list[int] = []
        for record in records:
            if row_filter and not row_filter(record):
                continue
            if self._target_column in record.columns:
                timestamp = record.get(self._target_column)
                self._target_timestamps.append(timestamp)

    def to_records(
        self,
        interval_ns: int = 1000000000,
        base_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> RecordsInterface:
        """
        Calculate frequency records.

        Parameters
        ----------
        interval_ns: int, optional
            Interval used for frequency calculation, by default 1000000000 [ns].
            The number of timestamps that exist in this time interval is counted.
        base_timestamp : int | None, optional
            Initial timestamp used for frequency calculation, by default None.
            If None, earliest timestamp is used.
        until_timestamp : int | None, optional
            End time of measurement.
            If None, oldest timestamp is used.

        Returns
        -------
        RecordsInterface
            frequency records.
            Columns
            - {timestamp_column}
            - {frequency_column}

        """
        records = self._create_empty_records()
        if not self._target_timestamps:
            return records

        timestamp_list, frequency_list = self._get_frequency_with_timestamp(
            interval_ns,
            base_timestamp or self._target_timestamps[0],
            until_timestamp or self._target_timestamps[-1]
        )
        for ts, freq in zip(timestamp_list, frequency_list):
            record = {
                self._target_column: ts,
                'frequency': freq
            }
            records.append(record)

        return records

    def _create_empty_records(
        self
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(None, columns=[
            ColumnValue(self._target_column), ColumnValue('frequency')
        ])

    def _get_frequency_with_timestamp(
        self,
        interval_ns: int,
        base_timestamp: int,
        until_timestamp: int
    ) -> tuple[list[int], list[int]]:
        timestamp_list: list[int] = [base_timestamp]
        frequency_list: list[int] = [0]
        interval_start_time = base_timestamp

        for timestamp in self._target_timestamps:
            if timestamp < base_timestamp:
                continue
            while not (interval_start_time <= timestamp < interval_start_time + interval_ns):
                next_interval_start_time = interval_start_time + interval_ns
                timestamp_list.append(next_interval_start_time)
                frequency_list.append(0)
                interval_start_time = next_interval_start_time
            frequency_list[-1] += 1

        while timestamp_list[-1] + interval_ns <= until_timestamp:
            timestamp_list.append(timestamp_list[-1] + interval_ns)
            frequency_list.append(0)

        return timestamp_list, frequency_list
