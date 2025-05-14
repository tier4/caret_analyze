# Copyright 2021 TIER IV, Inc.
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

import numpy as np

from ..column import ColumnValue
from ..interface import RecordsInterface
from ..record_factory import RecordsFactory
from ...common import ClockConverter


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
        converter: ClockConverter | None = None
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
        converter : ClockConverter | None, optional
            Converter to simulation time.

        Returns
        -------
        RecordsInterface
            frequency records.
            Columns
            - {timestamp_column}
            - {frequency_column}

        """
        if not self._target_timestamps:
            return self._create_empty_records()

        if base_timestamp is None:
            base_timestamp = self._target_timestamps[0]
        if until_timestamp is None:
            until_timestamp = self._target_timestamps[-1]

        timestamp_list, frequency_list = self._get_frequency_with_timestamp(
            interval_ns,
            base_timestamp,
            until_timestamp,
            converter=converter
        )

        records = self._create_empty_records()
        for ts, freq in zip(timestamp_list, frequency_list):
            record = {self._target_column: ts, 'frequency': freq}
            records.append(record)

        return records

    def _create_empty_records(self) -> RecordsInterface:
        columns = [ColumnValue(self._target_column), ColumnValue('frequency')]
        return RecordsFactory.create_instance(None, columns=columns)

    def _get_frequency_with_timestamp(
        self,
        interval_ns: int,
        base_timestamp: int,
        until_timestamp: int,
        converter: ClockConverter | None = None
    ) -> tuple[list[int], list[int]]:
        timestamp_array = np.array(self._target_timestamps).astype(np.int64)
        if converter:
            base_timestamp = round(converter.convert(base_timestamp))
            until_timestamp = round(converter.convert(until_timestamp))
            convert_vector = np.vectorize(converter.convert)
            timestamp_array = np.round(convert_vector(timestamp_array)).astype(np.int64)

        timestamp_array = timestamp_array[timestamp_array >= base_timestamp]
        timestamp_array = timestamp_array[timestamp_array <= until_timestamp]
        if len(timestamp_array) == 0:
            return [base_timestamp], [0]

        # frequency is the number of timestamps that appear in each interval
        interval_index_array = (timestamp_array - base_timestamp) // interval_ns
        frequency_list = np.bincount(interval_index_array).tolist()
        expected_freq_list_length = int(np.ceil((until_timestamp - base_timestamp) / interval_ns))
        frequency_list += [0] * (expected_freq_list_length - len(frequency_list))

        interval_start_time_array = np.arange(len(frequency_list)) * interval_ns + base_timestamp
        timestamp_list = interval_start_time_array.tolist()

        return timestamp_list, frequency_list
