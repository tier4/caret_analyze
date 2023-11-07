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


class Period:

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
            records to calculate period.
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

    def to_records(self) -> RecordsInterface:
        """
        Calculate period records.

        Returns
        -------
        RecordsInterface
            period records.
            Columns
            - {timestamp_column}
            - {period_column}

        """
        records = self._create_empty_records()

        for i in range(1, len(self._target_timestamps)):
            record = {
                self._target_column: self._target_timestamps[i-1],
                'period': self._target_timestamps[i] - self._target_timestamps[i-1]
            }
            records.append(record)

        return records

    def _create_empty_records(
        self
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(None, columns=[
            ColumnValue(self._target_column), ColumnValue('period')
        ])
