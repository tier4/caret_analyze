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

from typing import List, Optional

from .column import ColumnValue
from .interface import RecordsInterface
from .record_factory import RecordFactory, RecordsFactory


class Period:

    def __init__(
        self,
        records: RecordsInterface,
        target_column: Optional[str] = None
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate period.
        target_column : Optional[str], optional
            Column name of timestamps used in the calculation, by default None
            If None, the first column of records is selected.

        """
        self._target_column = target_column or records.columns[0]
        self._target_timestamps: List[int] = []
        for record in records:
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
        if not self._target_timestamps:
            return records

        for i in range(1, len(self._target_timestamps)):
            records.append(RecordFactory.create_instance(
                {self._target_column: self._target_timestamps[i],
                 'period': self._target_timestamps[i] - self._target_timestamps[i-1]}
            ))

        return records

    def _create_empty_records(
        self
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(columns=[
            ColumnValue(self._target_column), ColumnValue('period')
        ])
