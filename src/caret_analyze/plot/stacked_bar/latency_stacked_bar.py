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

import pandas as pd

from ..util import get_clock_converter
from ...common import ClockConverter
from ...record import ColumnValue, RecordsInterface, ResponseTime, StackedBar
from ...runtime import Path


class LatencyStackedBar:
    """Class that provides latency stacked bar data."""

    def __init__(
        self,
        target_objects: Path,
        case: str = 'all',
    ) -> None:
        self._target_objects = target_objects
        self._case = case

    def to_dataframe(
        self,
        xaxis_type: str = 'system_time'
    ) -> pd.DataFrame:
        """
        Get latency stacked bar data in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str, optional
            X axis value's type , by default 'system_time'.

        Returns
        -------
        pd.DataFrame
            Latency dataframe.

        Raises
        ------
        NotImplementedError
            Argument xaxis_type is not "system_time" or "sim_time".

        """
        # NOTE: returned columns aren't used because they don't include 'start time'
        # TODO: delete 1e-6
        stacked_bar_dict, _ = self.to_stacked_bar_data(xaxis_type)
        millisecond_dict: dict[str, list[float]] = {}
        if xaxis_type == 'system_time' or xaxis_type == 'sim_time':
            for column in stacked_bar_dict:
                millisecond_dict[column] = \
                    [timestamp * 1e-6 for timestamp in stacked_bar_dict[column]]
            df = pd.DataFrame(millisecond_dict)
            return df
        else:  # index
            raise NotImplementedError()

    def to_stacked_bar_data(
        self,
        xaxis_type: str = 'system_time'
    ) -> tuple[dict[str, list[int]], list[str]]:
        """
        Get stacked bar dict and columns.

        Parameters
        ----------
        xaxis_type : str, optional
            X axis value's type , by default 'system_time'.

        Returns
        -------
        dict[str, list[int]], list[str]
            Stacked bar dict.
            Columns (not include 'start time').

        """
        converter: ClockConverter | None = None
        if xaxis_type == 'sim_time':
            converter = get_clock_converter([self._target_objects])
        response_records: RecordsInterface = \
            self._get_response_time_record(self._target_objects)
        stacked_bar = StackedBar(response_records, converter=converter)
        return stacked_bar.to_dict(), stacked_bar.columns

    def _get_response_time_record(
        self,
        target_object: Path
    ) -> RecordsInterface:
        """
        Get response time records.

        Parameters
        ----------
        target_object : Path
            Target path object.

        Returns
        -------
        RecordsInterface
            Response time records of the path.

        Raises
        ------
        ValueError
            - Case is not "all", "best", "worst", or "worst-with-external-latency".

        """
        response_time = ResponseTime(target_object.to_records(),
                                     columns=target_object.column_names)
        # include timestamp of response time (best, worst)
        if self._case == 'all':
            record_if = response_time.to_all_stacked_bar()
        elif self._case == 'best':
            record_if = response_time.to_best_case_stacked_bar()
        elif self._case == 'worst':
            record_if = response_time.to_worst_case_stacked_bar()
        elif self._case == 'worst-with-external-latency':
            record_if = response_time.to_worst_with_external_latency_case_stacked_bar()
        else:
            raise ValueError('optional argument "case" must be following: \
                                "all", "best", "worst", "worst-with-external-latency".')

        if response_time._has_invalid_timestamps() is True:
            columns = []
            columns += [ColumnValue(column) for column in response_time._records._columns]
            columns += [ColumnValue('invalid_timestamps')]
            record_if = response_time._records._create_empty_records(columns)

        return record_if

    @property
    def target_objects(self) -> Path:
        """
        Get target objects.

        Returns
        -------
        Path
            target objects.

        """
        return self._target_objects
