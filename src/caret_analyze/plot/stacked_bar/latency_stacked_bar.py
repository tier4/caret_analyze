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

import pandas as pd

from ...record import RecordsInterface, ResponseTime, StackedBar
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
            # TODO: apply xaxis_type, now only support system time.

        Returns
        -------
        pd.DataFrame
            Latency dataframe.

        """
        # NOTE: returned columns aren't used because they don't include 'start time'
        # TODO: delete 1e-6
        stacked_bar_dict, _ = self.to_stacked_bar_data()
        millisecond_dict: dict[str, list[float]] = {}
        if xaxis_type == 'system_time':
            for column in stacked_bar_dict:
                millisecond_dict[column] = \
                    [timestamp * 1e-6 for timestamp in stacked_bar_dict[column]]
            df = pd.DataFrame(millisecond_dict)
            return df
        elif xaxis_type == 'index':
            raise NotImplementedError()
        else:  # sim_time
            raise NotImplementedError()

    def to_stacked_bar_data(
        self,
    ) -> tuple[dict[str, list[int]], list[str]]:
        """
        Get stacked bar dict and columns.

        Returns
        -------
        dict[str, list[int]], list[str]
            Stacked bar dict.
            Columns (not include 'start time').

        """
        response_records: RecordsInterface = \
            self._get_response_time_record(self._target_objects)
        stacked_bar = StackedBar(response_records)
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

        """
        response_time = ResponseTime(target_object.to_records(),
                                     columns=target_object.column_names)
        # include timestamp of response time (best, worst)
        if self._case == 'all':
            return response_time.to_all_stacked_bar()
        elif self._case == 'best':
            return response_time.to_best_case_stacked_bar()
        elif self._case == 'worst':
            return response_time.to_worst_case_stacked_bar()
        elif self._case == 'worst-with-external-latency':
            return response_time.to_worst_with_external_latency_case_stacked_bar()
        else:
            raise ValueError('optional argument "case" must be following: \
                                "all", "best", "worst", "worst-with-external-latency".')

    @property
    def target_objects(self) -> Path:
        return self._target_objects
