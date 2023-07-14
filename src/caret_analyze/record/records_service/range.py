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

from collections.abc import Sequence
from logging import getLogger

from ..interface import RecordsInterface


logger = getLogger(__name__)


class Range:
    """Class that calculates minimum/maximum timestamps from a list of records."""

    def __init__(self, records_list: Sequence[RecordsInterface]) -> None:
        self._records_list = records_list

    def get_range(self) -> tuple[int, int]:
        """
        Get minimum and maximum timestamps.

        Returns
        -------
        tuple[int, int]
            Minimum and Maximum timestamps.

        Notes
        -----
        The first column is system time for now.
        The other columns could be other than system time.
        Only the system time is picked out here.

        """
        def remove_none(series: Sequence[int | None]) -> list[int]:
            return [v for v in series if v is not None]

        base_series = [remove_none(r.get_column_series(r.columns[0])) for r in self._records_list]
        min_series: list[int] = []
        max_series: list[int] = []
        for series in base_series:
            if len(series) > 0:
                min_series.append(min(series))
                max_series.append(max(series))

        has_valid_data = len(min_series) > 0 and len(max_series) > 0
        if has_valid_data:
            range_min = min(min_series)
            range_max = max(max_series)
            return range_min, range_max
        else:
            logger.warning('No valid measurement data.')
            return 0, 1
