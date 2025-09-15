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

from collections.abc import Sequence

import pandas as pd

from ..metrics_base import MetricsBase
from ..util import get_clock_converter
from ...common import Util
from ...record import Frequency, RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription)


def _get_frequency_computing_timestamp_offset_ns(timeseries_record: RecordsInterface) -> int:
    """
    Calculate the offset time for frequency calculation.

    Parameters
    ----------
    timeseries_record : RecordsInterface
        Timeseries record.

    Returns
    -------
    int
        Offset in nanoseconds. The offset is half the interval time.
        For example, if the frequency is 1Hz, the interval is 500ms.

    Notes
    -----
    If the data frequency is 1Hz and calculated without an offset,
    the output may become unstable. For instance, if the interval is 999ms,
    no data is included, resulting in 0Hz. Conversely, if the interval is 1001ms,
    two data points are included, resulting in 2Hz.
    Using an offset ensures stable calculations.

    """
    if len(timeseries_record) == 0:
        return 0

    # Get timestamp array
    timestamp_column_name = timeseries_record.columns[0]
    timestamp_list = timeseries_record.get_column_series(timestamp_column_name)
    if len(timestamp_list) < 2:
        return 0
    if timestamp_list[0] is None or timestamp_list[1] is None:
        return 0

    # Calculate timestamp offset
    # Approximate period estimation using the first two timestamps.
    # Accuracy is not critical, as the offset only needs to prevent boundary collisions.
    timestamp_diff = int(timestamp_list[1]) - int(timestamp_list[0])
    timestamp_offset_ns = int(timestamp_diff / 2)
    return timestamp_offset_ns


class FrequencyTimeSeries(MetricsBase):
    """Class that provides frequency timeseries data."""

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes]
    ) -> None:
        super().__init__(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get frequency timeseries data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        pd.DataFrame
            Multi-column frequency DataFrame.

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        timeseries_records_list = self.to_timeseries_records_list(xaxis_type)
        all_df = pd.DataFrame()
        for to, frequency_records in zip(self.target_objects, timeseries_records_list):
            frequency_df = frequency_records.to_dataframe()
            frequency_df.rename(
                columns={
                    frequency_df.columns[0]: f'{self._get_ts_column_name(to)}',
                    frequency_df.columns[1]: 'frequency [Hz]',
                },
                inplace=True
            )
            # TODO: Multi-column DataFrame are difficult for users to handle,
            #       so it should be a single-column DataFrame.
            frequency_df = self._add_top_level_column(frequency_df, to)
            all_df = pd.concat([all_df, frequency_df], axis=1)

        return all_df.sort_index(level=0, axis=1, sort_remaining=False)

    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> list[RecordsInterface]:
        """
        Get frequency records list of all target objects.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Returns
        -------
        list[RecordsInterface]
            Frequency records list of all target objects.

        """
        if self._target_objects and isinstance(self._target_objects[0], Communication):
            columns = self._target_objects[0].to_records().columns
            start_column = columns[0]
            end_column = columns[1]

            def row_filter_communication(record) -> bool:
                """Return True only if communication is established."""
                comm_start_column = start_column
                comm_end_column = end_column
                if (record.data.get(comm_start_column) is not None
                        and record.data.get(comm_end_column) is not None):
                    return True
                else:
                    return False

        timeseries_records_list: list[RecordsInterface] = [
            obj.to_records() for obj in self._target_objects
        ]

        if xaxis_type == 'sim_time':
            converter = get_clock_converter(self._target_objects)
        else:
            converter = None

        min_time, max_time = self._get_timestamp_range(timeseries_records_list)

        frequency_timeseries_list: list[RecordsInterface] = []
        for records in timeseries_records_list:
            # Calculate the start time to shift calculation period from event period.
            offset_ns = _get_frequency_computing_timestamp_offset_ns(records)
            start_ns = min_time + offset_ns

            # Calculate the end time to exclude periods with insufficient data.
            INTERVAL_NS = 1000000000  # 1 second. Default value to calculate frequency.
            sample_num = (max_time - start_ns) // INTERVAL_NS
            end_ns = start_ns + sample_num*INTERVAL_NS - 1

            frequency = Frequency(
                records,
                row_filter=row_filter_communication
                if isinstance(records, Communication) else None
            )
            frequency_record = frequency.to_records(
                interval_ns=INTERVAL_NS,
                base_timestamp=start_ns,
                until_timestamp=end_ns,
                converter=converter
            )
            frequency_timeseries_list.append(frequency_record)

        return frequency_timeseries_list

    # TODO: Migrate into record.
    @staticmethod
    def _get_timestamp_range(
        timeseries_records_list: list[RecordsInterface]
    ) -> tuple[int, int]:
        all_timestamps = []
        for records in timeseries_records_list:
            if len(records) == 0:
                continue
            timestamps = records.get_column_series(records.columns[0])
            all_timestamps.extend(Util.filter_items(lambda x, : isinstance(x, int), timestamps))

        if len(all_timestamps) == 0:
            return 0, 1  # Intended to show an empty figure.
        else:
            return min(all_timestamps), max(all_timestamps)
