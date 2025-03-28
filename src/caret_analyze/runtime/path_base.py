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

from abc import ABCMeta, abstractmethod
from copy import deepcopy
from logging import getLogger

import math

import numpy as np
import pandas as pd

from ..exceptions import Error, InvalidRecordsError
from ..record import (DataFrameShaper,
                      RecordsFactory,
                      RecordsInterface,
                      Strip)

logger = getLogger(__name__)


class PathBase(metaclass=ABCMeta):
    """Base class for Latency."""

    def __init__(self) -> None:
        self.__records_cache: RecordsInterface | None = None

    def to_records(self) -> RecordsInterface:
        """
        Calculate records.

        Returns
        -------
        RecordsInterface
            Execution time of each operation.

        """
        return self.__records.clone()

    @abstractmethod
    def _to_records_core(self) -> RecordsInterface:
        """
        Calculate records.

        Returns
        -------
        RecordsInterface
            Execution time of each operation.

        """

    def clear_cache(self) -> None:
        self.__records_cache = None

    @property
    def __records(self) -> RecordsInterface:
        if self.__records_cache is None:
            try:
                self.__records_cache = self._to_records_core()
            except Error as e:
                logger.warning(e)
                self.__records_cache = RecordsFactory.create_instance()

        assert self.__records_cache is not None
        return self.__records_cache

    @property
    def column_names(self) -> list[str]:
        """
        Get column names.

        Returns
        -------
        list[str]
            column names

        """
        return deepcopy(self.to_records().columns)

    def to_dataframe(
        self,
        remove_dropped: bool = False,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0,
        *,
        shaper: DataFrameShaper | None = None,
    ) -> pd.DataFrame:
        """
        Calculate dataframe.

        Parameters
        ----------
        remove_dropped : bool
            If true, eliminate the records that caused the drop.
        treat_drop_as_delay : bool
            Convert dropped records as a delay.
            Valid only when remove_dropped=false.
        lstrip_s: float | None
            Remove from beginning. [s]
        rstrip_s: float | None
            Remove from end [s]
        shaper: DataFrameShaper | None
            shaper

        Returns
        -------
        pandas.DataFrame
            Execution time of each operation.

        """
        records = self.to_records()
        column_names = self.column_names

        if remove_dropped is False and treat_drop_as_delay:
            records.bind_drop_as_delay()

        df = records.to_dataframe()
        for column in column_names:
            if column in df.columns:
                continue
            # TODO(hsgwa): For pandas Int64, pd.NA should be used for missing values.
            df[column] = np.nan
        df = df[column_names]

        if lstrip_s > 0 or rstrip_s > 0:
            strip = Strip(lstrip_s, rstrip_s)
            df = strip.execute(df)
        if shaper:
            df = shaper.execute(df)

        if remove_dropped:
            df.dropna(inplace=True)

        for missing_column in set(column_names) - set(df.columns):
            # TODO(hsgwa): For pandas Int64, pd.NA should be used for missing values.
            df[missing_column] = np.nan

        return df

    def to_timeseries(
        self,
        remove_dropped: bool = False,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0,
        *,
        shaper: DataFrameShaper | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate timeseries data.

        Parameters
        ----------
        remove_dropped : bool | None
            If true, eliminate the records that caused the drop. default: False.
        treat_drop_as_delay : bool | None
            Convert dropped records as a delay.
            Valid only when remove_dropped=false. default: False.
        lstrip_s: float | None
            Remove from beginning. [s] default: 0
        rstrip_s: float | None
            Remove from end [s] default: 0
        shaper: DataFrameShaper | None
            shaper

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            time[ns], latency[ns]
            len(time) == len(latency)

        Raises
        ------
        InvalidRecordsError
            Failed to find any records.

        """
        df = self.to_dataframe(
            remove_dropped, treat_drop_as_delay, lstrip_s, rstrip_s, shaper=shaper)

        if len(df.columns) == 0:
            msg = 'Failed to calculate time series latency.'
            msg += 'There is a possibility that records are dummy data.'
            raise InvalidRecordsError(msg)

        if len(df) == 0:
            msg = 'Failed to find any records that went through the path.'
            msg += 'There is a possibility that all records are lost.'
            raise InvalidRecordsError(msg)
        source_stamps_ns = np.array(df.iloc[:, 0].values)
        dest_stamps_ns = np.array(df.iloc[:, -1].values)
        t = source_stamps_ns

        latency_ns = dest_stamps_ns - source_stamps_ns
        if remove_dropped:
            t = t.astype('int64')
            latency_ns = latency_ns.astype('int64')
        return t, latency_ns

    def to_histogram(
        self,
        binsize_ns: int = 1000000,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0,
        *,
        shaper: DataFrameShaper | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate histogram data.

        Parameters
        ----------
        binsize_ns : int
            bin size for histogram. default 1ms.
        treat_drop_as_delay : bool
            Convert dropped records as a delay.
        lstrip_s: float | None
            Remove from beginning. [s]
        rstrip_s: float | None
            Remove from end [s]
        shaper: DataFrameShaper | None
            shaper

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latency[ns]

            len(frequency)+1 == len(latency)

        See Also
        --------
            https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        remove_dropped = not treat_drop_as_delay
        _, latency_ns = self.to_timeseries(
            remove_dropped, treat_drop_as_delay, lstrip_s, rstrip_s, shaper=shaper)
        latency_ns = latency_ns[[not pd.isnull(_) for _ in latency_ns]]
        range_min = math.floor(min(latency_ns) / binsize_ns) * binsize_ns
        range_max = math.ceil(max(latency_ns) / binsize_ns) * binsize_ns
        bin_num = math.ceil((range_max - range_min) / binsize_ns)
        return np.histogram(latency_ns, bins=bin_num, range=(range_min, range_max))
