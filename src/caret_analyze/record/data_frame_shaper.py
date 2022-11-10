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

from abc import ABCMeta, abstractmethod

import pandas as pd


class DataFrameShaper(metaclass=ABCMeta):
    """Class for formatting pandas.Dataframe."""

    @abstractmethod
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute pandas.Dataframe formatting.

        Parameters
        ----------
        df : pd.DataFrame
            Data frame to format.

        Returns
        -------
        pd.DataFrame
            Formatted data frame.
        """
        pass


class Clip(DataFrameShaper):
    """
    Class that filters only for times in a specified interval.

    The first column is used for time filtering.
    """

    def __init__(self, ltrim_ns: int, rtrim_ns: int) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        ltrim_ns : int
            Minimum time after filtering [ns].
        rtrim_ns : int
            Maximum time after filtering [ns].

        """
        self._ltrim_ns = ltrim_ns
        self._rtrim_ns = rtrim_ns

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute and return formatted data frame.

        Parameters
        ----------
        df : pd.DataFrame
            Data frame to be formatted.

        Returns
        -------
        pd.DataFrame
            Formatted data frame.

        """
        if len(df.columns) == 0:
            return df

        first_column = df.columns[0]

        df_clipped = df[
            (df[first_column] >= self._ltrim_ns) &
            (df[first_column] <= self._rtrim_ns)
        ]

        df_clipped.reset_index(inplace=True, drop=True)
        return df_clipped

    @property
    def min_ns(self) -> int:
        """
        Get minimum time after filtering [ns].

        Returns
        -------
        int
            Minimum time after filtering [ns].

        """
        return self._ltrim_ns

    @property
    def max_ns(self) -> int:
        """
        Get maximum time after filtering [ns].

        Returns
        -------
        int
            Maximum time after filtering [ns].

        """
        return self._rtrim_ns


class Strip(DataFrameShaper):
    """
    Class that strips before and after the measurement for a specified time.

    The first column is used for time filtering.
    """

    def __init__(self, lstrip_s: float, rstrip_s: float) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        lstrip_s : float
            Time to strip from start of measurement [s].
        rstrip_s : float
            Time to strip from end of measurement [s].

        """
        self._lstrip_ns = lstrip_s * 1.0e9
        self._rstrip_ns = rstrip_s * 1.0e9

    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Execute and return formatted data frame.

        Parameters
        ----------
        df : pd.DataFrame
            Data frame to be formatted.

        Returns
        -------
        pd.DataFrame
            Formatted data frame.

        """
        if len(df.columns) == 0 or len(df) == 0:
            return df

        clip = self.to_clip(df)
        return clip.execute(df)

    def to_clip(self, df: pd.DataFrame) -> Clip:
        """
        Convert to Clip.

        Parameters
        ----------
        df : pd.DataFrame
            Data frame for use as a time reference.

        Returns
        -------
        Clip
            Converted clip.

        """
        if len(df.columns) == 0 or len(df) == 0:
            return Clip(0, 1)
        first_column = df.columns[0]
        start_ns = df.at[0, first_column]
        end_ns = df.at[len(df)-1, first_column]

        stripped_start_ns = start_ns + self._lstrip_ns
        stripped_end_ns = end_ns - self._rstrip_ns
        clip = Clip(stripped_start_ns, stripped_end_ns)

        return clip
