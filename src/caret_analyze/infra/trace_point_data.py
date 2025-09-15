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

from collections.abc import Callable, Sequence
from copy import deepcopy
from typing import Any

from multimethod import multimethod as singledispatchmethod

import pandas as pd


class TracePointIntermediateData:
    """Intermediate data for reading trace points."""

    def __init__(
        self,
        columns: Sequence[str],
        dtypes: dict[str, str] | None = None,
        *,
        exclusion_columns_for_drop_duplicates: list[str] | None = None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        columns : Sequence[str]
            Column names.
        dtypes : Sequence[str]
            Type given to pandas dtypes. ['Int64', 'object'].
            This argument is used to manually determine which columns may be None.
            For columns that do not contain None,
            pandas.DataFrame.convert_dtypes will automatically determine it.
        exclusion_columns_for_drop_duplicates : list[str] | None
            Column names to ignore when performing drop_duplicates.
            Default value is ['timestamp']

        """
        self._data: dict[str, Any] = {column: [] for column in columns}
        self._columns = list(columns)
        self._dtypes = dtypes
        self._exclusion_columns = exclusion_columns_for_drop_duplicates or ['timestamp']

    def __len__(self) -> int:
        return len(self._data)

    def append(self, series_data: dict[str, Any]):
        """
        Append single row.

        Parameters
        ----------
        series_data : dict[str, Any]
            row values.

        """
        for k, v in series_data.items():
            self._data[k].append(v)

        missing_columns = set(self._columns) - set(series_data.keys())
        for column in missing_columns:
            self._data[column].append(None)

    def get_finalized(self, index_column: str | None = None) -> TracePointData:
        """
        Get finalized data.

        Parameters
        ----------
        index_column : str | None, optional
            column name to set as index, by default None.
            If None, an index number is created.

        Returns
        -------
        TracePointData
            Finalized data

        """
        df = pd.DataFrame(self._data, columns=self._columns)
        if self._dtypes:
            df = df.astype(self._dtypes)  # type: ignore

        subset_for_drop_duplicates = list(set(df.columns) - set(self._exclusion_columns))
        df.drop_duplicates(subset_for_drop_duplicates, inplace=True)

        if index_column:
            df.set_index(index_column, inplace=True, drop=True)

        return TracePointData(df)

    @property
    def columns(self) -> list[str]:
        """
        Get column names.

        Returns
        -------
        list[str]
            column names.

        """
        return list(self._columns)


class TracePointData:
    """
    Class to store TracepointData.

    DataFrame.convert_dtypes() internally and store it as
    nullable Int64 or object type.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        df : pd.DataFrame
            internal data.

        Note:
        ----
        Note that even in cases where you want the data to be recognized as Int64,
        if all data is None, it will be converted as an object.

        """
        self._df: pd.DataFrame = df.convert_dtypes()

    @staticmethod
    def concat(
        trace_point_data: Sequence[TracePointData],
        columns: Sequence[str]
    ) -> TracePointData:
        """
        Concatenate TracePointData.

        Parameters
        ----------
        trace_point_data : Sequence[TracePointData]
            data to concatenate
        columns : Sequence[str]
            column names to be used.

        Returns
        -------
        TracePointData
            concatenated data.

        """
        concat_targets = []

        for data in trace_point_data:
            has_columns = (set(data.columns) & set(columns)) == set(columns)
            if not has_columns:
                continue
            concat_targets.append(data.df[list(columns)])

        return TracePointData(pd.concat(concat_targets, axis=0).reset_index(drop=True))

    def __len__(self) -> int:
        return len(self._df)

    @property
    def columns(self) -> list[str]:
        """
        Column names.

        Returns
        -------
        list[str]
            column names.

        """
        return list(self._df.columns)

    def iat(self, row: int, column: int) -> Any:
        """
        Get value.

        Parameters
        ----------
        row : int
            row index.
        column : int
            column index.

        Returns
        -------
        Any
            selected value.

        """
        return self._df.iat[row, column]

    def at(self, row: Any, column: Any) -> Any:
        """
        Get value.

        Parameters
        ----------
        row : Any
            row value.
        column : Any
            column value.

        Returns
        -------
        Any
            selected value.

        """
        return self._df.at[row, column]

    def add_column(
        self,
        column: str,
        f: Callable[[pd.Series], Any]
    ) -> None:
        """
        Add column.

        Parameters
        ----------
        column : str
            column name to be added.
        f : Callable[[pd.Series], Any]
            column value for each row.

        """
        df = deepcopy(self._df)

        data = []
        for i in range(len(df)):
            row = df.iloc[i, :]
            data.append(f(row))
        df[column] = data

        self._df = df

    def remove_column(self, column: str) -> None:
        """
        Remove column.

        Parameters
        ----------
        column : str
            column name to be removed.

        """
        self._df.drop(column, axis=1, inplace=True)

    def filter_rows(self, column: str, value: Any) -> None:
        """
        Filter data.

        Parameters
        ----------
        column : str
            column name to be used.
        value : Any
            value for match.

        """
        if column == 'index':
            filtered = self._df[self._df.index == value]
        else:
            filtered = self._df[self._df[column] == value]

        self._df = self._ensure_df(filtered)

    def rename_column(self, old: str, new: str) -> None:
        """
        Rename column name.

        Parameters
        ----------
        old : str
            column name to be renamed.
        new : str
            new column name.

        """
        self._df.rename(columns={old: new}, inplace=True)

    @singledispatchmethod
    def merge(self, arg) -> None:
        """
        Merge data.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError('')

    @merge.register
    def _merge_single_join_key(
        self,
        other: TracePointData,
        on: str,
        how='inner',
        *,
        drop_columns: list[str] | None = None
    ) -> None:
        self._merge_impl(other, on, how, drop_columns=drop_columns)

    @merge.register
    def _merge_multiple_join_key(
        self,
        other: TracePointData,
        on: list[str],
        how='inner',
        *,
        drop_columns: list[str] | None = None
    ) -> None:
        self._merge_impl(other, on, how, drop_columns=drop_columns)

    def _merge_impl(
        self,
        other: TracePointData,
        on: list[str] | str,
        how='inner',
        *,
        drop_columns: list[str] | None = None
    ) -> None:
        """
        Merge TracePointData.

        Parameters
        ----------
        other : TracePointData
            data to be merged
        on : list[str] | str
            column names for matching
        how : str, optional
            merge method, by default 'inner'
        drop_columns : list[str] | None, optional
            column names to be dropped, by default None

        """
        def drop(df: pd.DataFrame, drop_columns: list[str]) -> pd.DataFrame:
            columns = list(set(df) & set(drop_columns))
            if len(columns) == 0:
                return df
            return df.drop(columns, axis=1)

        left_df = self._df
        right_df = other.df
        if drop_columns:
            left_df = drop(left_df, drop_columns)
            right_df = drop(right_df, drop_columns)

        if isinstance(on, str):
            assert on in set(self.columns)
            assert on in set(other.columns)
        else:
            assert set(on) <= set(self.columns)
            assert set(on) <= set(other.columns)

        self._df = pd.merge(
            left_df,
            right_df,
            left_on=on,
            right_on=on,
            how=how  # type: ignore
        )

    def set_columns(
        self,
        columns: list[str]
    ) -> None:
        """
        Set columns.

        Create columns that are not existed.
        Remove columns that are not specified.

        Parameters
        ----------
        columns : list[str]
            column names.

        """
        df = self._df.copy()
        for missing_column in set(columns) - set(self._df.columns):
            df[missing_column] = pd.NA
        self._df = df[columns]

    def drop_duplicate(self) -> None:
        """Remove duplicated rows."""
        self._df.drop_duplicates(inplace=True)

    def drop_row(self, index: Sequence[int]) -> None:
        """
        Drop row.

        Parameters
        ----------
        index : Sequence[int]
            index value to be dropped.

        """
        self._df.drop(index=index, inplace=True)

    def sort(self, column_name: str) -> None:
        """
        Sort rows by values.

        Parameters
        ----------
        column_name : str
            column name to be sorted.

        """
        self._df.sort_values(column_name, inplace=True)

    def drop_column(self, column_name: str) -> None:
        """
        Drop column.

        Parameters
        ----------
        column_name : str
            column name to be dropped.

        """
        self._df.drop(columns=column_name, inplace=True, axis=1)

    def clone(self) -> TracePointData:
        """
        Return cloned instance.

        Returns
        -------
        TracePointData
            deep copied instance.

        """
        return deepcopy(self)

    def reset_index(self) -> None:
        """
        Reset index.

        Old index are set as a new column.

        """
        df = self._df.reset_index()
        self._df = df.convert_dtypes()

    @property
    def df(self) -> pd.DataFrame:
        """
        Return data frame.

        Returns
        -------
        pd.DataFrame
            data

        """
        return self._ensure_df(self._df.convert_dtypes())

    @staticmethod
    def _ensure_df(data) -> pd.DataFrame:
        if isinstance(data, pd.Series):
            return pd.DataFrame([data])
        else:
            return data
