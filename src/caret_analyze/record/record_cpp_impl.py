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

import pandas as pd

from record_cpp_impl import RecordBase, RecordsBase

from .column import Column, Columns, ColumnValue
from .interface import RecordInterface, RecordsInterface
from .record_operations import validate_rename_rule
from ..exceptions import InvalidArgumentError


class RecordCppImpl(RecordBase, RecordInterface):
    pass


class RecordsCppImpl(RecordsInterface):

    def __init__(
        self,
        init: Sequence[RecordInterface] | None = None,
        columns: Sequence[ColumnValue] | None = None,
    ):
        columns = columns or []
        column_names = [str(c) for c in columns]
        init_ = [] if init is None else list(init)
        RecordsCppImpl._validate(init_, column_names)
        self._columns = Columns(columns)
        self._records = RecordsBase(init_, column_names)

    def export_yaml(self, path: str) -> None:
        import yaml

        data_dict = [dic.data for dic in self.data]
        s = yaml.dump(data_dict)

        with open(path, mode='w') as f:
            f.write(s)
        return None

    def _append_dict(
        self,
        other: dict[str, int]
    ) -> None:
        record = RecordBase(other)
        self._records.append(record)

    def _append_record(
        self,
        other: RecordInterface
    ) -> None:
        unknown_columns = set(other.columns) - set(self.columns)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)

        self._records.append(other)

    def concat(
        self, other: RecordsInterface
    ) -> None:
        assert isinstance(other, RecordsCppImpl)
        unknown_columns = set(other.columns) - set(self.columns)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)
        self._records.concat(other._records)
        return None

    def sort(
        self, key: str, sub_key: str | None = None, ascending=True
    ) -> None:
        if key not in self.columns:
            raise InvalidArgumentError(f'column [{key}] not found.')
        self._records.sort(key, sub_key or '', ascending)
        return None

    def sort_column_order(
        self,
        ascending: bool = True,
        put_none_at_top=True,
    ) -> None:
        self._records.sort_column_order(ascending, put_none_at_top)

    def bind_drop_as_delay(self) -> None:
        self._records.bind_drop_as_delay()

    def to_dataframe(self):
        data_dict = [record.data for record in self.data]
        return RecordsCppImpl._to_dataframe(data_dict, self.columns)

    def rename_columns(
        self, columns: dict[str, str]
    ) -> None:
        validate_rename_rule(columns)
        self._records.rename_columns(columns)
        self._columns.rename(columns)
        return None

    def merge(
        self,
        right_records: RecordsInterface,
        join_left_key: str,
        join_right_key: str,
        columns: list[str],
        how: str,
    ) -> RecordsCppImpl:
        assert how in ['inner', 'left', 'right', 'outer']
        assert isinstance(right_records, RecordsCppImpl)

        RecordsCppImpl._validate_merge_records(columns, self, right_records)

        merged_cpp_base = self._records.merge(
            right_records._records, join_left_key, join_right_key,
            columns, how)

        column_values = [ColumnValue(c) for c in columns]
        merged = RecordsCppImpl(None, column_values)
        merged._insert_records(merged_cpp_base)
        return merged

    def groupby(
        self,
        columns: list[str]
    ) -> dict[tuple[int, ...], RecordsInterface]:
        assert 0 < len(columns) and len(columns) <= 3

        group_cpp_base = self._records.groupby(*columns)
        group: dict[tuple[int, ...], RecordsInterface] = {}
        for k, v in group_cpp_base.items():
            column_values = [ColumnValue(c) for c in self.columns]
            records = RecordsCppImpl(None, column_values)
            records._insert_records(v)
            group[k] = records
        return group

    def get_row_series(self, index: int) -> RecordInterface:
        if index >= len(self.data):
            raise InvalidArgumentError('index exceeds the row size.')
        return self.data[index]

    def get_column_series(self, column_name: str) -> Sequence[int | None]:
        return RecordsCppImpl._get_column_series_core(self, column_name)

    @property
    def columns(self) -> list[str]:
        return self._columns.column_names

    def equals(self, other: RecordsInterface) -> bool:
        if not isinstance(other, RecordsCppImpl):
            return False
        match_records = self._records.equals(other._records)
        match_columns = self._columns.to_value() == other._columns.to_value()
        return match_records and match_columns

    def reindex(self, columns: list[str]) -> None:
        miss_match_columns = set(columns) ^ set(self.columns)
        if len(miss_match_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{miss_match_columns}'
            raise InvalidArgumentError(msg)
        self._records.reindex(columns)
        self._columns.reindex(columns)

    def clone(self) -> RecordsCppImpl:
        records_clone = self._records.clone()
        records = RecordsCppImpl(None, self._columns.to_value())
        records._insert_records(records_clone)
        return records

    def _insert_records(self, records: RecordsBase) -> None:
        self._records = records

    def append_column(
        self,
        column: ColumnValue,
        values: list[int]
    ) -> None:
        if len(values) != len(self):
            raise InvalidArgumentError('len(values) != len(records)')

        self._columns.append(Column(column))
        self._records.append_column(column.column_name, values)

    def drop_columns(self, column_names: list[str]) -> None:
        if not isinstance(column_names, list):
            raise InvalidArgumentError('columns must be list.')
        self._columns.drop(column_names)
        self._records.drop_columns(column_names)

    def filter_if(self, f: Callable[[RecordInterface], bool]) -> None:
        self._records.filter_if(f)

    @property
    def data(self) -> Sequence[RecordInterface]:
        """
        Get records list.

        Returns
        -------
        Sequence[RecordInterface]
            Records list.

        Warnings
        --------
            Each Execution copies all records.
            An implementation such as records.data[i] will significantly degrade performance.
            It's strongly recommend to store records.data in a temporary variable or
            use it as an iterable.

        See Also
        --------
            https://pybind11.readthedocs.io/en/stable/advanced/cast/stl.html#binding-stl-containers

        """
        return self._records.data

    def merge_sequential(
        self,
        right_records: RecordsInterface,
        left_stamp_key: str,
        right_stamp_key: str,
        join_left_key: str | None,
        join_right_key: str | None,
        columns: list[str],
        how: str,
    ) -> RecordsInterface:
        RecordsCppImpl._validate_merge_records(columns, self, right_records)

        assert isinstance(right_records, RecordsCppImpl)
        merged_cpp_base = self._records.merge_sequential(
            right_records._records,
            left_stamp_key,
            right_stamp_key,
            join_left_key or '',
            join_right_key or '',
            columns,
            how,
        )

        column_values = [ColumnValue(c) for c in columns]
        merged = RecordsCppImpl(None, column_values)
        merged._insert_records(merged_cpp_base)
        return merged

    def merge_sequential_for_addr_track(
        self,
        source_stamp_key: str,
        source_key: str,
        copy_records: RecordsInterface,
        copy_stamp_key: str,
        copy_from_key: str,
        copy_to_key: str,
        sink_records: RecordsInterface,
        sink_stamp_key: str,
        sink_from_key: str,
        columns: list[str],
    ) -> RecordsInterface:
        assert isinstance(copy_records, RecordsCppImpl)
        assert isinstance(sink_records, RecordsCppImpl)

        RecordsCppImpl._validate_merge_records(columns, self, copy_records, sink_records)

        merged_cpp_base = self._records.merge_sequential_for_addr_track(
            source_stamp_key,
            source_key,
            copy_records._records,
            copy_stamp_key,
            copy_from_key,
            copy_to_key,
            sink_records._records,
            sink_stamp_key,
            sink_from_key,
        )

        column_values = [ColumnValue(c) for c in columns]
        merged = RecordsCppImpl(None, column_values)
        merged._insert_records(merged_cpp_base)
        return merged

    @staticmethod
    def _validate(
        init: list[RecordInterface] | None,
        columns: list[str] | None
    ) -> None:
        init = init or []
        columns = columns or []

        columns_set = set(columns)
        for record in init:
            RecordsCppImpl.__validate_unknown_columns(set(record.columns), columns_set)

        RecordsCppImpl.__validate_duplicated_columns(columns)

    @staticmethod
    def __validate_duplicated_columns(columns: Sequence[str]):
        if len(set(columns)) != len(columns):
            from itertools import groupby
            msg = 'columns must be unique. '
            columns = sorted(columns)
            msg += 'duplicated columns: '
            for key, group in groupby(columns):
                if len(list(group)) >= 2:
                    msg += f'{key}, '

            raise InvalidArgumentError(msg)

    @staticmethod
    def __validate_unknown_columns(
        selected_columns: set[str],
        columns: set[str]
    ) -> None:
        unknown_column = selected_columns - columns
        if len(unknown_column) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_column}'
            raise InvalidArgumentError(msg)

    @staticmethod
    def _validate_merge_records(
        columns: list[str] | None,
        *records_args: RecordsInterface,
    ) -> None:
        columns = columns or []

        columns_set = set(columns)
        columns_set_ = set()
        for records in records_args:
            columns_set_ |= set(records.columns)

        RecordsCppImpl.__validate_unknown_columns(columns_set, columns_set_)

        RecordsCppImpl.__validate_duplicated_columns(columns)

    @staticmethod
    def _get_column_series_core(records: RecordsInterface, column_name: str):
        if column_name not in records.columns:
            raise InvalidArgumentError(f'Unknown column_name: {column_name}')
        l: list[int | None] = []
        for datum in records.data:
            if column_name in datum.columns:
                l.append(datum.get(column_name))
            else:
                l.append(None)
        return l

    @staticmethod
    def _to_dataframe(
        df_list: list[dict[str, int]],
        columns: list[str]
    ) -> pd.DataFrame:
        # When from_dict is used,
        # dataframe values are rounded to a float type,
        # so here uses a dictionary type.
        df_dict: dict[str, list[int | None]]
        df_dict = {c: [None]*len(df_list) for c in columns}

        int64_max = 2**63-1

        for i, df_row in enumerate(df_list):
            for c in columns:
                if c in df_row:
                    # uint64 to int64 conversion.
                    # This is workaround to fix some uint64 trace points.
                    if df_row[c] <= int64_max:
                        df_dict[c][i] = df_row[c]
                    else:
                        # convert uint64 to int64
                        df_dict[c][i] = ~(df_row[c] & int64_max)

        df = pd.DataFrame(df_dict, dtype='Int64')

        missing_columns = list(set(columns) - set(df.columns))
        df_miss = pd.DataFrame(columns=missing_columns)
        df = pd.concat([df, df_miss])
        return df[columns]
