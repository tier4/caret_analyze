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

from typing import Callable, Dict, List, Optional, Sequence, Tuple

from record_cpp_impl import RecordBase, RecordsBase

from .column import Column, Columns, ColumnValue
from .record import RecordInterface, Records, RecordsInterface, validate_rename_rule
from ..common import Progress
from ..exceptions import InvalidArgumentError


class RecordCppImpl(RecordBase, RecordInterface):
    pass


class RecordsCppImpl(RecordsInterface):

    def __init__(
        self,
        init: Optional[Sequence[RecordInterface]] = None,
        columns: Optional[Sequence[ColumnValue]] = None,
    ):
        columns = columns or []
        column_names = [str(c) for c in columns]
        init_ = [] if init is None else list(init)
        Records._validate(init_, column_names)
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
        other: Dict[str, int]
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
        self, key: str, sub_key: Optional[str] = None, ascending=True
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
        return Records._to_dataframe(data_dict, self.columns)

    def rename_columns(
        self, columns: Dict[str, str]
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
        columns: List[str],
        how: str,
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsCppImpl:
        progress_label = progress_label or ''
        assert how in ['inner', 'left', 'right', 'outer']
        assert isinstance(right_records, RecordsCppImpl)

        merged_cpp_base = self._records.merge(
            right_records._records, join_left_key, join_right_key,
            columns, how,
            Progress.records_label(progress_label))

        column_values = [ColumnValue(c) for c in columns]
        merged = RecordsCppImpl(None, column_values)
        merged._insert_records(merged_cpp_base)
        return merged

    def groupby(
        self,
        columns: List[str]
    ) -> Dict[Tuple[int, ...], RecordsInterface]:
        assert 0 < len(columns) and len(columns) <= 3

        group_cpp_base = self._records.groupby(*columns)
        group: Dict[Tuple[int, ...], RecordsInterface] = {}
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

    def get_column_series(self, column_name: str) -> Sequence[Optional[int]]:
        return Records._get_column_series_core(self, column_name)

    @property
    def columns(self) -> List[str]:
        return self._columns.column_names

    def equals(self, other: RecordsInterface) -> bool:
        if not isinstance(other, RecordsCppImpl):
            return False
        match_records = self._records.equals(other._records)
        match_columns = self._columns.to_value() == other._columns.to_value()
        return match_records and match_columns

    def reindex(self, columns: List[str]) -> None:
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
        values: List[int]
    ) -> None:
        if len(values) != len(self):
            raise InvalidArgumentError('len(values) != len(records)')

        self._columns.append(Column(column))
        self._records.append_column(column.column_name, values)

    def drop_columns(self, column_names: List[str]) -> None:
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
        join_left_key: Optional[str],
        join_right_key: Optional[str],
        columns: List[str],
        how: str,
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsInterface:
        progress_label = progress_label or ''
        assert isinstance(right_records, RecordsCppImpl)
        merged_cpp_base = self._records.merge_sequential(
            right_records._records,
            left_stamp_key,
            right_stamp_key,
            join_left_key or '',
            join_right_key or '',
            columns,
            how,
            Progress.records_label(progress_label)
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
        columns: List[str],
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsInterface:
        assert isinstance(copy_records, RecordsCppImpl)
        assert isinstance(sink_records, RecordsCppImpl)

        progress_label = progress_label or ''
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
            Progress.records_label(progress_label)
        )

        column_values = [ColumnValue(c) for c in columns]
        merged = RecordsCppImpl(None, column_values)
        merged._insert_records(merged_cpp_base)
        return merged
