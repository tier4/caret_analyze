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

from copy import deepcopy
from typing import Callable, Dict, List, Optional, Sequence, Tuple, Union

from record_cpp_impl import RecordBase, RecordsBase

from .column import Column, Columns
from .record import RecordInterface, Records, RecordsInterface, validate_rename_rule
from ..common import Progress
from ..exceptions import InvalidArgumentError, ItemNotFoundError


class RecordCppImpl(RecordBase, RecordInterface):
    pass


class RecordsCppImpl(RecordsInterface):

    def __init__(
        self,
        init: Optional[List[RecordInterface]] = None,
        columns: Optional[List[Column]] = None,
    ):
        columns = columns or []
        Records._validate(init, columns)
        column_names = [str(c) for c in columns]
        self._records = RecordsBase(init or [], column_names)
        self._name_to_column = {}
        if columns is not None:
            for c in columns:
                assert isinstance(c, Column)
            self._name_to_column = {str(c): c for c in columns}

    @property
    def column_names(self) -> List[str]:
        return self._records.columns

    def export_json(self, path: str) -> None:
        import json

        data_dict = [dic.data for dic in self.data]
        s = json.dumps(data_dict)

        with open(path, mode='w') as f:
            f.write(s)
        return None

    def append(
        self,
        record: RecordInterface
    ) -> None:
        unknown_columns = set(record.columns) - set(self.column_names)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)

        self._records.append(record)

    def concat(
        self, other: RecordsInterface
    ) -> None:
        assert isinstance(other, RecordsCppImpl)
        unknown_columns = set(other.column_names) - set(self.column_names)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)
        self._records.concat(other._records)
        return None

    def sort(
        self,
        key: Union[str, List[str]],
        ascending=True
    ) -> None:
        keys = key if isinstance(key, list) else [key]

        not_exist = set(keys) - set(self.column_names)
        if len(not_exist) > 0:
            raise InvalidArgumentError(f'column [{not_exist}] not found.')
        self._records.sort(keys, ascending)
        return None

    def get_column(
        self,
        column_name: str
    ) -> Column:
        if column_name in self._name_to_column:
            return self._name_to_column[column_name]
        raise ItemNotFoundError(f'Failed to find column: {column_name}')

    def bind_drop_as_delay(self) -> None:
        self._records.bind_drop_as_delay()

    def to_dataframe(self):
        data_dict = [record.data for record in self.data]
        return Records._to_dataframe(data_dict, self.columns)

    def rename_columns(
        self,
        column_names: Dict[str, str]
    ) -> None:
        validate_rename_rule(column_names, self.column_names)
        self._records.rename_columns(column_names)

        for k, v in column_names.items():
            if k in self._name_to_column:
                old_column = self._name_to_column.pop(k)
                new_column = old_column.create_renamed(v)
                self._name_to_column[v] = new_column
        return None

    def merge(
        self,
        right_records: RecordsInterface,
        join_left_key: Union[str, List[str]],
        join_right_key: Union[str, List[str]],
        how: str,
        *,
        progress_label: Optional[str] = None
    ) -> RecordsCppImpl:
        progress_label = progress_label or ''
        assert how in ['inner', 'left', 'right', 'outer']
        assert isinstance(right_records, RecordsCppImpl)

        if isinstance(join_left_key, str):
            join_left_keys = [join_left_key]
        else:
            join_left_keys = join_left_key
        if isinstance(join_right_key, str):
            join_right_keys = [join_right_key]
        else:
            join_right_keys = join_right_key

        if not (set(join_left_keys) <= set(self.column_names)):
            raise InvalidArgumentError('Failed to find column')

        if not set(join_right_keys) <= set(right_records.column_names):
            raise InvalidArgumentError('Failed to find column')

        columns = Columns(self.columns + right_records.columns).as_list()
        column_names = [str(c) for c in columns]

        merged_cpp_base = self._records.merge(
            right_records._records, join_left_keys, join_right_keys,
            column_names, how,
            Progress.records_label(progress_label))

        merged = RecordsCppImpl()
        merged._insert_records(merged_cpp_base)
        merged._insert_columns(self.columns + right_records.columns)
        return merged

    def groupby(
        self,
        column_names: List[str]
    ) -> Dict[Tuple[int, ...], RecordsInterface]:
        assert 0 < len(column_names) and len(column_names) <= 3

        group_cpp_base = self._records.groupby(*column_names)
        group: Dict[Tuple[int, ...], RecordsInterface] = {}
        for k, v in group_cpp_base.items():
            records = RecordsCppImpl()
            records._insert_records(v)
            records._insert_columns(self.columns)
            group[k] = records
        return group

    def get_row_series(
        self,
        index: int
    ) -> RecordInterface:
        if index >= len(self.data):
            raise InvalidArgumentError('index exceeds the row size.')
        return self.data[index]

    def get_column_series(
        self,
        column_name: str
    ) -> Sequence[Optional[int]]:
        return Records._get_column_series_core(self, column_name)

    @property
    def columns(self) -> List[Column]:
        return [self._name_to_column[c] for c in self._records.columns]

    def equals(
        self,
        other: RecordsInterface
    ) -> bool:
        if not isinstance(other, RecordsCppImpl):
            return False
        return self._records.equals(other._records) and self.columns == other.columns

    def reindex(self, column_names: List[str]) -> None:
        miss_match_columns = set(column_names) ^ set(self.column_names)
        if len(miss_match_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{miss_match_columns}'
            raise InvalidArgumentError(msg)
        self._records.reindex(column_names)

    def clone(
        self
    ) -> RecordsCppImpl:
        records_clone = self._records.clone()
        records = RecordsCppImpl(None, deepcopy(self.columns))
        records._insert_records(records_clone)
        return records

    def _insert_records(
        self,
        records: RecordsBase
    ) -> None:
        self._records = records

    def _insert_columns(
        self,
        columns: List[Column]
    ) -> None:
        for c in columns:
            self._name_to_column[str(c)] = c

    def append_column(
        self,
        column: Union[Column, str],
        values: List[int]
    ) -> None:
        assert isinstance(column, Column) or isinstance(column, str)

        if len(values) != len(self):
            raise InvalidArgumentError('len(values) != len(records)')

        if isinstance(column, Column):
            column_tmp = column
        elif isinstance(column, str):
            column_tmp = Column(column)
        self._records.append_column(column_tmp.column_name, values)
        self._name_to_column[column_tmp.column_name] = column_tmp

    def drop_columns(self, columns: List[str]) -> None:
        if not isinstance(columns, list):
            raise InvalidArgumentError('columns must be list.')
        self._records.drop_columns(columns)
        for c in columns:
            if c in self._name_to_column:
                self._name_to_column.pop(c)

    def filter_if(
        self,
        f: Callable[[RecordInterface], bool]
    ) -> None:
        self._records.filter_if(f)

    @property
    def data(self) -> Sequence[RecordInterface]:
        return self._records.data

    def merge_sequencial(
        self,
        right_records: RecordsInterface,
        left_stamp_key: str,
        right_stamp_key: str,
        join_left_key: Optional[Union[str, List[str]]],
        join_right_key: Optional[Union[str, List[str]]],
        how: str,
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsInterface:
        progress_label = progress_label or ''
        assert isinstance(right_records, RecordsCppImpl)
        columns = Columns(self.columns + right_records.columns).as_list()
        join_left_key = join_left_key or []
        join_right_key = join_right_key or []
        join_left_keys = [join_left_key] if isinstance(join_left_key, str) else join_left_key
        join_right_keys = [join_right_key] if isinstance(join_right_key, str) else join_right_key

        missing_left_columns = set(join_left_keys) - set(self.column_names)
        if len(missing_left_columns) > 0:
            raise InvalidArgumentError(f'Failed to find columns. {missing_left_columns}')
        missing_right_columns = set(join_right_keys) - set(right_records.column_names)
        if len(missing_right_columns) > 0:
            raise InvalidArgumentError(f'Failed to find columns. {missing_right_columns}')

        merged_cpp_base = self._records.merge_sequencial(
            right_records._records,
            left_stamp_key,
            right_stamp_key,
            join_left_keys,
            join_right_keys,
            [str(c) for c in columns],
            how,
            Progress.records_label(progress_label)
        )

        merged = RecordsCppImpl()
        merged._insert_records(merged_cpp_base)
        merged._insert_columns(columns)
        return merged

    def merge_sequencial_for_addr_track(
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
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsInterface:
        assert isinstance(copy_records, RecordsCppImpl)
        assert isinstance(sink_records, RecordsCppImpl)

        source_columns = {source_stamp_key, source_key}
        copy_columns = {copy_stamp_key, copy_from_key, copy_to_key}
        sink_columns = {sink_stamp_key, sink_from_key}
        if not source_columns <= set(self.column_names):
            raise InvalidArgumentError('Failed to find columns')
        if not copy_columns <= set(copy_records.column_names):
            raise InvalidArgumentError('Failed to find columns')
        if not sink_columns <= set(sink_records.column_names):
            raise InvalidArgumentError('Failed to find columns')

        progress_label = progress_label or ''
        merged_cpp_base = self._records.merge_sequencial_for_addr_track(
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

        merged = RecordsCppImpl()
        merged._insert_records(merged_cpp_base)
        merged._insert_columns(self.columns + sink_records.columns)
        return merged
