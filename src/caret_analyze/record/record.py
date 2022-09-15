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
from enum import IntEnum
from itertools import groupby
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import pandas as pd

from .column import Column, Columns, ColumnValue
from .interface import RecordInterface, RecordsInterface
from ..exceptions import InvalidArgumentError


class MergeSide(IntEnum):
    LEFT = 0
    RIGHT = 1


# class Record(collections.UserDict, RecordInterface):
class Record(RecordInterface):

    def __init__(self, init: Optional[Dict] = None) -> None:
        init = init or {}
        self._data = init or {}
        self._columns = set(init.keys())

    def get(self, key: str) -> int:
        return self._data[key]

    def get_with_default(self, key: str, v: int) -> int:
        return self._data.get(key, v)

    @property
    def data(self) -> Dict[str, int]:
        return self._data

    @property
    def columns(self) -> Set[str]:
        return deepcopy(self._columns)

    def drop_columns(self, columns: List[str]) -> None:
        if not isinstance(columns, list):
            raise InvalidArgumentError('columns must be list.')

        data: Dict[str, int]

        data = self._data

        for column in columns:
            if column not in self.columns:
                continue
            del data[column]

        self._columns -= set(columns)
        return None

    def equals(self, other: RecordInterface) -> bool:
        is_columns_equal = self.columns == other.columns
        if is_columns_equal is False:
            return False
        return self.data == other.data

    def add(self, key: str, stamp: int):
        self._columns.add(key)
        self._data[key] = stamp

    def merge(self, other: RecordInterface) -> None:
        self._data.update(other.data)
        self._columns |= other.columns

    def change_dict_key(self, old_key: str, new_key: str) -> None:
        self._data[new_key] = self._data.pop(old_key, None)
        self._columns -= {old_key}
        self._columns |= {new_key}


class Records(RecordsInterface):

    def __init__(
        self,
        init: Optional[Sequence[RecordInterface]] = None,
        column_values: Optional[Sequence[ColumnValue]] = None
    ) -> None:
        init_: List[RecordInterface] = [] if init is None else list(init)
        column_values = [] if column_values is None else list(column_values)

        column_names = [str(c) for c in column_values]
        self._validate(init_, column_names)
        self._data: List[RecordInterface] = init_
        self._columns: Columns = Columns(column_values or [])

    @staticmethod
    def _validate(
        init: Optional[List[RecordInterface]],
        columns: Optional[List[str]]
    ) -> None:
        init = init or []
        columns = columns or []

        columns_set = set(columns)
        for record in init:
            unknown_column = set(record.columns) - columns_set
            if len(unknown_column) > 0:
                msg = 'Contains an unknown columns. '
                msg += f'{unknown_column}'
                raise InvalidArgumentError(msg)

        if len(set(columns)) != len(columns):
            from itertools import groupby
            msg = 'columns must be unique. '
            columns = sorted(columns)
            msg += 'duplicated columns: '
            for key, group in groupby(columns):
                if len(list(group)) >= 2:
                    msg += f'{key}, '

            raise InvalidArgumentError(msg)

    def __len__(self) -> int:
        return len(self.data)

    @property
    def columns(self) -> List[str]:
        return self._columns.column_names

    def sort(
        self, key: str, sub_key: Optional[str] = None, ascending=True
    ) -> None:
        data_ = self.data

        if ascending:
            if sub_key is not None:
                data_.sort(
                    key=lambda record: (record.get(key), record.get(sub_key))  # type: ignore
                )
            else:
                data_.sort(key=lambda record: record.get(key))
        else:
            if sub_key is not None:
                data_.sort(
                    key=lambda record: (-record.get(key), - record.get(sub_key))  # type: ignore
                )
            else:
                data_.sort(key=lambda record: -record.get(key))

        return None

    def sort_column_order(
        self,
        ascending=True,
        put_none_at_top=True,
    ) -> None:
        data_ = self.data
        maxsize = 2**64 - 1

        if ascending:
            default_value = maxsize if put_none_at_top else 0

            def sort_func(record: RecordInterface):
                return tuple(record.get_with_default(k, default_value) for k in self.columns)
        else:
            default_value = 0 if put_none_at_top else maxsize

            def sort_func(record: RecordInterface):
                return tuple(-record.get_with_default(k, default_value) for k in self.columns)

        data_.sort(key=sort_func)

        return None

    @property
    def data(self) -> List[RecordInterface]:
        return self._data

    def _append_dict(self, other: Dict[str, int]):
        record = Record(other)
        self._append_record(record)

    def _append_record(self, other: RecordInterface):
        self._data.append(other)
        unknown_columns = set(other.columns) - set(self.columns)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)

    def concat(self, other: RecordsInterface) -> None:
        unknown_columns = set(other.columns) - set(self.columns)
        if len(unknown_columns) > 0:
            msg = 'Contains an unknown columns. '
            msg += f'{unknown_columns}'
            raise InvalidArgumentError(msg)
        self._data += list(other.data)

    def drop_columns(self, columns: List[str]) -> None:
        data_: List[RecordInterface]

        self._columns.drop(columns)
        data_ = self._data

        for record in data_:
            record.drop_columns(columns)
        return None

    def rename_columns(self, columns: Dict[str, str]) -> None:
        validate_rename_rule(columns)

        data_: List[RecordInterface]
        data_ = self._data

        for record in data_:
            for key_from, key_to in columns.items():
                if key_from not in record.columns:
                    continue
                record.change_dict_key(key_from, key_to)

        self._columns.rename(columns)
        return None

    def append_column(self, column: ColumnValue, values: List[int]) -> None:
        assert isinstance(column, ColumnValue)

        if len(values) != len(self):
            raise InvalidArgumentError('len(values) != len(records)')

        self._columns.append(Column(column))
        for record, value in zip(self.data, values):
            record.add(column.column_name, value)

    def filter_if(self, f: Callable[[RecordInterface], bool]) -> None:
        records = Records(None, self._columns.to_value())
        for record in self._data:
            if f(record):
                records.append(record)

        self._data = records._data
        return None

    def equals(self, records: RecordsInterface) -> bool:
        if len(self.data) != len(records.data):
            return False

        for r, r_ in zip(self.data, records.data):
            if r.equals(r_) is False:
                return False

        # TODO(hsgwa): fix protected variable accessing.
        if self._columns.to_value() != records._columns.to_value():
            return False

        return True

    def reindex(self, columns: List[str]) -> None:
        err_columns = set(self.columns) ^ set(columns)
        if len(err_columns) > 0:
            msg = 'Column names do not match. '
            for err_column in err_columns:
                msg += f'{err_column}, '
            raise InvalidArgumentError(msg)

        self._columns.reindex(columns)

    def to_dataframe(self) -> pd.DataFrame:
        pd_dict = [record.data for record in self.data]
        return self._to_dataframe(pd_dict, self.columns)

    def get_column_series(self, column_name: str) -> Sequence[Optional[int]]:
        return self._get_column_series_core(self, column_name)

    def get_row_series(self, index: int) -> RecordInterface:
        if index >= len(self.data):
            raise InvalidArgumentError('index exceeds the row size.')
        return self.data[index]

    @staticmethod
    def _get_column_series_core(records: RecordsInterface, column_name: str):
        if column_name not in records.columns:
            raise InvalidArgumentError(f'Unknown column_name: {column_name}')
        l: List[Optional[int]] = []
        for datum in records.data:
            if column_name in datum.columns:
                l.append(datum.get(column_name))
            else:
                l.append(None)
        return l

    @staticmethod
    def _to_dataframe(
        df_list: List[Dict[str, int]],
        columns: List[str]
    ) -> pd.DataFrame:
        # When from_dict is used,
        # dataframe values are rounded to a float type,
        # so here uses a dictionary type.
        df_dict: Dict[str, List[Optional[int]]]
        df_dict = {c: [None]*len(df_list) for c in columns}
        for i, df_row in enumerate(df_list):
            for c in columns:
                if c in df_row:
                    df_dict[c][i] = df_row[c]

        df = pd.DataFrame(df_dict, dtype='Int64')

        missing_columns = set(columns) - set(df.columns)
        df_miss = pd.DataFrame(columns=missing_columns)
        df = pd.concat([df, df_miss])
        return df[columns]

    def clone(self) -> Records:
        from copy import deepcopy

        return deepcopy(self)

    def bind_drop_as_delay(self) -> None:
        self.sort_column_order(ascending=False, put_none_at_top=False)

        oldest_values: Dict[str, int] = {}

        for record in self.data:
            for key in self.columns:
                if key not in record.columns and key in oldest_values.keys():
                    record.add(key, oldest_values[key])
                if key in record.columns:
                    oldest_values[key] = record.get(key)

        self.sort_column_order(ascending=True, put_none_at_top=True)

    def merge(
        self,
        right_records: RecordsInterface,
        join_left_key: str,
        join_right_key: str,
        columns: List[str],
        how: str,
        *,
        progress_label: Optional[str] = None  # unused
    ) -> Records:
        maxsize = 2**64 - 1
        self._validate(None, columns)

        left_records = self.clone()
        merge_left = how in ['left', 'outer']
        merge_right = how in ['right', 'outer']

        assert how in ['inner', 'left', 'right', 'outer']

        column_side = '_tmp_side'
        column_merge_stamp = '_tmp_stamp'
        column_has_valid_join_key = '_tmp_has_valid_join_key'
        column_join_key = '_tmp_join_key'
        column_found_right_record = '_tmp_found_right_record'

        left_records.append_column(ColumnValue(column_side), [MergeSide.LEFT]*len(left_records))
        right_records.append_column(ColumnValue(column_side), [MergeSide.RIGHT]*len(right_records))

        concat_columns = Columns.from_str(
            left_records.columns +
            right_records.columns +
            [
                column_side, column_has_valid_join_key, column_merge_stamp, column_join_key
            ]
        )
        concat_records = Records(None, concat_columns.to_value())
        concat_records.concat(left_records)
        concat_records.concat(right_records)

        record: RecordInterface

        for record in concat_records.data:
            if record.get(column_side) == MergeSide.LEFT:
                join_key = join_left_key
            if record.get(column_side) == MergeSide.RIGHT:
                join_key = join_right_key

            has_valid_join_key = join_key in record.columns
            record.add(column_has_valid_join_key, has_valid_join_key)
            if has_valid_join_key:
                record.add(column_merge_stamp, record.get(join_key))
                record.add(column_join_key, record.get(
                    join_key))  # type: ignore
            else:
                record.add(column_merge_stamp, maxsize)

        concat_records.sort(key=column_merge_stamp, sub_key=column_side)

        empty_records: List[RecordInterface] = []
        left_records_: List[RecordInterface] = []
        processed_stamps: Set[int] = set()

        merged_records = Records(
            None,
            Columns.from_str(
                concat_records.columns +
                [column_found_right_record]
            ).to_value()
        )

        def move_left_to_empty(
            left: List[RecordInterface],
            empty: List[RecordInterface]
        ):
            for left_record in left_records_:
                if left_record.get(column_found_right_record) is False:
                    empty_records.append(left_record)

        for record in concat_records._data:

            if record.get(column_has_valid_join_key) is False:
                empty_records.append(record)
                continue

            join_value = record.get(column_join_key)
            if join_value not in processed_stamps:
                move_left_to_empty(left_records_, empty_records)
                left_records_ = []
                processed_stamps.add(join_value)

            if record.get(column_side) == MergeSide.LEFT:
                record.add(column_found_right_record, False)
                left_records_.append(record)
                continue

            for left_record in left_records_:
                left_record.add(column_found_right_record, True)
                merged_record = deepcopy(record)
                merged_record.merge(left_record)
                merged_records.append(merged_record)

            if len(left_records_) == 0:
                empty_records.append(record)

        move_left_to_empty(left_records_, empty_records)

        for record in empty_records:
            side = record.get(column_side)
            if side == MergeSide.LEFT and merge_left:
                merged_records.append(record)
            elif side == MergeSide.RIGHT and merge_right:
                merged_records.append(record)

        temporary_columns = [column_side, column_merge_stamp, column_join_key,
                             column_has_valid_join_key, column_found_right_record]

        merged_records.drop_columns(temporary_columns)
        left_records.drop_columns(temporary_columns)
        right_records.drop_columns(temporary_columns)

        merged_records.reindex(columns)

        return merged_records

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
        progress_label: Optional[str] = None  # unused
    ) -> RecordsInterface:
        maxsize = 2**64 - 1
        self._validate(None, columns)

        assert how in ['inner', 'left', 'right', 'outer', 'left_use_latest']
        left_records = self

        merge_left = how in ['left', 'outer', 'left_use_latest']
        bind_latest_left_record = how in ['left_use_latest']
        merge_right = how in ['right', 'outer']

        column_side = '_tmp_side'
        column_has_valid_join_key = '_tmp_has_valid_join_key'
        column_merge_stamp = '_tmp_stamp'
        column_has_merge_stamp = '_tmp_has_merge_stamp'
        column_sub_records = '_tmp_sub_records'

        left_records.append_column(ColumnValue(column_side), [MergeSide.LEFT]*len(left_records))
        right_records.append_column(ColumnValue(column_side), [MergeSide.RIGHT]*len(right_records))

        concat_columns = Columns.from_str(
            left_records.columns +
            right_records.columns
            + [
                column_has_valid_join_key, column_merge_stamp, column_has_merge_stamp
            ]
        )
        concat_records = Records(None, concat_columns.to_value())
        concat_records.concat(left_records)
        concat_records.concat(right_records)

        for record in concat_records.data:
            if record.get(column_side) == MergeSide.LEFT:
                record.add(column_has_valid_join_key,
                           join_left_key is None or join_left_key in record.columns)
            else:
                record.add(column_has_valid_join_key,
                           join_right_key is None or join_right_key in record.columns)

            if record.get(column_side) == MergeSide.LEFT and left_stamp_key in record.columns:
                record.add(column_merge_stamp, record.get(left_stamp_key))
                record.add(column_has_merge_stamp, True)
            elif record.get(column_side) == MergeSide.RIGHT and right_stamp_key in record.columns:
                record.add(column_has_merge_stamp, True)
                record.add(column_merge_stamp, record.get(right_stamp_key))
            else:
                record.add(column_merge_stamp, maxsize)
                record.add(column_has_merge_stamp, False)

        def get_join_value(record: RecordInterface) -> Optional[int]:
            if record.get(column_side) == MergeSide.LEFT:
                join_key = join_left_key
            else:
                join_key = join_right_key

            if join_key is None:
                return 0
            elif join_key in record.columns:
                return record.get(join_key)
            else:
                return None

        concat_records.sort(key=column_merge_stamp, sub_key=column_side)

        to_left_records: Dict[int, RecordInterface] = {}
        for record in concat_records.data:
            if not record.get(column_has_merge_stamp):
                continue

            if record.get(column_side) == MergeSide.LEFT:
                record.add(column_sub_records, [])  # type: ignore

                join_value = get_join_value(record)
                if join_value is None:
                    continue
                to_left_records[join_value] = record
            elif record.get(column_side) == MergeSide.RIGHT:
                join_value = get_join_value(record)
                if join_value is None:
                    continue
                if join_value not in to_left_records.keys():
                    continue
                left_record_to_be_bind = to_left_records[join_value]
                left_record_to_be_bind.data[column_sub_records].append(record)  # type: ignore

        merged_records = Records(
            None,
            Columns.from_str(
                concat_records.columns + [column_sub_records]
            ).to_value()
        )

        added: Set[RecordInterface] = set()
        for current_record in concat_records.data:
            recorded = current_record in added
            if recorded:
                continue

            if not current_record.get(column_has_merge_stamp) or not current_record.get(
                column_has_valid_join_key
            ):
                if current_record.get(column_side) == MergeSide.RIGHT and merge_right:
                    merged_records.append(current_record)
                    added.add(current_record)
                elif current_record.get(column_side) == MergeSide.LEFT and merge_left:
                    merged_records.append(current_record)
                    added.add(current_record)
                continue

            if current_record.get(column_side) == MergeSide.RIGHT:
                if merge_right:
                    merged_records.append(current_record)
                    added.add(current_record)
                continue

            sub_records: List[RecordInterface]
            sub_records = current_record.data[column_sub_records]  # type: ignore

            if sub_records == []:
                if merge_left:
                    merged_records.append(current_record)
                    added.add(current_record)
                continue

            for i, sub_record in enumerate(sub_records):
                if 1 <= i and not bind_latest_left_record:
                    break
                if sub_record in added:
                    if merge_left:
                        merged_records.append(current_record)
                        added.add(current_record)
                    continue

                merged_record: RecordInterface = Record()
                merged_record.merge(current_record)
                merged_record.merge(sub_record)
                merged_records.append(merged_record)
                added.add(current_record)
                added.add(sub_record)

        temporary_columns = [
            column_side,
            column_merge_stamp,
            column_has_merge_stamp,
            column_has_valid_join_key,
            column_sub_records,
        ]
        merged_records.drop_columns(temporary_columns)
        left_records.drop_columns(temporary_columns)
        right_records.drop_columns(temporary_columns)

        merged_records.reindex(columns)

        return merged_records

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
        progress_label: Optional[str] = None  # unused
    ) -> Records:
        assert isinstance(copy_records, Records)
        assert isinstance(sink_records, Records)

        column_type = '_tmp_type'
        column_timestamp = '_tmp_timestamp'

        source_records = self.clone()
        copy_records = copy_records.clone()
        sink_records = sink_records.clone()

        source_records.append_column(
            ColumnValue(column_type), [RecordType.SOURCE]*len(source_records))
        copy_records.append_column(ColumnValue(column_type), [RecordType.COPY]*len(copy_records))
        sink_records.append_column(ColumnValue(column_type), [RecordType.SINK]*len(sink_records))

        source_timestamps = [r.get(source_stamp_key) for r in source_records.data]
        source_records.append_column(ColumnValue(column_timestamp), source_timestamps)
        copy_records.rename_columns({copy_stamp_key: column_timestamp})
        sink_timestamps = [r.get(sink_stamp_key) for r in sink_records.data]
        sink_records.append_column(ColumnValue(column_timestamp), sink_timestamps)

        merged_records_column = Columns.from_str(
            source_records.columns +
            copy_records.columns +
            sink_records.columns
        )
        merged_records: Records = Records(None, merged_records_column.to_value())

        concat_records = Records(source_records._data + copy_records._data + sink_records._data,
                                 merged_records_column.to_value())
        concat_records.sort(column_timestamp, ascending=False)
        # Searching for records in chronological order is not good
        # because the lost records stay forever. Sort in reverse chronological order.

        #  Dict of records to be added by sink and removed by source
        processing_records: Dict[int, RecordInterface] = {}

        sink_from_keys = sink_from_key + '_'

        def merge_processing_record_keys(processing_record: RecordInterface):
            for processing_record_ in filter(
                lambda x: x.get(sink_from_keys) & processing_record.get(
                    sink_from_keys)
                and x.get(sink_from_keys) != processing_record.get(sink_from_key),
                processing_records.values(),
            ):
                processing_record_keys = processing_record.get(sink_from_keys)
                corresponding_record_keys = processing_record_.get(
                    sink_from_keys)

                merged_set = processing_record_keys | corresponding_record_keys
                processing_record.data[sink_from_keys] = merged_set
                processing_record_.data[sink_from_keys] = merged_set

        for record in concat_records.data:

            if record.get(column_type) == RecordType.SINK:
                addr = record.get(sink_from_key)
                record.data[sink_from_keys] = {record.get(sink_from_key)}  # type: ignore
                processing_records[addr] = record

            elif record.get(column_type) == RecordType.COPY:
                records_need_to_merge = filter(
                    lambda x: record.get(copy_to_key) in x.data[sink_from_keys],  # type: ignore
                    processing_records.values()
                )
                for processing_record in records_need_to_merge:
                    processing_record.data[sink_from_keys].add(  # type: ignore
                        record.get(copy_from_key))
                    merge_processing_record_keys(processing_record)
                    # No need for subsequent loops since we integrated them.
                    break

            elif record.get(column_type) == RecordType.SOURCE:
                merged_addrs = []
                for processing_record in filter(
                    lambda x: record.get(source_key) in x.data[sink_from_keys],  # type: ignore
                    processing_records.values(),
                ):
                    addr = processing_record.get(sink_from_key)
                    merged_addrs.append(addr)
                    processing_record.merge(record)
                    merged_records.append(processing_record)
                for addr in merged_addrs:
                    if addr in processing_records:
                        processing_records.pop(addr)

        # Deleting an added key
        merged_records.drop_columns(
            [column_type, column_timestamp, sink_from_key,
             copy_from_key, copy_to_key, copy_stamp_key])

        merged_records.reindex(columns)
        for record in merged_records.data:
            record.data.pop(sink_from_keys)

        return merged_records

    def groupby(self, columns: List[str]) -> Dict[Tuple[int, ...], RecordsInterface]:
        group: Dict[Tuple[int, ...], RecordsInterface] = {}

        m = 2**64 - 1
        for record in self._data:
            k = tuple(record.get_with_default(column, m) for column in columns)
            if k not in group:
                column_values = [ColumnValue(c) for c in self._columns.column_names]
                group[k] = Records(None, column_values)
            group[k].append(record)

        return group


def merge(
    left_records: RecordsInterface,
    right_records: RecordsInterface,
    join_left_key: str,
    join_right_key: str,
    columns: List[str],
    how: str,
    *,
    progress_label: Optional[str] = None
) -> RecordsInterface:
    assert type(left_records) == type(right_records)

    return left_records.merge(
        right_records,
        join_left_key,
        join_right_key,
        columns,
        how,
        progress_label=progress_label
    )


def merge_sequential(
    left_records: RecordsInterface,
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
    assert type(left_records) == type(right_records)

    return left_records.merge_sequential(
        right_records,
        left_stamp_key,
        right_stamp_key,
        join_left_key,
        join_right_key,
        columns,
        how,
        progress_label=progress_label,
    )


class RecordType(IntEnum):
    SOURCE = (0,)
    COPY = (1,)
    SINK = 2


def merge_sequential_for_addr_track(
    source_records: RecordsInterface,
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
    progress_label: Optional[str] = None
):
    assert type(source_records) == type(copy_records) and type(
        copy_records) == type(sink_records)

    return source_records.merge_sequential_for_addr_track(
        source_stamp_key,
        source_key,
        copy_records,
        copy_stamp_key,
        copy_from_key,
        copy_to_key,
        sink_records,
        sink_stamp_key,
        sink_from_key,
        columns,
        progress_label=progress_label,
    )


def validate_rename_rule(rename_rule: Dict[str, str]):
    # overwrite columns
    if len(set(rename_rule.keys()) & set(rename_rule.values())) > 0:
        msg = 'Overwrite columns. '
        msg += str(rename_rule)
        raise InvalidArgumentError(msg)

    # duplicate columns after change
    for _, group in groupby(rename_rule.values()):
        if len(list(group)) > 1:
            msg = 'duplicate columns'
            msg += str(rename_rule)
            raise InvalidArgumentError(msg)
