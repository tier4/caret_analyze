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
from typing import List, Optional, Callable, Dict, Set
from enum import Enum
import pandas as pd
import sys

from abc import ABCMeta, abstractmethod


class RecordInterface(metaclass=ABCMeta):
    @abstractmethod
    def equals(self, other: RecordInterface) -> bool:
        pass

    @abstractmethod
    def merge(self, other: RecordInterface, inplace=False) -> Optional[Record]:
        pass

    @abstractmethod
    def drop_columns(self, columns: List[str], inplace: bool = False) -> Optional[Record]:
        pass

    @abstractmethod
    def add(self, key: str, stamp: int) -> None:
        pass

    @abstractmethod
    def change_dict_key(self, old_key: str, new_key: str) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> int:
        pass

    @property
    @abstractmethod
    def data(self) -> Dict[str, int]:
        pass

    @property
    @abstractmethod
    def columns(self) -> Set[str]:
        pass


class RecordsInterface(metaclass=ABCMeta):
    @abstractmethod
    def equals(self, other: RecordsInterface) -> bool:
        pass

    @abstractmethod
    def append(self, other: RecordInterface) -> None:
        pass

    @abstractmethod
    def concat(self, other: RecordsInterface, inplace=False) -> Optional[RecordsInterface]:
        pass

    @abstractmethod
    def sort(self, key: str) -> None:
        pass

    def filter(
        self, f: Callable[[RecordInterface], bool], inplace: bool = False
    ) -> Optional[RecordsInterface]:
        pass

    @property
    @abstractmethod
    def data(self) -> List[RecordInterface]:
        pass

    @abstractmethod
    def drop_columns(
        self, columns: List[str], inplace: bool = False
    ) -> Optional[RecordsInterface]:
        pass

    @abstractmethod
    def rename_columns(
        self, columns: Dict[str, str], inplace: bool = False
    ) -> Optional[RecordsInterface]:
        pass

    @property
    @abstractmethod
    def columns(self) -> Set[str]:
        pass

    @abstractmethod
    def merge(
        self,
        records_: Records,
        join_key: str,
        how: str = "inner",
        record_sort_key: Optional[str] = None,
        record_sort_key_: Optional[str] = None,
    ) -> Records:
        pass

    @abstractmethod
    def merge_sequencial(
        self,
        right_records: Records,
        left_stamp_key: str,
        right_stamp_key: str,
        join_key: Optional[str],
        how: str = "inner",
        *,
        left_sort_key: Optional[str] = None,
        right_sort_key: Optional[str] = None,
    ) -> Records:
        pass

    @abstractmethod
    def merge_sequencial_with_copy(
        self,
        source_stamp_key: str,
        source_key: str,
        copy_records: Records,
        copy_stamp_key: str,
        copy_from_key: str,
        copy_to_key: str,
        sink_records: Records,
        sink_stamp_key: str,
        sink_from_key: str,
    ) -> RecordsInterface:
        pass


class MergeSideInfo(Enum):
    LEFT = 1
    RIGHT = 2


# class Record(collections.UserDict, RecordInterface):
class Record(RecordInterface):
    def __init__(self, init: Optional[Dict] = None):
        init = init or {}
        self._data = init or {}
        self._columns = set(init.keys())

    def get(self, key: str) -> int:
        return self._data[key]

    @property
    def data(self) -> Dict[str, int]:
        return self._data

    @property
    def columns(self) -> Set[str]:
        return self._columns

    def drop_columns(self, columns: List[str], inplace=False) -> Optional[Record]:
        data: Dict[str, int]

        if inplace:
            data = self._data
        else:
            data = deepcopy(self)._data

        for column in columns:
            if column not in self.columns:
                continue
            del data[column]

        if inplace:
            self._columns -= set(columns)
            return None
        else:
            return Record(data)

    def equals(self, other: Record) -> bool:  # type: ignore
        is_columns_equal = self.columns == other.columns
        if is_columns_equal is False:
            return False
        return self.data == other.data

    def add(self, key: str, stamp: int):
        self.columns.add(key)
        self._data[key] = stamp

    def merge(self, other: Record, inplace=False) -> Optional[Record]:  # type: ignore
        if inplace:
            self._data.update(other.data)
            self._columns |= other.columns
            return None
        else:
            d = deepcopy(self.data)
            d.update(deepcopy(other.data))
            return Record(d)

    def change_dict_key(self, old_key: str, new_key: str) -> None:
        self._data[new_key] = self._data.pop(old_key, None)
        self._columns -= set([old_key])
        self._columns |= set([new_key])


# class Records(collections.UserList, RecordsInterface):
class Records(RecordsInterface):
    def __init__(self, init: List[Record] = None):
        self._columns: Set[str] = set()
        for record in init or []:
            self._columns |= record.columns
        self._data: List[Record] = init or []

    @property
    def columns(self) -> Set[str]:
        return self._columns

    def sort(self, key: str) -> None:
        self.data.sort(key=lambda record: record.get(key))

    @property
    def data(self) -> List[Record]:  # type: ignore
        return self._data

    def append(self, other: Record):  # type: ignore
        assert isinstance(other, Record)
        self._data.append(other)
        self._columns |= other.columns

    def concat(self, other: Records, inplace=False) -> Optional[Records]:  # type: ignore
        if inplace:
            self._data += other._data
            self._columns |= other.columns
            return None
        else:
            d = deepcopy(self._data)
            d += deepcopy(other._data)
            return Records(d)

    def drop_columns(self, columns: List[str], inplace: bool = False) -> Optional[Records]:
        data: List[Record]

        if inplace:
            data = self._data
        else:
            data = deepcopy(self._data)

        for record in data:
            record.drop_columns(columns, inplace=True)

        if not inplace:
            return Records(data)
        else:
            self._columns -= set(columns)
            return None

    def rename_columns(self, columns: Dict[str, str], inplace: bool = False) -> Optional[Records]:
        self._columns -= set(columns.keys())
        self._columns |= set(columns.values())

        data: List[Record]
        if inplace:
            data = self._data
        else:
            data = deepcopy(self._data)

        for record in data:
            for key_from, key_to in columns.items():
                if key_from not in record.columns:
                    continue
                record.change_dict_key(key_from, key_to)

        if not inplace:
            return Records(data)
        else:
            return None

    def filter(self, f: Callable[[Record], bool], inplace: bool = False) -> Optional[Records]:
        records = Records()
        init_columns = self.columns
        for record in self._data:  # type: Record
            if f(record):
                records.append(record)

        if not inplace:
            records._columns = init_columns
            records._data = deepcopy(records._data)
            return records
        else:
            self._data = records._data
            return None

    def equals(self, records: Records) -> bool:  # type: ignore
        for r, r_ in zip(self.data, records.data):
            if r.equals(r_) is False:
                return False

        return True

    def to_dataframe(self) -> pd.DataFrame:
        pd_dict = [record.data for record in self.data]
        df = pd.DataFrame.from_dict(pd_dict)
        missing_columns = set(self.columns) - set(df.columns)
        df_miss = pd.DataFrame(columns=missing_columns)
        df = pd.concat([df, df_miss])
        return df

    def to_string(self) -> str:
        return self.to_dataframe().to_string()

    def merge(
        self,
        records_: Records,
        join_key: str,
        how: str = "inner",
        record_sort_key: Optional[str] = None,
        record_sort_key_: Optional[str] = None,
    ) -> Records:
        records = self

        merged_records = Records()
        assert how in ["inner", "left", "right", "outer"]

        records.sort(record_sort_key or join_key)
        records_.sort(record_sort_key_ or join_key)

        records_inserted: List[Record] = []

        for record in records._data:
            if join_key not in record.columns:
                if how in ["left", "outer"]:
                    merged_records.append(record)
                continue

            record_inserted = False
            for record_ in records_._data:
                if join_key not in record_.columns:
                    continue
                if record_.get(join_key) == record.get(join_key):
                    records_inserted.append(record_)

                    merged_record = deepcopy(record_)
                    merged_record.merge(record, inplace=True)
                    merged_records.append(merged_record)
                    record_inserted = True
                    break
            if record_inserted:
                continue

            if how in ["left", "outer"]:
                merged_records.append(record)

        if how in ["right", "outer"]:
            for record_ in records_._data:
                if record_ not in records_inserted:
                    merged_records.append(record_)

        return merged_records

    def merge_sequencial(
        self,
        right_records: Records,
        left_stamp_key: str,
        right_stamp_key: str,
        join_key: Optional[str],
        how: str = "inner",
        *,
        left_sort_key: Optional[str] = None,
        right_sort_key: Optional[str] = None,
    ) -> Records:
        assert how in ["inner", "left", "right", "outer"]
        left_records = self

        def is_key_matched(record: Record, record_: Record) -> bool:
            if join_key is None:
                return True
            if join_key not in record.columns or join_key not in record_.columns:
                return False
            return record.data[join_key] == record_.data[join_key]

        merged_records: Records = Records()

        for left in left_records.data:
            left.add("side", MergeSideInfo.LEFT)  # type: ignore

        for right in right_records.data:
            right.add("side", MergeSideInfo.RIGHT)  # type: ignore

        left_sort_key = left_sort_key or left_stamp_key
        right_sort_key = right_sort_key or right_stamp_key

        records: Records
        records = left_records.concat(right_records)  # type: ignore

        for i, record in enumerate(records._data):
            record.add("has_valid_join_key", join_key is None or join_key in record.columns)

            if record.get("side") == MergeSideInfo.LEFT and left_sort_key in record.columns:
                record.add("sort_value", record.get(left_sort_key))
                record.add("has_sort_value", True)
            elif record.get("side") == MergeSideInfo.RIGHT and right_sort_key in record.columns:
                record.add("sort_value", record.get(right_sort_key))
                record.add("has_sort_value", True)
            else:
                record.add("sort_value", sys.maxsize)
                record.add("has_sort_value", False)

        records.sort(key="sort_value")

        added = set()
        for i, current_record in enumerate(records._data):
            recorded = current_record.get("sort_value") in added
            if recorded:
                continue

            if not current_record.get("has_sort_value") or not current_record.get(
                "has_valid_join_key"
            ):
                if current_record.get("side") == MergeSideInfo.RIGHT and how in ["right", "outer"]:
                    merged_records.append(current_record)
                    added.add(current_record.get("sort_value"))
                elif current_record.get("side") == MergeSideInfo.LEFT and how in ["left", "outer"]:
                    merged_records.append(current_record)
                    added.add(current_record.get("sort_value"))
                continue

            if current_record.get("side") == MergeSideInfo.RIGHT:
                if how in ["right", "outer"]:
                    merged_records.append(current_record)
                    added.add(current_record.get("sort_value"))
                continue

            next_record: Optional[Record] = None
            sub_record: Optional[Record] = None
            if join_key is None:
                for record in records._data[i + 1 :]:
                    if next_record is not None and sub_record is not None:
                        break
                    if record.get("has_sort_value") is False:
                        continue
                    if record.get("side") == MergeSideInfo.LEFT and next_record is None:
                        next_record = record
                    if record.get("side") == MergeSideInfo.RIGHT and sub_record is None:
                        sub_record = record
            else:
                for record in records._data[i + 1 :]:
                    if next_record is not None and sub_record is not None:
                        break
                    if record.get("has_sort_value") is False:
                        continue

                    key_matched = record.get(join_key) == current_record.get(join_key)
                    if (
                        key_matched
                        and record.get("side") == MergeSideInfo.LEFT
                        and next_record is None
                    ):
                        next_record = record
                    if (
                        key_matched
                        and record.get("side") == MergeSideInfo.RIGHT
                        and sub_record is None
                    ):
                        sub_record = record

            has_valid_next_record = (
                next_record is not None
                and sub_record is not None
                and next_record.get("sort_value") < sub_record.get("sort_value")
            )
            if (
                has_valid_next_record
                or sub_record is None
                or sub_record.get("sort_value") in added
            ):
                if how in ["left", "outer"]:
                    merged_records.append(current_record)
                    added.add(current_record.get("sort_value"))
                continue

            merged_record = deepcopy(current_record)
            merged_record.merge(sub_record, inplace=True)
            merged_records.append(merged_record)
            added.add(current_record.get("sort_value"))
            added.add(sub_record.get("sort_value"))

        temporay_columns = ["side", "sort_value", "has_sort_value", "has_valid_join_key"]
        merged_records.drop_columns(temporay_columns, inplace=True)
        left_records.drop_columns(temporay_columns, inplace=True)
        right_records.drop_columns(temporay_columns, inplace=True)

        return merged_records

    def merge_sequencial_with_copy(
        self,
        source_stamp_key: str,
        source_key: str,
        copy_records: Records,
        copy_stamp_key: str,
        copy_from_key: str,
        copy_to_key: str,
        sink_records: Records,
        sink_stamp_key: str,
        sink_from_key: str,
    ) -> Records:

        source_records = deepcopy(self)
        copy_records = deepcopy(copy_records)
        sink_records = deepcopy(sink_records)

        merged_records: Records = Records()

        for record in source_records.data:
            record.add("type", RecordType.SOURCE)  # type: ignore
            record.add("timestamp", record.get(source_stamp_key))
        for record in copy_records.data:
            record.add("type", RecordType.COPY)  # type: ignore
            record.add("timestamp", record.get(copy_stamp_key))
        for record in sink_records.data:
            record.add("type", RecordType.SINK)  # type: ignore
            record.add("timestamp", record.get(sink_stamp_key))

        records = Records(source_records._data + copy_records._data + sink_records._data)
        records.data.sort(key=lambda x: x.get("timestamp"), reverse=True)
        # Searching for records in chronological order is not good
        # because the lost records stay forever. Sort in reverse chronological order.

        #  List of records to be added by sink and removed by source
        processing_records: List[Record] = []

        def merge_processing_record_keys(processing_record: Record):
            for processing_record_ in filter(
                lambda x: x.get(sink_from_key) & processing_record.get(sink_from_key)
                and x.get(sink_from_key) != processing_record.get(sink_from_key),
                processing_records,
            ):
                processing_record_keys = processing_record.get(sink_from_key)
                coresponding_record_keys = processing_record_.get(sink_from_key)

                merged_set = processing_record_keys | coresponding_record_keys
                processing_record.data[sink_from_key] = merged_set
                processing_record_.data[sink_from_key] = merged_set

        for record in records.data:
            if record.get("type") == RecordType.SINK:
                record._data[sink_from_key] = set([record.get(sink_from_key)])  # type: ignore
                processing_records.append(record)

            elif record.get("type") == RecordType.COPY:
                records_need_to_merge = filter(
                    lambda x: record.get(copy_to_key) in x._data[sink_from_key], processing_records
                )
                for processing_record in records_need_to_merge:
                    processing_record._data[sink_from_key].add(record.get(copy_from_key))
                    merge_processing_record_keys(processing_record)
                    # No need for subsequent loops since we integrated them.
                    break

            elif record.get("type") == RecordType.SOURCE:
                for processing_record in filter(
                    lambda x: record.get(source_key) in x._data[sink_from_key],
                    processing_records[:],
                ):
                    processing_records.remove(processing_record)
                    processing_record.merge(record, inplace=True)
                    merged_records.append(processing_record)

        # Deleting an added key
        merged_records.drop_columns(["type", "timestamp"], inplace=True)

        return merged_records


def merge(
    records: RecordsInterface,
    records_: RecordsInterface,
    join_key: str,
    how: str = "inner",
    record_sort_key: Optional[str] = None,
    record_sort_key_: Optional[str] = None,
) -> Records:
    assert type(records) == type(records_)

    return records.merge(
        records_, join_key, how, record_sort_key, record_sort_key_  # type: ignore
    )


def merge_sequencial(
    left_records: RecordsInterface,
    right_records: RecordsInterface,
    left_stamp_key: str,
    right_stamp_key: str,
    join_key: Optional[str],
    how: str = "inner",
    *,
    left_sort_key: Optional[str] = None,
    right_sort_key: Optional[str] = None,
) -> Records:
    assert type(left_records) == type(right_records)

    return left_records.merge_sequencial(
        right_records,  # type: ignore
        left_stamp_key,
        right_stamp_key,
        join_key,
        how,
        left_sort_key=left_sort_key,
        right_sort_key=right_sort_key,
    )


class RecordType(Enum):
    SOURCE = (0,)
    COPY = (1,)
    SINK = 2


def merge_sequencial_with_copy(
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
):
    assert type(source_records) == type(copy_records) and type(copy_records) == type(sink_records)

    return source_records.merge_sequencial_with_copy(
        source_stamp_key,
        source_key,
        copy_records,  # type: ignore
        copy_stamp_key,
        copy_from_key,
        copy_to_key,
        sink_records,  # type: ignore
        sink_stamp_key,
        sink_from_key,
    )
