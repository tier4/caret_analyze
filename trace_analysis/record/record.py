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

import collections
import copy
from typing import List, Optional, Callable, Dict, Set
from enum import Enum
import pandas as pd


class Record(collections.UserDict):
    def equals(self, record: Record) -> bool:
        return self.data == record.data


class Records(collections.UserList):
    def __init__(self, init: List[Record] = None):
        self.columns: Set[str] = set()
        for record in init or []:
            self.columns |= record.keys()
        super().__init__(init)

    def append(self, other: Record):
        super().append(other)
        self.columns |= other.keys()
        pass

    def __add__(self, other) -> Records:
        if isinstance(other, self.__class__):
            records = Records()
            records.data = copy.deepcopy(self.data)
            records.columns = copy.deepcopy(self.columns)

            for record in other:
                records.append(record)
            return records
        else:
            raise NotImplementedError()

    def __iadd__(self, other):
        if isinstance(other, self.__class__):
            for record in other:
                self.append(record)
            return self
        else:
            raise NotImplementedError()

    def iterrows(self) -> RecordsIterator:
        return RecordsIterator(self)

    def drop_columns(self, columns: List[str] = [], inplace: bool = False) -> Optional[Records]:

        data: List[Record]

        if inplace:
            data = self.data
        else:
            data = copy.deepcopy(self).data

        for record in data:
            for column in columns:
                if column not in record.keys():
                    continue
                del record[column]

        self.columns -= set(columns)
        if not inplace:
            return Records(data)
        else:
            return None

    def rename_columns(self, columns: Dict[str, str], inplace: bool = False) -> Optional[Records]:
        def change_dict_key(d: Record, old_key: str, new_key: str):
            d[new_key] = d.pop(old_key, None)

        self.columns -= set(columns.keys())
        self.columns |= set(columns.values())

        data: List[Record]
        if inplace:
            data = self.data
        else:
            data = copy.deepcopy(self).data

        for record in data:
            for key_from, key_to in columns.items():
                change_dict_key(record, key_from, key_to)

        if not inplace:
            return Records(data)
        else:
            return None

    def filter(self, f: Callable[[Record], bool], inplace: bool = False) -> Optional[Records]:
        records = Records()
        init_columns = self.columns
        for record in self:  # type: Record
            if f(record):
                records.append(record)

        if not inplace:
            records.columns = init_columns
            records.data = copy.deepcopy(records.data)
            return records
        else:
            self.data = records.data
            return None

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.data)

    def to_string(self) -> str:
        return self.to_dataframe().to_string()

    def equals(self, records: Records) -> bool:
        for r, r_ in zip(self, records):
            if r.equals(r_) is False:
                return False

        return True


class RecordsIterator:
    def __init__(self, records):
        self._i = 0
        self._i_next = 0
        self._records = records

    def __iter__(self) -> RecordsIterator:
        return self

    def __next__(self) -> Record:
        self._i = self._i_next
        self._i_next += 1
        self._i_next = min(self._i_next, len(self._records))
        if not self._has_current_value():
            raise StopIteration()
        return self.current_value()  # type: ignore

    def copy(self) -> RecordsIterator:
        it = RecordsIterator(self._records)
        it._i = self._i
        it._i_next = self._i_next
        return it

    def _has_current_value(self) -> bool:
        return self._i < len(self._records)

    def _has_next_value(self) -> bool:
        return self._i_next < len(self._records)

    def next_value(self) -> Optional[Record]:
        has_next_value = self._has_next_value()
        if not has_next_value:
            return None
        return self._records[self._i_next]

    def current_value(self) -> Optional[Record]:
        if not self._has_current_value():
            return None
        return self._records[self._i]

    def forward_while(self, condition: Callable[[Record], bool]) -> Records:
        befores = Records()
        while True:
            value = self.current_value()
            if value is not None and condition(value):
                befores.append(value)
                next(self)
                continue
            return befores

    def find_item(
        self,
        condition: Callable[[Record], bool],
        item_index: int = 0,
        inplace_iter_count: bool = False,
    ) -> Optional[Record]:
        assert item_index >= 0

        if inplace_iter_count:
            it = self
        else:
            it = self.copy()
        i = 0

        while True:
            record = it.current_value()
            if record is not None and condition(record):
                if i == item_index:
                    if inplace_iter_count:
                        return None
                    else:
                        return record
                i += 1
            try:
                next(it)
            except StopIteration:
                return None


def merge(
    records: Records,
    records_: Records,
    join_key: str,
    how: str = "inner",
    record_sort_key: Optional[str] = None,
) -> Records:
    merged_records = Records()
    assert how in ["inner", "left", "right", "outer"]

    record_sort_key = record_sort_key or join_key
    records.sort(key=lambda x: x[record_sort_key], reverse=True)

    records_.sort(key=lambda x: x[join_key], reverse=True)

    records_inseted: List[Record] = []

    for record in records:
        if join_key not in record.keys():
            if how in ["left", "outer"]:
                merged_records.append(record)
            continue

        record_inserted = False
        for record_ in records_:
            if join_key not in record_.keys():
                continue
            if record_[join_key] == record[join_key]:
                records_inseted.append(record_)

                merged_record = Record(record_)
                merged_record.update(record)
                merged_records.append(merged_record)
                record_inserted = True
                break
        if record_inserted:
            continue

        if how in ["left", "outer"]:
            merged_records.append(record)

    if how in ["right", "outer"]:
        for record_ in records_:
            if record_ not in records_inseted:
                merged_records.append(record_)

    return merged_records


def merge_sequencial(
    records: Records,
    sub_records: Records,
    record_stamp_key: str,
    sub_record_stamp_key: str,
    join_key: Optional[str],
    remove_dropped=False,
    record_sort_key: Optional[str] = None,
) -> Records:
    def is_key_matched(record: Record, sub_record: Record) -> bool:
        if join_key is None:
            return True
        if join_key not in record.keys() or join_key not in sub_record.keys():
            return False
        return record[join_key] == sub_record[join_key]

    def is_next_sub_record_valid(
        record: Record, sub_records_it: RecordsIterator, join_key: Optional[str]
    ) -> bool:
        # Get the second sub_record that matches the condition without forwarding the iterator.
        if join_key is None:
            next_sub_record = sub_records_it.find_item(lambda _: True, item_index=1)
        else:
            next_sub_record = sub_records_it.find_item(
                lambda sub_record: is_key_matched(record, sub_record), item_index=1
            )

        if next_sub_record is None:
            return False

        # If write -> read order is also true for the second sub_record,
        # ignore current sub_record.
        record_stamp = record.get(record_stamp_key, 0)
        next_sub_record_stamp = next_sub_record.get(sub_record_stamp_key, 0)
        if record_stamp < next_sub_record_stamp:
            return True
        return False

    def is_invalid(record: Record, sub_record: Record) -> bool:

        if (
            record_stamp_key not in record.keys()
            or sub_record_stamp_key not in sub_record.keys()
            or record[record_stamp_key] is None
            or sub_record[sub_record_stamp_key] is None
        ):
            return True
        return record[record_stamp_key] > sub_record[sub_record_stamp_key]

    merged_records: Records = Records()

    record_sort_key = record_sort_key or record_stamp_key
    records.sort(key=lambda x: x[record_sort_key], reverse=True)
    sub_records.sort(key=lambda x: x[sub_record_stamp_key], reverse=True)

    # Proceed for the first record.
    try:
        records_it: RecordsIterator = records.iterrows()
        sub_records_it: RecordsIterator = sub_records.iterrows()
        next(records_it)
    except StopIteration:
        return merged_records

    recorded_stamps = set()

    while True:
        try:
            sub_record: Record = next(sub_records_it)
            try:
                # From now on, proceed with itertor
                # in order to skip the confirmation of unneeded records.
                passed_records = records_it.forward_while(
                    lambda record: is_invalid(record, sub_record)
                )
                if remove_dropped is False:
                    for passed in passed_records:
                        is_recorded = (
                            record_stamp_key in passed.keys()
                            and passed[record_stamp_key] in recorded_stamps
                        )
                        if is_recorded or not is_key_matched(passed, sub_record):
                            continue
                        merged_records.append(passed)

                # Get the first record that matches the condition without fowarding the iterator.
                record = records_it.find_item(lambda record: is_key_matched(record, sub_record))

                if record is None:
                    continue

                # The case where the next (chronologically earlier) data
                # is sending the same data earlier.
                if is_next_sub_record_valid(record, sub_records_it, join_key):
                    continue

                if record[record_stamp_key] not in recorded_stamps:
                    recorded_stamps.add(record[record_stamp_key])
                    merged_record = Record(record)
                    merged_record.update(sub_record)
                    merged_records.append(merged_record)
            except StopIteration:
                pass
        except StopIteration:
            return merged_records


class RecordType(Enum):
    SOURCE = (0,)
    COPY = (1,)
    SINK = 2


def merge_sequencial_with_copy(
    source_records: Records,
    source_stamp_key: str,
    source_key: str,
    copy_records: Records,
    copy_stamp_key: str,
    copy_from_key: str,
    copy_to_key: str,
    sink_records: Records,
    sink_stamp_key: str,
    sink_from_key: str,
):

    source_records = copy.deepcopy(source_records)
    copy_records = copy.deepcopy(copy_records)
    sink_records = copy.deepcopy(sink_records)

    merged_records: Records = Records()

    for record in source_records:
        record["type"] = RecordType.SOURCE
        record["timestamp"] = record[source_stamp_key]
    for record in copy_records:
        record["type"] = RecordType.COPY
        record["timestamp"] = record[copy_stamp_key]
    for record in sink_records:
        record["type"] = RecordType.SINK
        record["timestamp"] = record[sink_stamp_key]

    records = Records(source_records.data + copy_records.data + sink_records.data)
    records.sort(key=lambda x: x["timestamp"], reverse=True)
    # Searching for records in chronological order is not good
    # because the lost records stay forever. Sort in reverse chronological order.

    #  List of records to be added by sink and removed by source
    processing_records: List[Record] = []

    def merge_processing_record_keys(processing_record: Record):
        for processing_record_ in filter(
            lambda x: x[sink_from_key] & processing_record[sink_from_key]
            and x[sink_from_key] != processing_record[sink_from_key],
            processing_records,
        ):
            processing_record_keys = processing_record[sink_from_key]
            coresponding_record_keys = processing_record_[sink_from_key]

            merged_set = processing_record_keys | coresponding_record_keys
            processing_record[sink_from_key] = merged_set
            processing_record_[sink_from_key] = merged_set

    for record in records:
        if record["type"] == RecordType.SINK:
            record[sink_from_key] = set([record[sink_from_key]])
            processing_records.append(record)

        elif record["type"] == RecordType.COPY:
            for processing_record in filter(
                lambda x: record[copy_to_key] in x[sink_from_key], processing_records
            ):
                processing_record[sink_from_key].add(record[copy_from_key])
                merge_processing_record_keys(processing_record)
                # No need for subsequent loops since we integrated them.
                break

        elif record["type"] == RecordType.SOURCE:
            for processing_record in filter(
                lambda x: record[source_key] in x[sink_from_key], processing_records[:]
            ):
                processing_records.remove(processing_record)
                processing_record.update(record)
                merged_records.append(processing_record)

    # Deleting an added key
    for record in merged_records:
        del record["type"]
        del record["timestamp"]

    return merged_records
