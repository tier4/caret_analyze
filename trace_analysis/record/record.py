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
import sys


class MergeSideInfo(Enum):
    LEFT = 1
    RIGHT = 2


class Record(collections.UserDict):
    def __init__(self, init: Optional[Dict] = None, timestamp: Optional[float] = None):
        init = init or {}
        super().__init__(init)
        self.columns = set(init.keys())
        self.latest_stamp = timestamp or 0
        self.oldest_stamp = timestamp or sys.float_info.max

    def equals(self, record: Record) -> bool:
        return self.data == record.data

    def merge(self, other: Record) -> None:
        self.data.update(other.data)
        self.columns |= other.columns

        if other.latest_stamp is None or other.oldest_stamp is None:
            return None

        if self.latest_stamp is None or self.oldest_stamp is None:
            self.latest_stamp = other.latest_stamp
            self.oldest_stamp = other.oldest_stamp
        else:
            self.latest_stamp = max(self.latest_stamp, other.latest_stamp)
            self.oldest_stamp = min(self.oldest_stamp, other.oldest_stamp)


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
        df = pd.DataFrame.from_dict(self.data)
        missing_columns = set(self.columns) - set(df.columns)
        df_miss = pd.DataFrame(columns=missing_columns)
        df = pd.concat([df, df_miss])
        return df

    def to_string(self) -> str:
        return self.to_dataframe().to_string()

    def equals(self, records: Records) -> bool:
        for r, r_ in zip(self, records):
            if r.equals(r_) is False:
                return False

        return True


def merge(
    records: Records,
    records_: Records,
    join_key: str,
    how: str = "inner",
    record_sort_key: Optional[str] = None,
) -> Records:
    merged_records = Records()
    assert how in ["inner", "left", "right", "outer"]

    records.sort(key=lambda x: x.latest_stamp)
    records_.sort(key=lambda x: x.latest_stamp)

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

                merged_record = copy.deepcopy(record_)
                merged_record.merge(record)
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

    merged_records.columns = records.columns | records_.columns

    return merged_records


def merge_sequencial(
    left_records: Records,
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

    def is_key_matched(record: Record, record_: Record) -> bool:
        if join_key is None:
            return True
        if join_key not in record.columns or join_key not in record_.columns:
            return False
        return record[join_key] == record_[join_key]

    merged_records: Records = Records()

    for left in left_records:
        left["side"] = MergeSideInfo.LEFT

    for right in right_records:
        right["side"] = MergeSideInfo.RIGHT

    left_sort_key = left_sort_key or left_stamp_key
    right_sort_key = right_sort_key or right_stamp_key

    records = left_records + right_records
    records.sort(key=lambda x: x.latest_stamp)

    added = set()
    for i, current_record in enumerate(records):
        if current_record.latest_stamp in added:
            continue
        if join_key is not None and join_key not in current_record.columns:
            continue

        if current_record["side"] == MergeSideInfo.RIGHT:
            if how in ["right", "outer"]:
                merged_records.append(current_record)
                added.add(current_record.stamp)
            continue

        next_record: Optional[Record] = None
        sub_record: Optional[Record] = None
        if join_key is None:
            for record in records.data[i + 1:]:
                if next_record is not None and sub_record is not None:
                    break
                if record["side"] == MergeSideInfo.LEFT and next_record is None:
                    next_record = record
                if record["side"] == MergeSideInfo.RIGHT and sub_record is None:
                    sub_record = record
        else:
            for record in records.data[i + 1:]:
                if next_record is not None and sub_record is not None:
                    break
                if join_key not in record.columns:
                    continue

                key_matched = record[join_key] == current_record[join_key]
                if key_matched and record["side"] == MergeSideInfo.LEFT and next_record is None:
                    next_record = record
                if key_matched and record["side"] == MergeSideInfo.RIGHT and sub_record is None:
                    sub_record = record

        has_valid_next_record = (
            next_record is not None
            and sub_record is not None
            and next_record.latest_stamp < sub_record.latest_stamp
        )
        if has_valid_next_record or sub_record is None or sub_record.latest_stamp in added:
            if how in ["left", "outer"]:
                merged_records.append(current_record)
                added.add(current_record.latest_stamp)
            continue

        merged_record = copy.deepcopy(current_record)
        merged_record.merge(sub_record)
        merged_records.append(merged_record)
        added.add(current_record.latest_stamp)
        added.add(sub_record.latest_stamp)

    merged_records.drop_columns(["side"], inplace=True)
    left_records.drop_columns(["side"], inplace=True)
    right_records.drop_columns(["side"], inplace=True)

    merged_records.columns = left_records.columns | right_records.columns

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
                processing_record.merge(record)
                merged_records.append(processing_record)

    # Deleting an added key
    for record in merged_records:
        del record["type"]
        del record["timestamp"]

    return merged_records
