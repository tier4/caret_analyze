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
from typing import (
    List,
    Optional,
    Callable,
    Dict,
)
from enum import Enum
import pandas as pd


class Record(collections.UserDict):
    def equals(self, record: Record) -> bool:
        return self.data == record.data


class Records(collections.UserList):
    def iterrows(self) -> RecordsIterator:
        return RecordsIterator(self)

    def drop_columns(
        self,
        columns: List[str] = [],
        inplace: bool = False
    ) -> Optional[Records]:

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

        if not inplace:
            return Records(data)
        else:
            return None

    def rename_columns(
        self,
        columns: Dict[str, str],
        inplace: bool = False
    ) -> Optional[Records]:

        def change_dict_key(d, old_key, new_key):
            d[new_key] = d.pop(old_key, None)

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

    def filter(
        self,
        f: Callable[[Record], bool],
        inplace: bool = False
    ) -> Optional[Records]:
        data = []
        for record in self.data:
            if f(record):
                data.append(record)
        if not inplace:
            return Records(copy.deepcopy(data))
        else:
            self.data = data
            return None

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.data)

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

    def forward_while(
        self,
        condition: Callable[[Record], bool]
    ) -> None:
        while True:
            value = self.current_value()
            if value is not None and condition(value):
                next(self)
                continue
            return

    def find_item(
        self,
        condition: Callable[[Record], bool],
        item_index: int = 0,
        inplace_iter_count: bool = False
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
    join_key: str
) -> Records:
    merged_records = Records()

    records.sort(key=lambda x: x[join_key], reverse=True)
    records_.sort(key=lambda x: x[join_key], reverse=True)

    for record in records:
        for record_ in records_:
            if record_[join_key] != record[join_key]:
                continue
            merged_record = Record(record_)
            merged_record.update(record)
            merged_records.append(merged_record)

    return merged_records


def merge_sequencial(
    records: Records,
    sub_records: Records,
    record_stamp_key: str,
    sub_record_stamp_key: str,
    join_key: str
) -> Records:

    # assert isinstance(records, Records)
    # assert isinstance(sub_records, Records)

    def is_next_sub_record_valid(
        record: Record,
        sub_records_it: RecordsIterator,
        join_key: str
    ) -> bool:
        # iterator count を進めずに、条件に合う2番目ののsub_recordを取得する。
        next_sub_record = sub_records_it.find_item(
            lambda sub_record: record[join_key] == sub_record[join_key],
            item_index=1)

        if next_sub_record is None:
            return False

        # 2番目のsub_recordでもタイミング的にwrite -> read が成り立っている場合
        # write -> read が成り立っている場合 ：sub_record の方が時間が大きい場合。
        record_stamp = record[record_stamp_key]
        next_sub_record_stamp = next_sub_record[sub_record_stamp_key]
        if record_stamp < next_sub_record_stamp:
            return True
        return False

    def is_invalid(record: Record, sub_record: Record) -> bool:
        return record[record_stamp_key] > sub_record[sub_record_stamp_key]

    def is_key_matched(record: Record, sub_record: Record) -> bool:
        return record[join_key] == sub_record[join_key]

    merged_records: Records = Records()

    records.sort(key=lambda x: x[record_stamp_key], reverse=True)
    sub_records.sort(key=lambda x: x[sub_record_stamp_key], reverse=True)

    # 最初のレコード分進める。
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
                # 以降、必要のないrecordの確認スキップさせるためにitertor countを進める。
                records_it.forward_while(
                    lambda record: is_invalid(record, sub_record))

                # iterator count を進めずに、条件に合う最初のrecordを取得する。
                record = records_it.find_item(
                    lambda record: is_key_matched(record, sub_record))

                if record is None:
                    continue

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

    records = Records(source_records + copy_records + sink_records)
    records.sort(key=lambda x: x["timestamp"], reverse=True)
    # 時系列に行くと、ロストとかがずっと残って良くない。

    processing_records: List[Record] = []  # sinkで追加され、sourceで削除されるrecordのリスト

    def merge_processing_record_keys(processing_record: Record):
        for processing_record_ in filter(
            lambda x: x[sink_from_key] & processing_record[sink_from_key]
            and x[sink_from_key] != processing_record[sink_from_key],
            processing_records,
        ):
            processing_record_keys = processing_record[sink_from_key]
            coresponding_record_keys = processing_record_[sink_from_key]

            merged_set = (processing_record_keys | coresponding_record_keys)
            processing_record[sink_from_key] = merged_set
            processing_record_[sink_from_key] = merged_set

    for record in records:
        if record["type"] == RecordType.SINK:
            record[sink_from_key] = set([record[sink_from_key]])
            processing_records.append(record)

        elif record["type"] == RecordType.COPY:
            for processing_record in filter(
                    lambda x: record[copy_to_key] in x[sink_from_key],
                    processing_records):
                processing_record[sink_from_key].add(record[copy_from_key])
                merge_processing_record_keys(processing_record)
                # 統合したので、以降のループは不要。
                break

        elif record["type"] == RecordType.SOURCE:
            for processing_record in filter(
                    lambda x: record[source_key] in x[sink_from_key],
                    processing_records[:]):
                processing_records.remove(processing_record)
                processing_record.update(record)
                merged_records.append(processing_record)

    # 追加したキーの削除
    for record in merged_records:
        del record["type"]
        del record["timestamp"]

    return merged_records
