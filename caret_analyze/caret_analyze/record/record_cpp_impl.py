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

from typing import List, Optional, Dict, Callable
import pandas as pd

from .record import RecordInterface, RecordsInterface
from record_cpp_impl import RecordBase, RecordsBase


class RecordCppImpl(RecordBase, RecordInterface):
    def merge(  # type: ignore
        self, other: RecordCppImpl, inplace=False
    ) -> Optional[RecordCppImpl]:
        if inplace:
            self._merge(other)
            return None
        else:
            record = RecordCppImpl(self)
            record._merge(other)
            return record

    def drop_columns(self, columns: List[str], inplace: bool = False) -> Optional[RecordCppImpl]:
        if inplace:
            self._drop_columns(columns)
            return None
        else:
            record = RecordCppImpl(self)
            record._drop_columns(columns)
            return record


class RecordsCppImpl(RecordsBase, RecordsInterface):
    def __init__(self, init: Optional[List[RecordCppImpl]] = None):
        super().__init__(init or [])

    def to_string(self) -> str:
        return self.to_dataframe().to_string()

    def export_json(self, path: str) -> None:
        import json

        data_dict = [dic.data for dic in self.data]
        s = json.dumps(data_dict)

        with open(path, mode="w") as f:
            f.write(s)
        return None

    def concat(  # type: ignore
        self, other: RecordsCppImpl, inplace=False
    ) -> Optional[RecordsCppImpl]:
        if inplace:
            self._concat(other)
            return None
        else:
            records = RecordsCppImpl(self)
            records._concat(other)
            return records

    @property
    def data(self) -> List[RecordCppImpl]:  # type: ignore
        return [RecordCppImpl(record_base.data) for record_base in self._data]

    def sort(  # type: ignore
        self, key: str, sub_key: Optional[str] = None, ascending=True, inplace=False
    ) -> Optional[RecordsCppImpl]:
        if inplace:
            self._sort(key, sub_key or "", ascending)
            return None
        else:
            records = RecordsCppImpl(self)
            records._sort(key, sub_key or "", ascending)
            return records

    def clone(self) -> RecordsCppImpl:
        return RecordsCppImpl([RecordCppImpl(record.data) for record in self.data])

    def drop_columns(self, columns: List[str], inplace: bool = False) -> Optional[RecordsCppImpl]:
        if inplace:
            self._drop_columns(columns)
            return None
        else:
            records = RecordsCppImpl(self)
            records._drop_columns(columns)
            return records

    def to_dataframe(self):
        data_dict = [record.data for record in self.data]
        return pd.DataFrame.from_dict(data_dict)

    def rename_columns(
        self, columns: Dict[str, str], inplace: bool = False
    ) -> Optional[RecordsCppImpl]:
        if inplace:
            self._rename_columns(columns)
            return None
        else:
            records = RecordsCppImpl(self)
            records._rename_columns(columns)
            return records

    def filter(
        self, f: Callable[[RecordInterface], bool], inplace: bool = False
    ) -> Optional[RecordsInterface]:
        if inplace:
            self._filter(f)
            return None
        else:
            records = RecordsCppImpl(self)
            records._filter(f)
            return records

    def merge(  # type: ignore
        self,
        right_records: RecordsCppImpl,
        join_key: str,
        how: str = "inner",
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsCppImpl:
        progress_label = progress_label or ""
        assert how in ["inner", "left", "right", "outer"]
        merged_cpp_base = self._merge(right_records, join_key, how, progress_label)
        merged = RecordsCppImpl(merged_cpp_base)
        return merged

    def merge_sequencial(  # type: ignore
        self,
        right_records: RecordsCppImpl,
        left_stamp_key: str,
        right_stamp_key: str,
        join_key: Optional[str],
        how: str = "inner",
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsCppImpl:
        progress_label = progress_label or ""
        merged_cpp_base = self._merge_sequencial(
            right_records, left_stamp_key, right_stamp_key, join_key or "", how, progress_label
        )

        merged = RecordsCppImpl(merged_cpp_base)
        return merged

    def merge_sequencial_for_addr_track(  # type: ignore
        self,
        source_stamp_key: str,
        source_key: str,
        copy_records: RecordsCppImpl,
        copy_stamp_key: str,
        copy_from_key: str,
        copy_to_key: str,
        sink_records: RecordsCppImpl,
        sink_stamp_key: str,
        sink_from_key: str,
        *,
        progress_label: Optional[str] = None,
    ) -> RecordsCppImpl:
        progress_label = progress_label or ""
        merged_cpp_base = self._merge_sequencial_for_addr_track(
            source_stamp_key,
            source_key,
            copy_records,
            copy_stamp_key,
            copy_from_key,
            copy_to_key,
            sink_records,
            sink_stamp_key,
            sink_from_key,
            progress_label,
        )

        merged = RecordsCppImpl(merged_cpp_base)
        return merged
