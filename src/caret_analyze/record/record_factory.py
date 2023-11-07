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

from collections.abc import Sequence

from multimethod import multimethod as singledispatchmethod

from .column import ColumnValue
from .record import Record, RecordInterface, Records, RecordsInterface

try:
    import caret_analyze.record.record_cpp_impl as cpp_impl

    use_cpp_impl = True
    print('Succeed to find record_cpp_impl. the C++ version will be used.')
except ModuleNotFoundError:
    use_cpp_impl = False
    print('Failed to find record_cpp_impl. the Python version will be used.')


class RecordFactory:

    @classmethod
    def is_cpp_impl_valid(cls) -> bool:
        return use_cpp_impl

    @classmethod
    def create_instance(cls, init: dict | None = None) -> RecordInterface:
        if use_cpp_impl:
            return cls._create_cpp_instance(init)
        else:
            return Record(init)

    @classmethod
    def _create_cpp_instance(cls, init: dict | None = None) -> RecordInterface:
        if init is None:
            return cpp_impl.RecordCppImpl()
        else:
            return cpp_impl.RecordCppImpl(init)


class RecordsFactory:

    @staticmethod
    def is_cpp_impl_valid() -> bool:
        return use_cpp_impl

    @singledispatchmethod
    def create_instance(args) -> RecordsInterface:
        raise NotImplementedError('Not implemented arguments type')

    @staticmethod
    @create_instance.register
    def _create_instance() -> RecordsInterface:
        return RecordsFactory._create_instance_record([], [])

    @staticmethod
    @create_instance.register
    def _create_instance_record(
        init: Sequence[RecordInterface],
        columns: Sequence[ColumnValue] | None
    ) -> RecordsInterface:
        if use_cpp_impl:
            return RecordsFactory._create_cpp_instance(init, columns)
        else:
            return Records(init, columns)

    @staticmethod
    @create_instance.register
    def _create_instance_dict(
        init: Sequence[dict[str, int]] | None = None,
        columns: Sequence[ColumnValue] | None = None
    ) -> RecordsInterface:
        records: Sequence[RecordInterface] = [
            RecordFactory.create_instance(record)
            for record
            in init or []
        ]

        if use_cpp_impl:
            return RecordsFactory._create_cpp_instance(records, columns)
        else:
            return Records(records, columns)

    @staticmethod
    def _create_cpp_instance(
        init: Sequence[RecordInterface] | None = None,
        columns: Sequence[ColumnValue] | None = None,
    ) -> RecordsInterface:
        return cpp_impl.RecordsCppImpl(init or [], columns or [])
