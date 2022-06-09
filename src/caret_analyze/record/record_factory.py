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


from typing import Dict, List, Optional

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
    def create_instance(cls, init: Optional[Dict] = None) -> RecordInterface:
        if use_cpp_impl:
            return cls._create_cpp_instance(init)
        else:
            return Record(init)

    @classmethod
    def _create_cpp_instance(cls, init: Optional[Dict] = None) -> RecordInterface:
        if init is None:
            return cpp_impl.RecordCppImpl()
        else:
            return cpp_impl.RecordCppImpl(init)


class RecordsFactory:

    @staticmethod
    def is_cpp_impl_valid() -> bool:
        return use_cpp_impl

    @staticmethod
    def create_instance(
        init: Optional[List[RecordInterface]] = None,
        columns: Optional[List[str]] = None
    ) -> RecordsInterface:
        if use_cpp_impl:
            return RecordsFactory._create_cpp_instance(init, columns)
        else:
            return Records(init, columns)

    @staticmethod
    def _create_cpp_instance(
        init: Optional[List[RecordInterface]] = None,
        columns: Optional[List[str]] = None,
    ) -> RecordsInterface:
        return cpp_impl.RecordsCppImpl(init or [], columns or [])
