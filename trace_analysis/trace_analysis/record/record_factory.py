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


from typing import Optional, Dict, List

from trace_analysis.record.record import Record, Records, RecordInterface, RecordsInterface
from trace_analysis.record.record_cpp_impl import RecordCppImpl, RecordsCppImpl


use_cpp_impl = True


class RecordFactory:
    @classmethod
    def create_instance(self, init: Optional[Dict] = None) -> RecordInterface:
        if use_cpp_impl:
            if init is None:
                return RecordCppImpl()
            else:
                return RecordCppImpl(init)
        else:
            return Record(init)


class RecordsFactory:
    @classmethod
    def create_instance(cls, init: Optional[List[RecordInterface]] = None) -> RecordsInterface:
        if use_cpp_impl:
            return RecordsCppImpl(init or [])  # type: ignore
        else:
            return Records(init)  # type: ignore
