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

from typing import Tuple

from ....value_objects import ExecutorValue


class ExecutorValueLttng(ExecutorValue):
    def __init__(

        self,
        pid: int,
        executor_id: str,
        executor_type_name: str,
        callback_group_ids: Tuple[str, ...],
    ) -> None:
        super().__init__(
            executor_id,
            executor_type_name,
            callback_group_ids,
            executor_name=None)
        self._pid = pid

    @property
    def pid(self) -> int:
        return self._pid
