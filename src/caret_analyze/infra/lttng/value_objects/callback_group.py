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

from ....value_objects import CallbackGroupValue, ValueObject


class CallbackGroupValueLttng(CallbackGroupValue):
    def __init__(
        self,
        callback_group_type_name: str,
        node_name: str,
        node_id: str,
        callback_ids: tuple[str, ...],
        callback_group_id: str,
        callback_group_addr: int,
        executor_addr: int
    ) -> None:
        super().__init__(
            callback_group_type_name=callback_group_type_name,
            node_name=node_name,
            node_id=node_id,
            callback_ids=callback_ids,
            callback_group_id=callback_group_id)
        self._callback_group_addr = callback_group_addr
        self._executor_addr = executor_addr

    @property
    def callback_group_addr(self) -> int:
        return self._callback_group_addr

    @property
    def executor_addr(self) -> int:
        return self._executor_addr


class CallbackGroupAddr(ValueObject):

    def __init__(self, callback_group_addr: int) -> None:
        self._callback_group_addr = callback_group_addr

    def __str__(self) -> str:
        return str(self._callback_group_addr)

    @property
    def group_id(self) -> str:
        return self.to_id(self._callback_group_addr)

    @staticmethod
    def to_id(callback_group_addr: int) -> str:
        return f'callback_group_{callback_group_addr}'


class CallbackGroupId(ValueObject):

    def __init__(self, callback_group_id: str) -> None:
        self._callback_group_id = callback_group_id

    def __str__(self) -> str:
        return self._callback_group_id

    @property
    def group_addr(self) -> int:
        return self.to_addr(self._callback_group_id)

    @staticmethod
    def to_addr(callback_group_id: str) -> int:
        return int(callback_group_id.replace('callback_group_', ''))
