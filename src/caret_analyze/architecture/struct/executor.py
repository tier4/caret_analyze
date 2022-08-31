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

from typing import Optional, Tuple

from .callback import CallbackStruct
from .callback_group import CallbackGroupStruct
from ...common import Summarizable, Summary, Util

from ...value_objects import ExecutorType, ExecutorStructValue


class ExecutorStruct(Summarizable):
    """Executor info for architecture."""

    def __init__(
        self,
        executor_type: ExecutorType,
        callback_groups: Tuple[CallbackGroupStruct, ...],
        executor_name: str,
    ) -> None:
        self._executor_type = executor_type
        self._cbg_values: Tuple[CallbackGroupStruct, ...] = callback_groups
        self._executor_name = executor_name

    @property
    def callbacks(self) -> Tuple[CallbackStruct, ...]:
        return tuple(Util.flatten([cbg.callbacks for cbg in self._cbg_values]))

    @property
    def callback_names(self) -> Tuple[str, ...]:
        return tuple(c.callback_name for c in self.callbacks)

    @property
    def executor_type(self) -> ExecutorType:
        return self._executor_type

    @property
    def executor_type_name(self) -> str:
        return self._executor_type.type_name

    @property
    def executor_name(self) -> str:
        return self._executor_name

    @property
    def callback_groups(self) -> Tuple[CallbackGroupStruct, ...]:
        return self._cbg_values

    @property
    def callback_group_names(self) -> Tuple[str, ...]:
        cbg_names = [cbg.callback_group_name for cbg in self._cbg_values]
        return tuple(cbg_names)

    @property
    def summary(self) -> Summary:
        return Summary({
            'name': self.executor_name,
            'type': self.executor_type_name,
            'callback_groups': [_.summary for _ in self.callback_groups]
        })

    def to_value(self) -> ExecutorStructValue:
        return ExecutorStructValue(self.executor_type, tuple(v.to_value() for v in self.callback_groups), self.executor_name)
