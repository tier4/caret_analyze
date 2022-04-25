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

from typing import List

from caret_analyze.common import Summarizable, Summary, Util
from caret_analyze.exceptions import InvalidArgumentError
from caret_analyze.value_objects import ExecutorStructValue, ExecutorType

from .callback import CallbackBase
from .callback_group import CallbackGroup


class Executor(Summarizable):

    def __init__(
        self,
        executor_value: ExecutorStructValue,
        callback_groups: List[CallbackGroup],
    ) -> None:
        self._val = executor_value
        self._cbgs: List[CallbackGroup] = callback_groups

    @property
    def executor_type(self) -> ExecutorType:
        return self._val.executor_type

    @property
    def executor_name(self) -> str:
        return self._val.executor_name

    @property
    def callbacks(self) -> List[CallbackBase]:
        cbs = Util.flatten([cbg.callbacks for cbg in self._cbgs])
        return sorted(cbs, key=lambda x: x.callback_name)

    def get_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackGroup:
        if not isinstance(callback_group_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(x: CallbackGroup):
            return x.callback_group_name == callback_group_name
        return Util.find_one(is_target, self.callback_groups)

    def get_callback(self, callback_name: str) -> CallbackBase:
        if not isinstance(callback_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target_callback(callback: CallbackBase):
            return callback.callback_name == callback_name

        return Util.find_one(is_target_callback, self.callbacks)

    def get_callbacks(self, *callback_names: str) -> List[CallbackBase]:
        callbacks = []
        for callback_name in callback_names:
            callbacks.append(self.get_callback(callback_name))

        return callbacks

    @property
    def callback_names(self) -> List[str]:
        return sorted(c.callback_name for c in self.callbacks)

    @property
    def callback_groups(self) -> List[CallbackGroup]:
        return sorted(self._cbgs, key=lambda x: x.callback_group_name)

    @property
    def callback_group_names(self) -> List[str]:
        return sorted(cbg.callback_group_name for cbg in self.callback_groups)

    @property
    def summary(self) -> Summary:
        return self._val.summary
