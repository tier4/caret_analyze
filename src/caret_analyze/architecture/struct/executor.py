# Copyright 2021 TIER IV, Inc.
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

from .callback import CallbackStruct
from .callback_group import CallbackGroupStruct
from ...common import Util

from ...value_objects import ExecutorStructValue, ExecutorType


class ExecutorStruct():
    """Executor info for architecture."""

    def __init__(
        self,
        executor_type: ExecutorType,
        callback_groups: list[CallbackGroupStruct],
        executor_name: str,
    ) -> None:
        self._executor_type = executor_type
        self._cbg_values: list[CallbackGroupStruct] = callback_groups
        self._executor_name = executor_name

    @property
    def callbacks(self) -> list[CallbackStruct]:
        return list(Util.flatten([cbg.callbacks for cbg in self._cbg_values]))

    @property
    def callback_names(self) -> list[str]:
        return [c.callback_name for c in self.callbacks]

    @property
    def executor_type(self) -> ExecutorType:
        return self._executor_type

    @property
    def executor_type_name(self) -> str:
        return self._executor_type.type_name

    @property
    def executor_name(self) -> str:
        return self._executor_name

    @executor_name.setter
    def executor_name(self, n: str):
        self._executor_name = n

    @property
    def callback_groups(self) -> list[CallbackGroupStruct]:
        return self._cbg_values

    @property
    def callback_group_names(self) -> list[str]:
        cbg_names = [cbg.callback_group_name for cbg in self._cbg_values]
        return cbg_names

    def to_value(self) -> ExecutorStructValue:
        """
        Get executor struct value.

        Returns
        -------
        ExecutorStructValue
            Executor struct value instance.

        """
        return ExecutorStructValue(self.executor_type,
                                   tuple(v.to_value() for v in self.callback_groups),
                                   self.executor_name)

    def rename_node(self, src: str, dst: str) -> None:
        """
        Rename node.

        Parameters
        ----------
        src : str
            Current node name.
        dst : str
            Updated node name.

        """
        for c in self._cbg_values:
            c.rename_node(src, dst)

    def rename_topic(self, src: str, dst: str) -> None:
        """
        Rename topic.

        Parameters
        ----------
        src : str
            Current topic name.
        dst : str
            Updated topic name.

        """
        for c in self._cbg_values:
            c.rename_topic(src, dst)
