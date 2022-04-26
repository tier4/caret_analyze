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

from logging import getLogger
from typing import Dict, Iterable, Iterator, Optional, Tuple

from .callback_group import CallbackGroupsStruct
from .node import NodesStruct
from ..reader_interface import ArchitectureReader
from ...common import Util
from ...exceptions import (Error)
from ...value_objects import (
    ExecutorStructValue,
    ExecutorType,
)

logger = getLogger(__name__)


class ExecutorStruct:
    def __init__(
        self,
        executor_id: str,
        executor_name: str,
        executor_type: ExecutorType
    ) -> None:
        self._cbgs: Optional[CallbackGroupsStruct] = None
        self._executor_id = executor_id
        self._executor_name = executor_name
        self._executor_type = executor_type

    @property
    def callback_groups(self) -> CallbackGroupsStruct:
        assert self._cbgs is not None
        return self._cbgs

    @callback_groups.setter
    def callback_groups(self, callback_groups: CallbackGroupsStruct) -> None:
        self._cbgs = callback_groups

    @property
    def executor_id(self) -> str:
        return self._executor_id

    @property
    def executor_name(self) -> str:
        return self._executor_name

    @property
    def executor_type(self) -> ExecutorType:
        return self._executor_type

    def to_value(self) -> ExecutorStructValue:
        return ExecutorStructValue(
            executor_type=self.executor_type,
            callback_groups=self.callback_groups.to_value(),
            executor_name=self.executor_name,
            executor_id=self.executor_id
        )


class ExecutorsStruct(Iterable):

    def __init__(
        self,
    ) -> None:
        self._data: Dict[str, ExecutorStruct] = {}

    def add(self, executor: ExecutorStruct) -> None:
        self._data[executor.executor_id] = executor

    @staticmethod
    def create_from_reader(
        reader: ArchitectureReader,
        nodes: NodesStruct,
    ) -> ExecutorsStruct:
        executors = ExecutorsStruct()

        exec_vals = reader.get_executors()

        for i, executor_value in enumerate(exec_vals):
            try:
                executor_name = executor_value.executor_name or Util.indexed_name('executor', i)
                executor_id = Util.indexed_name('executor', i)
                executor = ExecutorStruct(executor_id, executor_name, executor_value.executor_type)
                executor.callback_groups = nodes.callback_groups.get_cbgs(
                    *executor_value.callback_group_ids)
                executors.add(executor)
            except Error as e:
                logger.warning(
                    'Failed to load executor. skip loading. '
                    f'executor_name = {executor_name}. {e}')

        return executors

    def __iter__(self) -> Iterator[ExecutorStruct]:
        return iter(self._data.values())

    def to_value(self) -> Tuple[ExecutorStructValue, ...]:
        return tuple(_.to_value() for _ in self)
