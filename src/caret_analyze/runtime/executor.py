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
    """Class that represents executor."""

    def __init__(
        self,
        executor_value: ExecutorStructValue,
        callback_groups: List[CallbackGroup],
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        executor_value : ExecutorStructValue
            Static info.
        callback_groups : List[CallbackGroup]
            Callback groups added to the executor.

        """
        self._val = executor_value
        self._callback_groups: List[CallbackGroup] = callback_groups

    @property
    def executor_type(self) -> ExecutorType:
        """
        Get executor type.

        Returns
        -------
        ExecutorType
            executor type.

        """
        return self._val.executor_type

    @property
    def executor_name(self) -> str:
        """
        Get executor name.

        Returns
        -------
        str
            executor name defined in the architecture.

        """
        return self._val.executor_name

    @property
    def callbacks(self) -> List[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        List[CallbackBase]
            Callbacks added to the executor.

        """
        cbs = Util.flatten([cbg.callbacks for cbg in self._callback_groups])
        return sorted(cbs, key=lambda x: x.callback_name)

    def get_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackGroup:
        """
        Get callback group.

        Parameters
        ----------
        callback_group_name : str
            callback group name to get.

        Returns
        -------
        CallbackGroup
            Callback group that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(callback_group_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(x: CallbackGroup):
            return x.callback_group_name == callback_group_name
        return Util.find_one(is_target, self.callback_groups)

    def get_callback(self, callback_name: str) -> CallbackBase:
        """
        Get callback.

        Parameters
        ----------
        callback_name : str
            callback name to get.

        Returns
        -------
        CallbackBase
            callback that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(callback_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target_callback(callback: CallbackBase):
            return callback.callback_name == callback_name

        return Util.find_one(is_target_callback, self.callbacks)

    def get_callbacks(self, *callback_names: str) -> List[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        List[CallbackBase]
            callbacks that match the condition.

        """
        callbacks = []
        for callback_name in callback_names:
            callbacks.append(self.get_callback(callback_name))

        return callbacks

    @property
    def callback_names(self) -> List[str]:
        """
        Get callback names.

        Returns
        -------
        List[str]
            callback names added to the executor.

        """
        return sorted(c.callback_name for c in self.callbacks)

    @property
    def callback_groups(self) -> List[CallbackGroup]:
        """
        Get callback groups.

        Returns
        -------
        List[CallbackGroup]
            Callback groups added to the executor.

        """
        return sorted(self._callback_groups, key=lambda x: x.callback_group_name)

    @property
    def callback_group_names(self) -> List[str]:
        """
        Get callback group names.

        Returns
        -------
        List[str]
            Callback group names added to the executor.

        """
        return sorted(cbg.callback_group_name for cbg in self.callback_groups)

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._val.summary
