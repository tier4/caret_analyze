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

from .callback import CallbackStructValue
from .callback_group import CallbackGroupStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary, Util


class ExecutorType(ValueObject):
    """executor type class."""

    SINGLE_THREADED_EXECUTOR: ExecutorType
    MULTI_THREADED_EXECUTOR: ExecutorType

    def __init__(self, type_name: str) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        type_name : str
            Type name.

        """
        self._type_name = type_name

    @property
    def type_name(self) -> str:
        """
        Get executor type name.

        Returns
        -------
        str
            Type name.

        """
        return self._type_name

    def __str__(self) -> str:
        """
        Convert to string.

        Returns
        -------
        str
            Type name.

        """
        return self.type_name


ExecutorType.SINGLE_THREADED_EXECUTOR = ExecutorType('single_threaded_executor')
ExecutorType.MULTI_THREADED_EXECUTOR = ExecutorType('multi_threaded_executor')


class ExecutorStructValue(ValueObject, Summarizable):
    """Executor info for architecture."""

    def __init__(
        self,
        executor_type: ExecutorType,
        callback_group_values: tuple[CallbackGroupStructValue, ...],
        executor_name: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        executor_type : ExecutorType
            Executor type.
        callback_group_values : tuple[CallbackGroupStructValue, ...]
            Callback group values.
        executor_name : str
            Executor name.

        """
        self._executor_type = executor_type
        self._cbg_values: tuple[CallbackGroupStructValue, ...] = callback_group_values
        self._executor_name = executor_name

    @property
    def callbacks(self) -> tuple[CallbackStructValue, ...]:
        """
        Get Callbacks.

        Returns
        -------
        tuple[CallbackStructValue, ...]
            Callback value base.

        """
        return tuple(Util.flatten([cbg.callbacks for cbg in self._cbg_values]))

    @property
    def callback_names(self) -> tuple[str, ...]:
        """
        Get callback name list.

        Returns
        -------
        tuple[str, ...]
            Callback names.

        """
        return tuple(c.callback_name for c in self.callbacks)

    @property
    def executor_type(self) -> ExecutorType:
        """
        Get executor type.

        Returns
        -------
        ExecutorType
            Executor type.

        """
        return self._executor_type

    @property
    def executor_type_name(self) -> str:
        """
        Get executor type name.

        Returns
        -------
        str
            Executor type name.

        """
        return self._executor_type.type_name

    @property
    def executor_name(self) -> str:
        """
        Get executor name.

        Returns
        -------
        str
            Executor name.

        """
        return self._executor_name

    @property
    def callback_groups(self) -> tuple[CallbackGroupStructValue, ...]:
        """
        Get callback groups.

        Returns
        -------
        tuple[CallbackGroupStructValue, ...]
            Construct callback group value object.

        """
        return self._cbg_values

    @property
    def callback_group_names(self) -> tuple[str, ...]:
        """
        Get callback group names.

        Returns
        -------
        tuple[str, ...]
            Callback group names.

        """
        cbg_names = [cbg.callback_group_name for cbg in self._cbg_values]
        return tuple(cbg_names)

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
        return Summary({
            'name': self.executor_name,
            'type': self.executor_type_name,
            'callback_groups': [_.summary for _ in self.callback_groups]
        })


class ExecutorValue(ValueObject):
    """Executor value class."""

    def __init__(
        self,
        executor_type_name: str,
        callback_group_ids: tuple[str, ...],
        *,
        executor_name: str | None = None
    ) -> None:
        """
        Get executor type name.

        Parameters
        ----------
        executor_type_name : str
            Executor type name.
        callback_group_ids : tuple[str, ...]
            Callback group ids.
        executor_name: str | None = None
            Executor name.

        """
        self._executor_type = ExecutorType(executor_type_name)
        self._cbg_ids = callback_group_ids
        self._executor_name = executor_name

    @property
    def executor_type(self) -> ExecutorType:
        """
        Get executor type.

        Returns
        -------
        ExecutorType
            Executor type.

        """
        return self._executor_type

    @property
    def callback_group_ids(self) -> tuple[str, ...]:
        """
        Get callback group ids.

        Returns
        -------
        tuple[str, ...]
            Callback group id list.

        """
        return self._cbg_ids

    @property
    def executor_name(self) -> str | None:
        """
        Get executor name.

        Returns
        -------
        str | None
            Executor name.

        """
        return self._executor_name
