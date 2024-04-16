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


from .callback import CallbackBase
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError
from ..value_objects import (CallbackGroupStructValue,
                             CallbackGroupType)


class CallbackGroup(Summarizable):
    """A class that represents the callback group."""

    def __init__(
        self,
        callback_group_info: CallbackGroupStructValue,
        callbacks: list[CallbackBase],
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_group_info : CallbackGroupStructValue
            static information.
        callbacks : list[CallbackBase]
            callbacks to be added to the callback group.

        """
        self._val = callback_group_info
        self._callbacks: list[CallbackBase] = callbacks

    @property
    def callback_group_type(self) -> CallbackGroupType:
        """
        Get callback_group_type.

        Returns
        -------
        CallbackGroupType : CallbackGroupType
            REENTRANT / MUTUALLY_EXCLUSIVE

        """
        return self._val.callback_group_type

    @property
    def callback_group_type_name(self) -> str:
        """
        Get callback_group_type name.

        Returns
        -------
        CallbackGroupType name: str
            'reentrant' / 'mutually_exclusive'


        """
        return self._val.callback_group_type_name

    @property
    def callback_group_name(self) -> str:
        """
        Get callback group name.

        Returns
        -------
        str
            callback group name defined in the architecture.

        """
        return self._val.callback_group_name

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name which is contained this callback group.

        """
        return self._val.node_name

    @property
    def callbacks(self) -> list[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        list[CallbackBase]
            callbacks which are contained in this callback group.

        """
        return sorted(self._callbacks, key=lambda x: x.callback_name)

    @property
    def value(self) -> CallbackGroupStructValue:
        """
        Get StructValue object.

        Returns
        -------
        CallbackGroupStructValue
            callback group value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._val

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

    def get_callback(self, callback_name: str) -> CallbackBase:
        """
        Get a callback that matches the condition.

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

        """
        if not isinstance(callback_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        return Util.find_one(
            lambda x: x.callback_name == callback_name,
            self._callbacks
        )

    def get_callbacks(self, *callback_names: str) -> list[CallbackBase]:
        """
        Get callbacks that match the condition.

        Parameters
        ----------
        *callback_names : str
            callback names to get.

        Returns
        -------
        list[CallbackBase]
            callbacks that match the condition.

        """
        callbacks = []
        for callback_name in callback_names:
            callbacks.append(self.get_callback(callback_name))

        return callbacks
