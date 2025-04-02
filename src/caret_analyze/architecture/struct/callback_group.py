
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

from ...value_objects import CallbackGroupStructValue, CallbackGroupType


class CallbackGroupStruct():
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type: CallbackGroupType,
        node_name: str,
        callbacks: list[CallbackStruct],
        callback_group_name: str
    ) -> None:
        """
        Construct callback group value object.

        Parameters
        ----------
        callback_group_type : CallbackGroupType
            callback group type
        node_name: str
            node name
        callbacks: list[CallbackStruct, ...]
            callbacks
        callback_group_name: str
            callback group name

        """
        self._callback_group_type = callback_group_type
        self._node_name = node_name
        self._callbacks = callbacks
        self._callback_group_name = callback_group_name

    @property
    def callback_group_type(self) -> CallbackGroupType:
        """
        Get callback group type.

        Returns
        -------
        CallbackGroupType
            Callback group type.

        """
        return self._callback_group_type

    @property
    def callback_group_type_name(self) -> str:
        """
        Get callback group type name.

        Returns
        -------
        str
            Callback group type name.

        """
        return self._callback_group_type.type_name

    @property
    def callback_group_name(self) -> str:
        """
        Get callback group name.

        Returns
        -------
        str
            Callback group name.

        """
        return self._callback_group_name

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node_name

    @property
    def callbacks(self) -> list[CallbackStruct]:
        return self._callbacks

    @property
    def callback_names(self) -> list[str]:
        return [i.callback_name for i in self._callbacks]

    def to_value(self) -> CallbackGroupStructValue:
        """
        Get callback group struct value.

        Returns
        -------
        CallbackGroupStructValue
            Callback group struct value instance.

        """
        return CallbackGroupStructValue(self.callback_group_type, self.node_name,
                                        tuple(v.to_value() for v in self.callbacks),
                                        self.callback_group_name)

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
        if self.node_name == src:
            self._node_name = dst

        for c in self._callbacks:
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
        for c in self._callbacks:
            c.rename_topic(src, dst)
