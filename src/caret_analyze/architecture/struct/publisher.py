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
from ...value_objects import PublisherStructValue


class PublisherStruct():
    """Structured publisher value."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_values: list[CallbackStruct] | None,
        construction_order: int,
    ) -> None:
        self._node_name = node_name
        self._topic_name = topic_name
        self._callbacks = callback_values
        self._construction_order = construction_order

    def __str__(self) -> str:
        msg = ''
        msg += f'node_name: {self.node_name}, '
        msg += f'topic_name: {self.topic_name}, '
        return msg

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callbacks(self) -> list[CallbackStruct] | None:
        return self._callbacks

    @property
    def callback_names(self) -> list[str] | None:
        if self._callbacks is None:
            return None
        return [c.callback_name for c in self._callbacks]

    @property
    def construction_order(self) -> int:
        return self._construction_order

    def to_value(self) -> PublisherStructValue:
        """
        Get publisher struct value.

        Returns
        -------
        PublisherStructValue
            Publisher struct value instance.

        """
        return PublisherStructValue(
            node_name=self.node_name,
            topic_name=self.topic_name,
            callback_values=(
                None if self.callbacks is None
                else tuple(v.to_value() for v in self.callbacks)),
            construction_order=self.construction_order)

    def insert_callback(self, callback: CallbackStruct) -> None:
        """
        Insert callback.

        Parameters
        ----------
        callback : CallbackStruct
            callback struct to insert.

        """
        if self._callbacks is None:
            self._callbacks = [callback]
        elif callback.callback_name not in (self.callback_names or []):
            self._callbacks.append(callback)

    def remove_callback(self, callback: CallbackStruct) -> None:
        """
        Remove callback.

        Parameters
        ----------
        callback : CallbackStruct
            callback struct to remove.

        """
        if self._callbacks and callback in self._callbacks:
            self._callbacks.remove(callback)

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

        if self._callbacks is not None:
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
        if self.topic_name == src:
            self._topic_name = dst

        if self._callbacks is not None:
            for c in self._callbacks:
                c.rename_topic(src, dst)
