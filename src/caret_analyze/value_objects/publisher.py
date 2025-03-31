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
from .value_object import ValueObject
from ..common import Summarizable, Summary


class PublisherValue(ValueObject):
    """Publisher value."""

    def __init__(
        self,
        topic_name: str,
        node_name: str,
        node_id: str,
        callback_ids: tuple[str, ...] | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        topic_name : str
            Topic name.
        node_name : str
            Node name.
        node_id : str
            Node id.
        callback_ids : tuple[str, ...] | None
            Callback id.
        construction_order : int
            Construction order.

        """
        self._node_id = node_id
        self._node_name = node_name
        self._topic_name = topic_name
        self._callback_ids = callback_ids
        self._construction_order = construction_order

    @property
    def node_id(self) -> str:
        """
        Get node id.

        Returns
        -------
        str
            Node id.

        """
        return self._node_id

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
    def topic_name(self) -> str:
        """
        Get topic name.

        Returns
        -------
        str
            Topic name.

        """
        return self._topic_name

    @property
    def callback_ids(self) -> tuple[str, ...] | None:
        """
        Get callback ids.

        Returns
        -------
        tuple[str, ...]
            Callback ids.

        """
        return self._callback_ids

    def __str__(self) -> str:
        """
        Get message.

        Returns
        -------
        str
            Message.

        """
        msg = ''
        msg += f'node_name: {self.node_name}, '
        msg += f'topic_name: {self.topic_name}, '
        return msg

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order


class PublisherStructValue(ValueObject, Summarizable):
    """Structured publisher value."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_values: tuple[CallbackStructValue, ...] | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        topic_name: str
            Topic name.
        callback_values: tuple[CallbackStructValue, ...] | None
            Callback values.
        construction_order: int
            Construction order.

        """
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
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node_name

    @property
    def topic_name(self) -> str:
        """
        Get topic name.

        Returns
        -------
        str
            Topic name.

        """
        return self._topic_name

    @property
    def callbacks(self) -> tuple[CallbackStructValue, ...] | None:
        """
        Get callbacks.

        Returns
        -------
        tuple[CallbackStructValue, ...] | None
            Callbacks.

        """
        return self._callbacks

    @property
    def callback_names(self) -> tuple[str, ...] | None:
        """
        Get callback names.

        Returns
        -------
        tuple[str, ...] | None
            Callback names.

        """
        if self._callbacks is None:
            return None
        return tuple(c.callback_name for c in self._callbacks)

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order

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
            'node': self.node_name,
            'topic_name': self.topic_name,
            'callbacks': self.callback_names
        })
