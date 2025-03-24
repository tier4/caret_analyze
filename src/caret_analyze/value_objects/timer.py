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

from .callback import TimerCallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class TimerValue(ValueObject):
    """Timer info."""

    def __init__(
        self,
        period: int,
        node_name: str,
        node_id: str | None,
        callback_id: str | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        period : int
            Period of the timer.
        node_name : str
            Node name.
        node_id : str | None
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        callback_id : str | None
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        construction_order : int
            Order of instance creation within the identical node.

        """
        self._node_name = node_name
        self._node_id = node_id
        self._period = period
        self._callback_id = callback_id
        self._construction_order = construction_order

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
    def node_id(self) -> str | None:
        """
        Get node id.

        Returns
        -------
        str | None
            Node id.

        """
        return self._node_id

    @property
    def period(self) -> int:
        """
        Get period.

        Returns
        -------
        int
            Period of the timer.

        """
        return self._period

    @property
    def callback_id(self) -> str | None:
        """
        Get callback id.

        Returns
        -------
        str | None
            Callback unique id.

        """
        return self._callback_id


class TimerStructValue(ValueObject, Summarizable):
    """Timer info."""

    def __init__(
        self,
        node_name: str,
        period_ns: int,
        callback_info: TimerCallbackStructValue | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        period_ns : int
            Period of the timer.
        callback_info : TimerCallbackStructValue | None
            Static info of callback.
        construction_order : int
            Order of instance creation within the identical node.

        """
        self._node_name: str = node_name
        self._period_ns: int = period_ns
        self._callback_value = callback_info
        self._construction_order = construction_order

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
    def period_ns(self) -> int:
        """
        Get period.

        Returns
        -------
        int
            Period of the timer.

        """
        return self._period_ns

    @property
    def callback_name(self) -> str | None:
        """
        Get callback name.

        Returns
        -------
        str | None
            Callback name.

        """
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

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
            'period_ns': self.period_ns,
            'callback': self.callback_name
        })

    @property
    def callback(self) -> TimerCallbackStructValue | None:
        """
        Get callback.

        Returns
        -------
        TimerCallbackStructValue | None
            Callback.

        """
        return self._callback_value

    @property
    def construction_order(self) -> int | None:
        """
        Get construction order.

        Returns
        -------
        int | None
            Construction order.

        """
        return self._construction_order
