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

from .callback import TimerCallbackStruct
from ...value_objects import TimerStructValue


class TimerStruct():
    """Timer info."""

    def __init__(
        self,
        node_name: str,
        period_ns: int,
        callback_info: TimerCallbackStruct | None,
        construction_order: int,
    ) -> None:
        self._node_name: str = node_name
        self._period_ns: int = period_ns
        self._callback_value = callback_info
        self._construction_order = construction_order

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def callback_name(self) -> str | None:
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

    @property
    def callback(self) -> TimerCallbackStruct | None:
        return self._callback_value

    @property
    def construction_order(self) -> int:
        return self._construction_order

    def to_value(self) -> TimerStructValue:
        """
        Get timer struct value.

        Returns
        -------
        TimerStructValue
            Timer struct value instance.

        """
        return TimerStructValue(
            node_name=self.node_name,
            period_ns=self.period_ns,
            callback_info=None if self.callback is None else self.callback.to_value(),
            construction_order=self.construction_order)

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

        if self._callback_value is not None:
            self._callback_value.rename_node(src, dst)

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
        if self._callback_value is not None:
            self._callback_value.rename_topic(src, dst)
