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
from typing import Iterable, Iterator, List, Optional, Tuple

from .callback import CallbacksStruct, TimerCallbackStruct
from ..reader_interface import ArchitectureReader
from ...exceptions import Error
from ...value_objects import (
    NodeValue,
    TimerStructValue,
    TimerValue,
)

logger = getLogger(__name__)


class TimerStruct:

    def __init__(
        self,
        node_name: str,
        period_ns: int,
        callback: Optional[TimerCallbackStruct] = None,
    ) -> None:
        self._node_name: str = node_name
        self._period_ns: int = period_ns
        self._callback = callback

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def callback(self) -> Optional[TimerCallbackStruct]:
        return self._callback

    def to_value(self) -> TimerStructValue:
        callback = None if self.callback is None else self.callback.to_value()
        return TimerStructValue(
            node_name=self.node_name,
            period_ns=self.period_ns,
            callback=callback
        )

    @staticmethod
    def create_instance(
        callbacks: CallbacksStruct,
        timer_value: TimerValue
    ):
        callback = None
        if timer_value.callback_id is not None:
            callback = callbacks.get_callback(timer_value.callback_id)
            assert isinstance(callback, TimerCallbackStruct)

        return TimerStruct(
            timer_value.node_name,
            timer_value.period,
            callback  # type: ignore
        )


class TimersStruct(Iterable):
    def __init__(
        self,
    ) -> None:
        self._data: List[TimerStruct] = []

    def to_value(self) -> Tuple[TimerStructValue, ...]:
        return tuple(timer.to_value() for timer in self._data)

    def __iter__(self) -> Iterator[TimerStruct]:
        return iter(self._data)

    def add(self, timer: TimerStruct) -> None:
        self._data.append(timer)

    @staticmethod
    def create_from_reader(
        reader: ArchitectureReader,
        callbacks: CallbacksStruct,
        node: NodeValue
    ) -> TimersStruct:
        timers = TimersStruct()

        timer_values = reader.get_timers(node.node_name)
        for timer_value in timer_values:
            try:
                timer = TimerStruct.create_instance(callbacks, timer_value)
                timers.add(timer)
            except Error as e:
                logger.warning(e)

        return timers
