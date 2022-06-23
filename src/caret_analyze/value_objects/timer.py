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

from typing import Optional

from .callback import TimerCallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class TimerValue(ValueObject):
    """Timer info."""

    def __init__(
        self,
        period: int,
        node_name: str,
        node_id: Optional[str],
        callback_id: Optional[str],
    ) -> None:
        self._node_name = node_name
        self._node_id = node_id
        self._period = period
        self._callback_id = callback_id

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def node_id(self) -> Optional[str]:
        return self._node_id

    @property
    def period(self) -> int:
        return self._period

    @property
    def callback_id(self) -> Optional[str]:
        return self._callback_id


class TimerStructValue(ValueObject, Summarizable):
    """Timer info."""

    def __init__(
        self,
        node_name: str,
        period_ns: int,
        callback_info: Optional[TimerCallbackStructValue],
    ) -> None:
        self._node_name: str = node_name
        self._period_ns: int = period_ns
        self._callback_value = callback_info

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def callback_name(self) -> Optional[str]:
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

    @property
    def summary(self) -> Summary:
        return Summary({
            'period_ns': self.period_ns,
            'callback': self.callback_name
        })

    @property
    def callback(self) -> Optional[TimerCallbackStructValue]:
        return self._callback_value
