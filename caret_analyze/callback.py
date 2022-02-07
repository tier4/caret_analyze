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

from abc import abstractmethod
from typing import List, Optional

from .latency import LatencyBase
from .pub_sub import Publisher
from .pub_sub import Subscription
from .record import RecordsContainer
from .record import RecordsInterface
from .record.interface import CallbackInterface
from .record.interface import SubscriptionCallbackInterface
from .record.interface import TimerCallbackInterface
from .record.trace_points import TRACE_POINT


class CallbackBase(CallbackInterface, LatencyBase):

    _column_names = [
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
        TRACE_POINT.CALLBACK_END_TIMESTAMP
    ]

    def __init__(
        self,
        records_container: Optional[RecordsContainer],
    ) -> None:
        self._records_container = records_container

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def symbol(self) -> str:
        pass

    @property
    @abstractmethod
    def callback_name(self) -> str:
        pass

    @property
    @abstractmethod
    def publishes(self) -> List[Publisher]:
        pass

    @property
    @abstractmethod
    def subscription(self) -> Optional[Subscription]:
        pass

    @property
    def column_names(self) -> List[str]:
        return CallbackBase._column_names

    @property
    def unique_name(self) -> str:
        return f'{self.node_name}/{self.callback_name}'

    def to_records(self) -> RecordsInterface:
        assert self._records_container is not None
        records = self._records_container.compose_callback_records(self)
        records.sort(self.column_names[0], inplace=True)

        return records


class TimerCallback(CallbackBase, TimerCallbackInterface, LatencyBase):
    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        node_name: str,
        callback_name: str,
        symbol: str,
        period_ns: int,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(records_container)

        self._node_name: str = node_name
        self._callback_name: str = callback_name
        self._symbol: str = symbol
        self._publishes: List[Publisher] = publishes or []
        self._period_ns: int = period_ns

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def callback_name(self) -> str:
        return self._callback_name

    @property
    def publishes(self) -> List[Publisher]:
        return self._publishes

    @property
    def subscription(self) -> Optional[Subscription]:
        return None

    @property
    def period_ns(self) -> int:
        return self._period_ns


class SubscriptionCallback(CallbackBase, SubscriptionCallbackInterface, LatencyBase):
    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        node_name: str,
        callback_name: str,
        symbol: str,
        topic_name: str,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(records_container)
        self._subscription = Subscription(node_name, topic_name, callback_name)
        self._node_name: str = node_name
        self._callback_name: str = callback_name
        self._symbol: str = symbol
        self._publishes: List[Publisher] = publishes or []
        self._topic_name: str = topic_name

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def callback_name(self) -> str:
        return self._callback_name

    @property
    def publishes(self) -> List[Publisher]:
        return self._publishes

    @property
    def subscription(self) -> Optional[Subscription]:
        return self._subscription

    @property
    def topic_name(self) -> str:
        return self._topic_name
