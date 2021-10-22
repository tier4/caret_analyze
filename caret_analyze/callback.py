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
from .record import RecordsContainer, RecordsInterface
from .record.trace_points import TRACE_POINT
from .value_objects.callback_info import CallbackInfo, SubscriptionCallbackInfo, TimerCallbackInfo
from .value_objects.publisher import Publisher
from .value_objects.subscription import Subscription


class CallbackBase(LatencyBase):

    _column_names = [
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
        TRACE_POINT.CALLBACK_END_TIMESTAMP
    ]

    def __init__(
        self,
        info: CallbackInfo,
        records_container: Optional[RecordsContainer],
    ) -> None:
        self.__info = info
        self._records_container = records_container

    @property
    def node_name(self) -> str:
        return self.__info.node_name

    @property
    def symbol(self) -> str:
        return self.__info.symbol

    @property
    def callback_name(self) -> str:
        return self.__info.callback_name

    @property
    @abstractmethod
    def publishes(self) -> List[Publisher]:
        pass

    @property
    def subscription(self) -> Optional[Subscription]:
        return self.__info.subscription

    @property
    def column_names(self) -> List[str]:
        return CallbackBase._column_names

    @property
    def callback_unique_name(self) -> str:
        return self.__info.callback_unique_name

    @property
    @abstractmethod
    def info(self):
        pass

    def to_records(self) -> RecordsInterface:
        assert self._records_container is not None
        records = self._records_container.compose_callback_records(self.info)
        records.sort(self.column_names[0], inplace=True)

        return records


class TimerCallback(CallbackBase, LatencyBase):
    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        node_name: str,
        callback_name: str,
        symbol: str,
        period_ns: int,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        self.__info = TimerCallbackInfo(
            node_name, callback_name, symbol, period_ns
        )
        super().__init__(self.__info, records_container)

        self._publishes: List[Publisher] = publishes or []

    @property
    def publishes(self) -> List[Publisher]:
        return self._publishes

    @property
    def period_ns(self) -> int:
        return self.__info.period_ns

    @property
    def info(self) -> TimerCallbackInfo:
        return self.__info


class SubscriptionCallback(CallbackBase, LatencyBase):
    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        node_name: str,
        callback_name: str,
        symbol: str,
        topic_name: str,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        self.__info = SubscriptionCallbackInfo(node_name, callback_name, symbol, topic_name)
        super().__init__(self.__info, records_container)
        self._publishes: List[Publisher] = publishes or []

    @property
    def publishes(self) -> List[Publisher]:
        return self._publishes

    @property
    def topic_name(self) -> str:
        return self.__info.topic_name

    @property
    def info(self) -> SubscriptionCallbackInfo:
        return self.__info
