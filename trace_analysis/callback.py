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

from typing import List, Optional

from trace_analysis.pub_sub import Publisher, Subscription
from trace_analysis.latency import LatencyBase
from trace_analysis.record import LatencyComposer, Records
from trace_analysis.record.interface import (
    CallbackInterface,
    SubscriptionCallbackInterface,
    TimerCallbackInterface,
)

from abc import abstractmethod


class CallbackBase(CallbackInterface, LatencyBase):
    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
    ):
        self._latency_composer = latency_composer

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
    def unique_name(self) -> str:
        return f"{self.node_name}/{self.callback_name}"

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert self._latency_composer is not None

        # Consider that the callback function does not do any dropping.
        # Ignore remove_dropped flag.
        records = self._latency_composer.compose_callback_records(self)

        runtime_info_columns = ["callback_object"]
        if remove_runtime_info:
            records.drop_columns(runtime_info_columns, inplace=True)

        return records


class TimerCallback(CallbackBase, TimerCallbackInterface, LatencyBase):
    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
        node_name: str,
        callback_name: str,
        symbol: str,
        period_ns: int,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(latency_composer)

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
        latency_composer: Optional[LatencyComposer],
        node_name: str,
        callback_name: str,
        symbol: str,
        topic_name: str,
        publishes: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(latency_composer)
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
