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
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .latency import LatencyBase
from .pub_sub import Publisher
from .pub_sub import Subscription
from .record import LatencyComposer
from .record import RecordsInterface
from .record.interface import CallbackInterface
from .record.interface import SubscriptionCallbackInterface
from .record.interface import TimerCallbackInterface


class CallbackBase(CallbackInterface, LatencyBase):
    column_names = ['callback_start_timestamp', 'callback_end_timestamp']

    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
    ) -> None:
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

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return super().to_dataframe(remove_dropped, column_names=CallbackBase.column_names)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_timeseries(remove_dropped, column_names=CallbackBase.column_names)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_histogram(binsize_ns, column_names=CallbackBase.column_names)

    @property
    def unique_name(self) -> str:
        return f'{self.node_name}/{self.callback_name}'

    def to_records(self) -> RecordsInterface:
        assert self._latency_composer is not None
        records = self._latency_composer.compose_callback_records(self)
        records.sort(CallbackBase.column_names[0], inplace=True)

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
