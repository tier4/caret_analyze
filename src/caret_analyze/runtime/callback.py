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

from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription
from .timer import Timer
from ..common import Summarizable, Summary
from ..infra.interface import RecordsProvider
from ..record import RecordsInterface
from ..value_objects import (CallbackStructValue,
                             CallbackType,
                             SubscriptionCallbackStructValue,
                             TimerCallbackStructValue)


class CallbackBase(PathBase, Summarizable):

    def __init__(
        self,
        info: CallbackStructValue,
        records_provider: RecordsProvider,
        subscription: Optional[Subscription],
        publishers: Optional[List[Publisher]],
        timer: Optional[Timer]
    ) -> None:
        super().__init__()
        self.__val = info
        self._provider = records_provider
        self._sub = subscription
        self._pubs = publishers
        self._timer = timer

    @property
    def node_name(self) -> str:
        return self.__val.node_name

    @property
    def symbol(self) -> str:
        return self.__val.symbol

    @property
    def callback_name(self) -> str:
        return self.__val.callback_name

    @property
    def callback_type(self) -> CallbackType:
        return self.__val.callback_type

    @property
    def subscription(self) -> Optional[Subscription]:
        return self._sub

    @property
    def publishers(self) -> Optional[List[Publisher]]:
        if self._pubs is None:
            return None
        return sorted(self._pubs, key=lambda x: x.topic_name)

    @property
    def timer(self) -> Optional[Timer]:
        return self._timer

    @property
    def publish_topic_names(self) -> Optional[List[str]]:
        if self.__val.publish_topic_names is None:
            return None
        return sorted(self.__val.publish_topic_names)

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self.__val.subscribe_topic_name

    @property
    def summary(self) -> Summary:
        return self.__val.summary

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.callback_records(self.__val)

        return records


class TimerCallback(CallbackBase):
    def __init__(
        self,
        callback: TimerCallbackStructValue,
        records_provider: RecordsProvider,
        publishers: Optional[List[Publisher]],
        timer: Timer
    ) -> None:
        super().__init__(callback, records_provider, None, publishers, timer)
        self.__val: TimerCallbackStructValue = callback

    @property
    def period_ns(self) -> int:
        return self.__val.period_ns


class SubscriptionCallback(CallbackBase):
    def __init__(
        self,
        callback_info: SubscriptionCallbackStructValue,
        records_provider: RecordsProvider,
        subscription: Subscription,
        publishers: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(callback_info, records_provider, subscription, publishers, None)
