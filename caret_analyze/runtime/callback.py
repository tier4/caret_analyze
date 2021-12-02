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

from ..record import RecordsInterface
from ..infra.interface import RecordsProvider
from ..value_objects import (CallbackStructValue,
                             SubscriptionCallbackStructValue,
                             TimerCallbackStructValue)
from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription


class CallbackBase(PathBase):

    def __init__(
        self,
        info: CallbackStructValue,
        records_provider: RecordsProvider,
        subscription: Optional[Subscription],
        publishers: Optional[List[Publisher]],
    ) -> None:
        super().__init__()
        self.__info = info
        self._provider = records_provider
        self._sub = subscription
        self._pub = publishers

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
    def subscription(self) -> Optional[Subscription]:
        return self._sub

    @property
    def publishers(self) -> Optional[List[Publisher]]:
        return self._pub

    @property
    def publish_topic_names(self) -> Optional[List[str]]:
        if self.__info.publish_topic_names is None:
            return None
        return list(self.__info.publish_topic_names)

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self.__info.subscribe_topic_name

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.callback_records(self.__info)
        records.sort(records.columns[0])

        return records


class TimerCallback(CallbackBase):
    def __init__(
        self,
        callback_info: TimerCallbackStructValue,
        records_provider: RecordsProvider,
        publishers: Optional[List[Publisher]],
    ) -> None:
        super().__init__(callback_info, records_provider, None, publishers)
        self.__info: TimerCallbackStructValue = callback_info

    @property
    def period_ns(self) -> int:
        return self.__info.period_ns


class SubscriptionCallback(CallbackBase):
    def __init__(
        self,
        callback_info: SubscriptionCallbackStructValue,
        records_provider: RecordsProvider,
        subscription: Subscription,
        publishers: Optional[List[Publisher]] = None,
    ) -> None:
        super().__init__(callback_info, records_provider, subscription, publishers)
