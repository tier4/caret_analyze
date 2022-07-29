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

from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

from ..common import Summarizable, Summary


class CallbackType():
    """callback group type class."""

    TIMER: CallbackType
    SUBSCRIPTION: CallbackType

    def __init__(self, name: str) -> None:
        """
        Construct callback type.

        Parameters
        ----------
        name : str
            callback type name ['timer_callback', 'subscription_callback']

        """
        if name not in ['timer_callback', 'subscription_callback']:
            raise ValueError(f'Unsupported callback type: {name}')

        self._name = name

    def __str__(self) -> str:
        return self.type_name

    @property
    def type_name(self) -> str:
        """
        Return callback type name.

        Returns
        -------
        str
            type name.

        """
        return self._name


CallbackType.TIMER = CallbackType('timer_callback')
CallbackType.SUBSCRIPTION = CallbackType('subscription_callback')


class TimerCallbackStruct(CallbackStruct):
    """Structured timer callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        period_ns: int,
        publish_topic_names: Optional[Tuple[str, ...]],
        callback_name: str,
    ) -> None:
        super().__init__(
            node_name,
            symbol,
            None,
            publish_topic_names,
            callback_name)
        self._period_ns = period_ns

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.TIMER

    @property
    def period_ns(self) -> int:
        return self._period_ns

    @property
    def summary(self) -> Summary:
        return Summary({
            'name': self.callback_name,
            'type': self.callback_type_name,
            'period_ns': self.period_ns
        })


class SubscriptionCallbackStruct(CallbackStruct):
    """Structured subscription callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        subscribe_topic_name: str,
        publish_topic_names: Optional[Tuple[str, ...]],
        callback_name: str,
    ) -> None:
        super().__init__(node_name, symbol, subscribe_topic_name,
                         publish_topic_names, callback_name)

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.SUBSCRIPTION

    @property
    def summary(self) -> Summary:
        return Summary({
            'name': self.callback_name,
            'type': self.callback_type_name,
            'topic': self.subscribe_topic_name
        })
