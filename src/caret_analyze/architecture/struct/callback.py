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

from ...common import Summarizable, Summary

from ...value_objects import (CallbackStructValue, CallbackType,
                              SubscriptionCallbackStructValue,
                              TimerCallbackStructValue)


class CallbackStruct(Summarizable, metaclass=ABCMeta):
    """Callback value base class."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        subscribe_topic_name: Optional[str],
        publish_topic_names: Optional[Tuple[str, ...]],
        callback_name: str,
    ) -> None:
        self._node_name = node_name
        self._callback_name = callback_name
        self._symbol = symbol
        self._subscribe_topic_name = subscribe_topic_name
        self._publish_topic_names = publish_topic_names

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name

        """
        return self._node_name

    @property
    def symbol(self) -> str:
        """
        Get callback symbol name.

        Returns
        -------
        str
            callback symbol name

        """
        return self._symbol

    @property
    def callback_name(self) -> str:
        """
        Get callback name.

        Returns the backslash is redundant between brackets
        -------
        str
            callback name

        """
        return self._callback_name

    @property
    @abstractmethod
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            callback type

        """
        pass

    @property
    def callback_type_name(self) -> str:
        return str(self.callback_type)

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self._subscribe_topic_name

    @property
    def publish_topic_names(self) -> Optional[Tuple[str, ...]]:
        return self._publish_topic_names

    @property
    @abstractmethod
    def summary(self) -> Summary:
        pass

    @abstractmethod
    def to_value(self) -> CallbackStructValue:
        pass


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

    def to_value(self) -> TimerCallbackStructValue:
        return TimerCallbackStructValue(
            self.node_name, self.symbol, self.period_ns,
            None if self.subscribe_topic_name is None else self.publish_topic_names,
            self.callback_name)


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

    def to_value(self) -> SubscriptionCallbackStructValue:
        return SubscriptionCallbackStructValue(
            self.node_name, self.symbol,
            '' if self.subscribe_topic_name is None else self.subscribe_topic_name,
            None if self.publish_topic_names is None else self.publish_topic_names,
            self.callback_name)
