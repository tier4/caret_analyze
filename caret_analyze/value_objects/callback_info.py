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

from abc import abstractmethod
from typing import Optional

from .callback_name import CallbackName
from .callback_type import CallbackType
from .subscription import Subscription
from .value_object import ValueObject


class CallbackInfo():
    """Callback info base class."""

    def __init__(
        self,
        node_name: str,
        callback_name: str,
        symbol: str,
        subscription: Optional[Subscription]
    ) -> None:
        self._node_name = node_name
        self._callback_name = CallbackName(node_name, callback_name)
        self._symbol = symbol
        self._subscription = subscription

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

        Returns
        -------
        str
            callback name

        """
        return self._callback_name.name

    @property
    def callback_unique_name(self) -> str:
        """
        Get callback unique name.

        Returns
        -------
        str
            callback unique name

        """
        return self._callback_name.unique_name

    @property
    @abstractmethod
    def callback_type(self) -> str:
        """
        Get callback type name.

        Returns
        -------
        str
            callback type name

        """
        pass

    @property
    def subscription(self) -> Optional[Subscription]:
        return self._subscription


class TimerCallbackInfo(CallbackInfo, ValueObject):
    """Timer callback info."""

    TYPE_NAME = 'timer_callback'

    def __init__(
        self,
        node_name: str,
        callback_name: str,
        symbol: str,
        period_ns: int
    ) -> None:
        super().__init__(node_name, callback_name, symbol, None)
        self._period_ns = period_ns

    @classmethod
    def to_indexed_callback_name(cls, i: int) -> str:
        return f'{cls.TYPE_NAME}_{i}'

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.Timer

    @property
    def period_ns(self) -> int:
        return self._period_ns


class SubscriptionCallbackInfo(CallbackInfo, ValueObject):
    """Subscription callback info."""

    TYPE_NAME = 'subscription_callback'

    def __init__(
        self,
        node_name: str,
        callback_name: str,
        symbol: str,
        topic_name: str
    ) -> None:
        subscription = Subscription(node_name, topic_name, callback_name)
        super().__init__(node_name, callback_name, symbol, subscription)
        self._topic_name = topic_name

    @classmethod
    def to_indexed_callback_name(cls, i: int) -> str:
        return f'{cls.TYPE_NAME}_{i}'

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.Subscription

    @property
    def subscription(self) -> Optional[Subscription]:
        return self._subscription

    @property
    def topic_name(self) -> str:
        return self._topic_name
