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

from .value_object import ValueObject
from ..common import Summarizable, Summary


class CallbackType(ValueObject):
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


class CallbackValue(ValueObject, metaclass=ABCMeta):
    """Callback value base class."""

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        subscribe_topic_name: Optional[str],
        publish_topic_names: Optional[Tuple[str, ...]],
        *,  # for yaml reader only.
        callback_name: Optional[str] = None,
    ) -> None:
        self._callback_id = callback_id
        self._node_name = node_name
        self._node_id = node_id
        self._callback_name = callback_name
        self._symbol = symbol
        self._subscribe_topic_name = subscribe_topic_name
        self._publish_topic_names = publish_topic_names

    @property
    def callback_id(self) -> str:
        """
        Get callback id.

        callback id is is used to bind.
        callback id should be the same if the node name and other properties are the same.
        If any properties is different, it should be a different callback id.

        Returns
        -------
        str
            callback unique id.

        """
        return self._callback_id

    @property
    def node_id(self) -> str:
        """
        Get node id.

        Returns
        -------
        str
            node name

        """
        return self._node_id

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
    def callback_name(self) -> Optional[str]:
        """
        Get callback name.

        Note that different architecture_readers return different values.

        Returns
        -------
        str
            callback name

        """
        return self._callback_name

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self._subscribe_topic_name

    @property
    def publish_topic_names(self) -> Optional[Tuple[str, ...]]:
        return self._publish_topic_names

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


class TimerCallbackValue(CallbackValue):
    """Timer callback value."""

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        period_ns: int,
        publish_topic_names: Optional[Tuple[str, ...]],
        *,  # for yaml reader only.
        callback_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            callback_id,
            node_name,
            node_id,
            symbol,
            None,
            publish_topic_names,
            callback_name=callback_name)
        self._period_ns = period_ns

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.TIMER

    @property
    def period_ns(self) -> int:
        return self._period_ns


class SubscriptionCallbackValue(CallbackValue):
    """Subscription callback value."""

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        subscribe_topic_name: str,
        publish_topic_names: Optional[Tuple[str, ...]],
        *,  # for yaml reader only.
        callback_name: Optional[str] = None,
    ) -> None:
        self.__subscribe_topic_name = subscribe_topic_name
        super().__init__(
            callback_id,
            node_name,
            node_id,
            symbol,
            subscribe_topic_name,
            publish_topic_names,
            callback_name=callback_name)

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.SUBSCRIPTION

    @property
    def subscribe_topic_name(self) -> str:
        return self.__subscribe_topic_name


class CallbackStructValue(Summarizable, metaclass=ABCMeta):
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

        Returns
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


class TimerCallbackStructValue(CallbackStructValue, ValueObject):
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


class SubscriptionCallbackStructValue(CallbackStructValue, ValueObject):
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
