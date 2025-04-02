# Copyright 2021 TIER IV, Inc.
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

from ...exceptions import MultipleItemFoundError
from ...value_objects import (CallbackStructValue, CallbackType,
                              PublishTopicInfoValue,
                              ServiceCallbackStructValue,
                              SubscriptionCallbackStructValue,
                              TimerCallbackStructValue)


class CallbackStruct(metaclass=ABCMeta):
    """Callback value base class."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        subscribe_topic_name: str | None,
        service_name: str | None,
        publish_topics: list[PublishTopicInfoValue] | None,
        construction_order: int,
        callback_name: str
    ) -> None:
        self._node_name = node_name
        self._callback_name = callback_name
        self._symbol = symbol
        self._subscribe_topic_name = subscribe_topic_name
        self._service_name = service_name
        self._publish_topics = publish_topics
        self._construction_order = construction_order

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

    @callback_name.setter
    def callback_name(self, n: str):
        self._callback_name = n

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
    def subscribe_topic_name(self) -> str | None:
        return self._subscribe_topic_name

    @property
    def service_name(self) -> str | None:
        return self._service_name

    @property
    def publish_topics(self) -> list[PublishTopicInfoValue] | None:
        """
        Get publisher topic info.

        Returns
        -------
        PublishTopicInfo
            publish topics

        """
        return self._publish_topics

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            construction order

        """
        return self._construction_order

    @abstractmethod
    def to_value(self) -> CallbackStructValue:
        pass

    def insert_publisher(self, publish_topic_info: PublishTopicInfoValue) -> None:
        """
        Insert publisher.

        Parameters
        ----------
        publish_topic_info : PublishTopicInfoValue
            Publish topic info to insert.

        Raises
        ------
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if self.publish_topics is not None:
            for count in range(len(self.publish_topics)):
                if self.publish_topics[count] == publish_topic_info:
                    msg = 'publisher is already registered. '
                    msg += f' publish_topic_name: {publish_topic_info.topic_name}'
                    msg += f' construction_order: {publish_topic_info.construction_order}'
                    raise MultipleItemFoundError(msg)
            self.publish_topics.append(PublishTopicInfoValue
                                       (publish_topic_info.topic_name,
                                        publish_topic_info.construction_order))
        else:
            self._publish_topics = []
            self._publish_topics.append(PublishTopicInfoValue
                                        (publish_topic_info.topic_name,
                                         publish_topic_info.construction_order))

    def remove_publisher(self, publish_topic_info: PublishTopicInfoValue) -> None:
        """
        Remove publisher.

        Parameters
        ----------
        publish_topic_info : PublishTopicInfoValue
            Publish topic info to remove.

        """
        if self.publish_topics is not None:
            for publish_topic in self.publish_topics:
                if publish_topic == publish_topic_info:
                    self.publish_topics.remove(publish_topic)

    def rename_node(self, src: str, dst: str) -> None:
        """
        Rename node.

        Parameters
        ----------
        src : str
            Current node name.
        dst : str
            Updated node name.

        """
        if self.node_name == src:
            self._node_name = dst

    def rename_topic(self, src: str, dst: str) -> None:
        """
        Rename topic.

        Parameters
        ----------
        src : str
            Current topic name.
        dst : str
            Updated topic name.

        """
        if self.publish_topics is not None:
            for i, p in enumerate(self.publish_topics):
                if p.topic_name == src:
                    pub_info = PublishTopicInfoValue(dst, p.construction_order)
                    self.publish_topics[i] = pub_info

        if self._subscribe_topic_name is not None:
            if self._subscribe_topic_name == src:
                self._subscribe_topic_name = dst


class TimerCallbackStruct(CallbackStruct):
    """Structured timer callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        period_ns: int,
        publish_topics: list[PublishTopicInfoValue] | None,
        construction_order: int,
        callback_name: str,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscribe_topic_name=None,
            service_name=None,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)
        self._period_ns = period_ns

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.TIMER

    @property
    def period_ns(self) -> int:
        return self._period_ns

    def to_value(self) -> TimerCallbackStructValue:
        """
        Get timer callback struct value.

        Returns
        -------
        TimerCallbackStructValue
            Timer callback struct value instance.

        """
        return TimerCallbackStructValue(
            self.node_name, self.symbol, self.period_ns,
            None if self.publish_topics is None else tuple(self.publish_topics),
            self.construction_order, self.callback_name)


class SubscriptionCallbackStruct(CallbackStruct):
    """Structured subscription callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        subscribe_topic_name: str,
        publish_topics: list[PublishTopicInfoValue] | None,
        construction_order: int,
        callback_name: str,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscribe_topic_name=subscribe_topic_name,
            service_name=None,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)
        self.__subscribe_topic_name = subscribe_topic_name

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.SUBSCRIPTION

    @property
    def subscribe_topic_name(self) -> str:
        return self.__subscribe_topic_name

    def to_value(self) -> SubscriptionCallbackStructValue:
        """
        Get subscription callback struct value.

        Returns
        -------
        SubscriptionCallbackStructValue
            Subscription callback struct value instance.

        """
        return SubscriptionCallbackStructValue(
            self.node_name, self.symbol,
            self.subscribe_topic_name,
            None if self.publish_topics is None else tuple(self.publish_topics),
            self.construction_order, self.callback_name)

    def rename_topic(self, src: str, dst: str) -> None:
        """
        Rename topic.

        Parameters
        ----------
        src : str
            Current topic name.
        dst : str
            Updated topic name.

        """
        if self.publish_topics is not None:
            for i, p in enumerate(self.publish_topics):
                if p.topic_name == src:
                    self.publish_topics[i] = PublishTopicInfoValue(dst, p.construction_order)

        if self.subscribe_topic_name == src:
            self.__subscribe_topic_name = dst


class ServiceCallbackStruct(CallbackStruct):
    """Structured service callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        service_name: str,
        publish_topics: list[PublishTopicInfoValue] | None,
        construction_order: int,
        callback_name: str,
    ) -> None:
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscribe_topic_name=None,
            service_name=service_name,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)
        self.__service_name = service_name

    @property
    def callback_type(self) -> CallbackType:
        return CallbackType.SERVICE

    @property
    def service_name(self) -> str:
        return self.__service_name

    def to_value(self) -> ServiceCallbackStructValue:
        """
        Get service callback struct value.

        Returns
        -------
        ServiceCallbackStructValue
            Service callback struct value instance.

        """
        return ServiceCallbackStructValue(
            self.node_name,
            self.symbol,
            self.service_name,
            None if self.publish_topics is None else tuple(self.publish_topics),
            self.construction_order, self.callback_name)
