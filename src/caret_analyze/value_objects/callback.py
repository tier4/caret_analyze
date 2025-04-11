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

from .publish_topic_info import PublishTopicInfoValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class CallbackType(ValueObject):
    """callback group type class."""

    TIMER: CallbackType
    SUBSCRIPTION: CallbackType
    SERVICE: CallbackType

    def __init__(self, name: str) -> None:
        """
        Construct callback type.

        Parameters
        ----------
        name : str
            Callback type name ['timer_callback', 'subscription_callback', 'service_callback'].

        Raises
        ------
        ValueError
            Argument name is not "timer_callback", "subscription_callback", or "service_callback".

        """
        if name not in ['timer_callback', 'subscription_callback', 'service_callback']:
            raise ValueError(f'Unsupported callback type: {name}')

        self._name = name

    def __str__(self) -> str:
        """
        Convert to string.

        Returns
        -------
        str
            Type name.

        """
        return self.type_name

    @property
    def type_name(self) -> str:
        """
        Return callback type name.

        Returns
        -------
        str
            Type name.

        """
        return self._name


CallbackType.TIMER = CallbackType('timer_callback')
CallbackType.SUBSCRIPTION = CallbackType('subscription_callback')
CallbackType.SERVICE = CallbackType('service_callback')


class CallbackValue(ValueObject, metaclass=ABCMeta):
    """
    Value object class for representing a callback.

    This class has minimal information and no structure.
    It's used as the return value of ArchitectureReader.

    """

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        subscribe_topic_name: str | None,
        service_name: str | None,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        *,  # for yaml reader only.
        callback_name: str | None = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_id : str
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        node_name : str
            Node name.
        node_id : str
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        symbol : str
            Symbol name of the callback.
        subscribe_topic_name : str | None
            Topic name which the callback subscribes.
        service_name : str | None
            Service name which the callback service.
        publish_topics : tuple[PublishTopicInfoValue] | None
            publishes information which the callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name : str | None
            Callback name, by default None. This argument is used by ArchitectureReaderYaml.

        """
        self._callback_id = callback_id
        self._node_name = node_name
        self._node_id = node_id
        self._callback_name = callback_name
        self._symbol = symbol
        self._subscribe_topic_name = subscribe_topic_name
        self._service_name = service_name
        self._publish_topics = publish_topics
        self._construction_order = construction_order

    @property
    def callback_id(self) -> str:
        """
        Get callback id.

        Callback id is is used to bind.
        Callback id should be the same if the node name and other properties are the same.
        If any properties is different, it should be a different callback id.

        Returns
        -------
        str
            Callback unique id.

        """
        return self._callback_id

    @property
    def node_id(self) -> str:
        """
        Get node id.

        Returns
        -------
        str
            Node id.

        """
        return self._node_id

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node_name

    @property
    def symbol(self) -> str:
        """
        Get callback symbol name.

        Returns
        -------
        str
            Callback symbol name.

        """
        return self._symbol

    @property
    def callback_name(self) -> str | None:
        """
        Get callback name.

        Returns
        -------
        str
            Callback name.

        Note:
        -----
        Different architecture_readers may return different values.

        """
        return self._callback_name

    @property
    def subscribe_topic_name(self) -> str | None:
        """
        Get subscription topic name.

        Returns
        -------
        str | None
            Topic name which the callback subscribes.

        Note:
        -----
        Only one subscription callback have a single subscribe topic name.

        """
        return self._subscribe_topic_name

    @property
    def service_name(self) -> str | None:
        """
        Get service name.

        Returns
        -------
        str
            Service name.

        """
        return self._service_name

    @property
    def publish_topics(self) -> tuple[PublishTopicInfoValue, ...] | None:
        """
        Get publisher topic info.

        Returns
        -------
        tuple[PublishTopicInfoValue, ...]] | None
            publish topics

        Note:
        -----
        Since callback publishes multiple topics,
        there are multiple publish topic names & construction_order.

        """
        return self._publish_topics

    @property
    @abstractmethod
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        pass

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order


class TimerCallbackValue(CallbackValue):
    """
    Value object class for representing a timer.

    This class has minimal information and no structure,
    and used as the return value of ArchitectureReader.

    """

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        period_ns: int,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        *,  # for yaml reader only.
        callback_name: str | None = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_id : str
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        node_name : str
            Node name.
        node_id : str
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        symbol : str
            Symbol name of the callback.
        period_ns : int
            Period of the timer.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name: str | None
            Callback name, by default None. This argument is used by ArchitectureReaderYaml.

        """
        super().__init__(
            callback_id=callback_id,
            node_name=node_name,
            node_id=node_id,
            symbol=symbol,
            subscribe_topic_name=None,
            service_name=None,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)
        self._period_ns = period_ns

    @property
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.TIMER

    @property
    def period_ns(self) -> int:
        """
        Get period.

        Returns
        -------
        int
            Period of the timer.

        """
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
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        *,  # for yaml reader only.
        callback_name: str | None = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_id : str
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        node_name : str
            Node name.
        node_id : str
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        symbol : str
            Symbol name of the callback.
        subscribe_topic_name : str
            Topic name which the callback subscribes.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the callback publishes.
        construction_order : int
            Order of instance creation within the identical node.
        callback_name : str | None
            Callback name, by default None. This argument is used by ArchitectureReaderYaml.

        """
        self.__subscribe_topic_name = subscribe_topic_name
        super().__init__(
            callback_id=callback_id,
            node_name=node_name,
            node_id=node_id,
            symbol=symbol,
            subscribe_topic_name=subscribe_topic_name,
            service_name=None,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)

    @property
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.SUBSCRIPTION

    @property
    def subscribe_topic_name(self) -> str:
        """
        Get subscription topic name.

        Returns
        -------
        str
            Topic name which the callback subscribes.

        """
        return self.__subscribe_topic_name


class ServiceCallbackValue(CallbackValue):
    """Value object class for representing a service."""

    def __init__(
        self,
        callback_id: str,
        node_name: str,
        node_id: str,
        symbol: str,
        service_name: str,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        *,  # for yaml reader only.
        callback_name: str | None = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_id : str
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        node_name : str
            Node name.
        node_id : str
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        symbol : str
            Symbol name of the service callback.
        service_name : str
            Service name which the service callback.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the service callback publishes.
        construction_order : int
            Order of instance creation within the identical node.
        callback_name : str | None
            Service callback name, by default None.
            This argument is used by ArchitectureReaderYaml.

        """
        self.__service_name = service_name
        super().__init__(
            callback_id=callback_id,
            node_name=node_name,
            node_id=node_id,
            symbol=symbol,
            subscribe_topic_name=None,
            service_name=service_name,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)

    @property
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.SERVICE

    @property
    def service_name(self) -> str:
        """
        Get service name.

        Returns
        -------
        str
            Service name.

        """
        return self.__service_name


class CallbackStructValue(Summarizable, metaclass=ABCMeta):
    """Callback value base class."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        subscribe_topic_name: str | None,
        service_name: str | None,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        callback_name: str
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        symbol : str
            Symbol name of the callback.
        subscribe_topic_name : str | None
            Topic name which the callback subscribes.
        service_name : str | None
            Service name which the service callback service.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name : str
            Callback name.

        """
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
            Node name.

        """
        return self._node_name

    @property
    def symbol(self) -> str:
        """
        Get callback symbol name.

        Returns
        -------
        str
            Callback symbol name.

        """
        return self._symbol

    @property
    def callback_name(self) -> str:
        """
        Get callback name.

        Returns
        -------
        str
            Callback name.

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
            Callback type.

        """
        pass

    @property
    def callback_type_name(self) -> str:
        """
        Get callback type name.

        Returns
        -------
        str
            Callback type name.

        """
        return str(self.callback_type)

    @property
    def subscribe_topic_name(self) -> str | None:
        """
        Get subscribe topic name.

        Returns
        -------
        str | None
            Subscribe topic name.

        """
        return self._subscribe_topic_name

    @property
    def service_name(self) -> str | None:
        """
        Get service name.

        Returns
        -------
        str | None
            Service name.

        """
        return self._service_name

    @property
    def publish_topics(self) -> tuple[PublishTopicInfoValue, ...] | None:
        """
        Get publisher topic info.

        Returns
        -------
        tuple[PublishTopicInfoValue, ...] | None
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
            Construction order.

        """
        return self._construction_order

    @property
    @abstractmethod
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
        pass


class TimerCallbackStructValue(CallbackStructValue, ValueObject):
    """Structured timer callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        period_ns: int,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        callback_name: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        symbol : str
            Symbol name of the callback.
        period_ns : int
            Period of the timer.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name: str
            Callback name, by default None. This argument is used by ArchitectureReaderYaml.

        """
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
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.TIMER

    @property
    def period_ns(self) -> int:
        """
        Get period.

        Returns
        -------
        int
            Period of the timer.

        """
        return self._period_ns

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
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
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        callback_name: str
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        symbol : str
            Symbol name of the callback.
        subscribe_topic_name : str
            Topic name which the callback subscribes.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name : str
            Callback name.

        """
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscribe_topic_name=subscribe_topic_name,
            service_name=None,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)

    @property
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.SUBSCRIPTION

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
        return Summary({
            'name': self.callback_name,
            'node': self.node_name,
            'type': self.callback_type_name,
            'topic': self.subscribe_topic_name
        })

    @property
    def subscribe_topic_name(self) -> str:
        """
        Get subscription topic name.

        Returns
        -------
        str
            Topic name which the callback subscribes.

        """
        topic_name = super().subscribe_topic_name
        assert topic_name is not None
        return topic_name


class ServiceCallbackStructValue(CallbackStructValue, ValueObject):
    """Structured service callback value."""

    def __init__(
        self,
        node_name: str,
        symbol: str,
        service_name: str,
        publish_topics: tuple[PublishTopicInfoValue, ...] | None,
        construction_order: int,
        callback_name: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        symbol : str
            Symbol name of the service callback.
        service_name : str
            Service name which the service callback service.
        publish_topics : tuple[PublishTopicInfoValue, ...] | None
            Topic information which the service callback publishes.
        construction_order: int
            Order of instance creation within the identical node.
        callback_name: str
            Service callback name. This argument is used by ArchitectureReaderYaml.

        """
        super().__init__(
            node_name=node_name,
            symbol=symbol,
            subscribe_topic_name=None,
            service_name=service_name,
            publish_topics=publish_topics,
            construction_order=construction_order,
            callback_name=callback_name)

    @property
    def callback_type(self) -> CallbackType:
        """
        Get callback type name.

        Returns
        -------
        CallbackType
            Callback type.

        """
        return CallbackType.SERVICE

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
        return Summary({
            'name': self.callback_name,
            'node': self.node_name,
            'type': self.callback_type_name,
            'service': self.service_name
        })
