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

from abc import abstractmethod
from logging import getLogger


from .callback import CallbackStruct
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from ...exceptions import UnsupportedTypeError
from ...value_objects import (CallbackChain, InheritUniqueStamp,
                              MessageContext, MessageContextType,
                              Tilde, UseLatestMessage)

logger = getLogger(__name__)

MessageContextType.USE_LATEST_MESSAGE = \
    MessageContextType('use_latest_message')
MessageContextType.INHERIT_UNIQUE_STAMP = \
    MessageContextType('inherit_unique_stamp')
MessageContextType.CALLBACK_CHAIN = MessageContextType('callback_chain')
MessageContextType.TILDE = MessageContextType('tilde')


class MessageContextStruct():
    """Structured message context value."""

    def __init__(
        self,
        node_name: str,
        message_context_dict: dict,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        child: list[CallbackStruct] | None,
    ) -> None:
        # Since it is used as a value object,
        # mutable types such as dict should not be used.
        self._node_name = node_name
        self._message_context_dict = message_context_dict
        self._sub = subscription
        self._pub = publisher
        self._callbacks = child

    @property
    def type_name(self) -> str:
        return self.context_type.type_name

    @property
    @abstractmethod
    def context_type(self) -> MessageContextType:
        pass

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def callbacks(
        self
    ) -> list[CallbackStruct] | None:
        return self._callbacks

    def to_dict(self) -> dict:
        """
        Get message context struct dict data.

        Returns
        -------
        dict
            Message context struct dict data.

        """
        return {
            'context_type': str(self.type_name),
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name,
            'publisher_construction_order': self.publisher_construction_order,
            'subscription_construction_order': self.subscription_construction_order
        }

    def is_applicable_path(
        self,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        callbacks: list[CallbackStruct] | None
    ) -> bool:
        """
        Get applicable path.

        Parameters
        ----------
        subscription : SubscriptionStruct | None
            Target subscription value.
        publisher : PublisherStruct | None
            Target publisher value.
        callbacks : list[CallbackStruct] | None
            Target callbacks.

        Returns
        -------
        bool
            True if applicable path, false otherwise.

        """
        def _to_value(struct):
            return None if struct is None else struct.to_value()
        return _to_value(self._sub) == _to_value(subscription) \
            and _to_value(self._pub) == _to_value(publisher)

    @property
    def publisher_topic_name(self) -> str | None:
        if self._pub is None:
            return None
        return self._pub.topic_name

    @property
    def subscription_topic_name(self) -> str | None:
        if self._sub is None:
            return None
        return self._sub.topic_name

    @property
    def publisher_construction_order(self) -> int | None:
        if self._pub is None:
            return None
        return self._pub.construction_order

    @property
    def subscription_construction_order(self) -> int | None:
        if self._sub is None:
            return None
        return self._sub.construction_order

    @staticmethod
    def create_instance(
        context_type_name: str,
        context_dict: dict,
        node_name: str,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        child: list[CallbackStruct] | None
    ) -> MessageContextStruct:
        """
        Get create instance.

        Parameters
        ----------
        context_type_name : str
            Context type name.
        context_dict : dict
            Context dict.
        node_name : str
            Node name.
        subscription : SubscriptionStruct | None
            Target subscription value.
        publisher: PublisherStruct | None
            Target publisher.
        child : list[CallbackStruct] | None
            Child elements.

        Returns
        -------
        MessageContextStruct
            Created MessageContextStruct instance.

        Raises
        ------
        UnsupportedTypeError
            Argument context_type_name is not supported.

        """
        if context_type_name == str(MessageContextType.CALLBACK_CHAIN):
            return CallbackChainStruct(node_name,
                                       context_dict,
                                       subscription,
                                       publisher, child)
        if context_type_name == str(MessageContextType.INHERIT_UNIQUE_STAMP):
            return InheritUniqueStampStruct(node_name,
                                            context_dict,
                                            subscription,
                                            publisher,
                                            child)
        if context_type_name == str(MessageContextType.USE_LATEST_MESSAGE):
            return UseLatestMessageStruct(node_name,
                                          context_dict,
                                          subscription,
                                          publisher,
                                          child)
        if context_type_name == str(MessageContextType.TILDE):
            return TildeStruct(node_name,
                               context_dict,
                               subscription,
                               publisher,
                               child)

        raise UnsupportedTypeError(f'Failed to load message context. \
                                   message_context={context_type_name}')

    @abstractmethod
    def to_value(self) -> MessageContext:
        pass

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

        if self._pub is not None:
            self._pub.rename_node(src, dst)

        if self._sub is not None:
            self._sub.rename_node(src, dst)

        if self._callbacks is not None:
            for c in self._callbacks:
                c.rename_node(src, dst)

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
        if self._pub is not None:
            self._pub.rename_topic(src, dst)

        if self._sub is not None:
            self._sub.rename_topic(src, dst)

        if self._callbacks is not None:
            for c in self._callbacks:
                c.rename_topic(src, dst)


class UseLatestMessageStruct(MessageContextStruct):
    TYPE_NAME = 'use_latest_message'

    """Use message context"""

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.USE_LATEST_MESSAGE

    def to_value(self) -> UseLatestMessage:
        """
        Get use latest message.

        Returns
        -------
        UseLatestMessage
            UseLatestMessage instance.

        """
        return UseLatestMessage(
            self.node_name, self._message_context_dict,
            None if self._sub is None else self._sub.to_value(),
            None if self._pub is None else self._pub.to_value(),
            None if self.callbacks is None else tuple([v.to_value() for v in self.callbacks]))


class InheritUniqueStampStruct(MessageContextStruct):
    TYPE_NAME = 'inherit_unique_stamp'

    """
    Inherit header timestamp.

    Latency is calculated for pub/sub messages with the same timestamp value.
    If the input timestamp is not unique, it may calculate an incorrect value.
    """

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.INHERIT_UNIQUE_STAMP

    def to_value(self) -> InheritUniqueStamp:
        """
        Get inherit unique stamp.

        Returns
        -------
        InheritUniqueStamp
            InheritUniqueStamp instance.

        """
        return InheritUniqueStamp(
            self.node_name, self._message_context_dict,
            None if self._sub is None else self._sub.to_value(),
            None if self._pub is None else self._pub.to_value(),
            None if self.callbacks is None else tuple([v.to_value() for v in self.callbacks]))


class CallbackChainStruct(MessageContextStruct):
    TYPE_NAME = 'callback_chain'

    """
    Callback chain.

    Latency is calculated from callback durations in the node path.
    When a path within a node passes through multiple callbacks,
    it is assumed that messages are passed between callbacks by a buffer of
    queue size 1 (ex. a member variable that stores a single message).
    If the queue size is larger than 1,
    the node latency may be calculated to be small.

    """

    def __init__(
        self,
        node_name: str,
        message_context_dict: dict,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        callbacks: list[CallbackStruct] | None
    ) -> None:
        super().__init__(node_name,
                         message_context_dict,
                         subscription,
                         publisher,
                         callbacks)

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.CALLBACK_CHAIN

    def is_applicable_path(
        self,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        callbacks: list[CallbackStruct] | None
    ) -> bool:
        """
        Get applicable path.

        Parameters
        ----------
        subscription : SubscriptionStruct | None
            Target subscription value.
        publisher : PublisherStruct | None
            Target publisher value.
        callbacks : list[CallbackStruct] | None
            Target callbacks.

        Returns
        -------
        bool
            True if applicable path, false otherwise.

        """
        def _to_values(structs):
            if structs is None:
                return None
            else:
                return [v.to_value() for v in structs]
        if not super().is_applicable_path(subscription, publisher, callbacks):
            return False
        return _to_values(self.callbacks) == _to_values(callbacks)

    def to_dict(self) -> dict:
        """
        Get callback chain struct dict data.

        Returns
        -------
        dict
            Callback chain struct dict data.

        """
        d = super().to_dict()
        if self.callbacks is not None:
            d['callbacks'] = [_.callback_name for _ in self.callbacks]
        return d

    def to_value(self) -> CallbackChain:
        """
        Get callback chain.

        Returns
        -------
        CallbackChain
            Callback chain instance.

        """
        return CallbackChain(
            self.node_name, self._message_context_dict,
            None if self._sub is None else self._sub.to_value(),
            None if self._pub is None else self._pub.to_value(),
            None if self.callbacks is None else tuple([v.to_value() for v in self.callbacks]))


class TildeStruct(MessageContextStruct):
    TYPE_NAME = 'tilde'

    """
    tilde.

    Latency is calculated from tilde.

    """

    def __init__(
        self,
        node_name: str,
        message_context_dict: dict,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        callbacks: list[CallbackStruct] | None
    ) -> None:
        super().__init__(node_name,
                         message_context_dict,
                         subscription,
                         publisher,
                         callbacks)

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.TILDE

    def is_applicable_path(
        self,
        subscription: SubscriptionStruct | None,
        publisher: PublisherStruct | None,
        callbacks: list[CallbackStruct] | None
    ) -> bool:
        """
        Get applicable path.

        Parameters
        ----------
        subscription : SubscriptionStruct | None
            Target subscription value.
        publisher : PublisherStruct | None
            Target publisher value.
        callbacks : list[CallbackStruct] | None
            Target callbacks.

        Returns
        -------
        bool
            True if applicable path, false otherwise.

        """
        if not super().is_applicable_path(subscription, publisher, callbacks):
            return False
        return True

    def to_value(self) -> Tilde:
        """
        Get tilde value.

        Returns
        -------
        Tilde
            Tilde value instance.

        """
        return Tilde(
            self.node_name, self._message_context_dict,
            None if self._sub is None else self._sub.to_value(),
            None if self._pub is None else self._pub.to_value(),
            None if self.callbacks is None else tuple([v.to_value() for v in self.callbacks]))
