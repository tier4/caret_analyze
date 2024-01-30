
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
from logging import getLogger

from .callback import CallbackStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary
from ..exceptions import UnsupportedTypeError

logger = getLogger(__name__)


class MessageContextType(ValueObject):
    """Message context type."""

    USE_LATEST_MESSAGE: MessageContextType
    CALLBACK_CHAIN: MessageContextType
    INHERIT_UNIQUE_STAMP:  MessageContextType
    TILDE:  MessageContextType

    def __init__(
        self,
        type_name: str
    ) -> None:
        """
        Construct an instance.

        Parameter
        ---------
        type_name : str
            Type name.

        """
        self._type_name = type_name

    @property
    def type_name(self) -> str:
        """
        Get type name.

        Parameter
        ---------
        str
            Type name.

        """
        return self._type_name

    def __str__(self) -> str:
        """
        Get type name.

        Parameter
        ---------
        str
            Type name.

        """
        return self.type_name


MessageContextType.USE_LATEST_MESSAGE = \
    MessageContextType('use_latest_message')
MessageContextType.INHERIT_UNIQUE_STAMP = \
    MessageContextType('inherit_unique_stamp')
MessageContextType.CALLBACK_CHAIN = MessageContextType('callback_chain')
MessageContextType.TILDE = MessageContextType('tilde')


class MessageContext(ValueObject, Summarizable):
    """Structured message context value."""

    def __init__(
        self,
        node_name: str,
        message_context_dict: dict,
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        child: tuple[CallbackStructValue, ...] | None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        message_context_dict : dict
            Message context dict.
        subscription : SubscriptionStructValue | None
            Target subscription value.
        publisher : PublisherStructValue | None
            Target publisher.
        child : tuple[CallbackStructValue, ...] | None
            Child elements.

        """
        # Since it is used as a value object,
        # mutable types such as dict should not be used.
        self._node_name = node_name
        self._sub = subscription
        self._pub = publisher
        self._callbacks = child

    @property
    def type_name(self) -> str:
        """
        Get type name.

        Returns
        -------
        str
            Type name.

        """
        return self.context_type.type_name

    @property
    @abstractmethod
    def context_type(self) -> MessageContextType:
        """
        Get context type.

        Returns
        -------
        MessageContextType
            Message context type.

        """
        pass

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
    def callbacks(
        self
    ) -> tuple[CallbackStructValue, ...] | None:
        """
        Get callbacks.

        Returns
        -------
        tuple[CallbackStructValue, ...] | None
            Callback struct values.

        """
        return self._callbacks

    def to_dict(self) -> dict:
        """
        Get to dict.

        Returns
        -------
        dict
            Dict.

        """
        return {
            'context_type': str(self.type_name),
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name
        }

    def is_applicable_path(
        self,
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        callbacks: tuple[CallbackStructValue, ...] | None
    ) -> bool:
        """
        Get applicable path.

        Parameters
        ----------
        subscription : SubscriptionStructValue | None
            Target subscription value.
        publisher : PublisherStructValue | None
            Target publisher value.
        callbacks : tuple[CallbackStructValue, ...] | None
            Target callbacks.

        Returns
        -------
        bool
            True if applicable path, false otherwise.

        """
        return self._sub == subscription and self._pub == publisher

    @property
    def publisher_topic_name(self) -> str | None:
        """
        Get publisher topic name.

        Returns
        -------
        str | None
            Publisher topic name.

        """
        if self._pub is None:
            return None
        return self._pub.topic_name

    @property
    def subscription_topic_name(self) -> str | None:
        """
        Get subscription topic name.

        Returns
        -------
        str | None
            Subscription topic name.

        """
        if self._sub is None:
            return None
        return self._sub.topic_name

    @property
    def publisher_construction_order(self) -> int | None:
        """
        Get publisher topic name.

        Returns
        -------
        int | None
            Publisher construction order.

        """
        if self._pub is None:
            return None
        return self._pub.construction_order

    @property
    def subscription_construction_order(self) -> int | None:
        """
        Get subscription construction order.

        Returns
        -------
        int | None
            Subscription construction order.

        """
        if self._sub is None:
            return None
        return self._sub.construction_order

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary.

        """
        return Summary({
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name,
            'type': str(self.type_name)
        })

    @abstractmethod
    def verify(self) -> bool:
        """
        Get verify.

        Returns
        -------
        bool
            Verify or not.

        """
        pass

    @staticmethod
    def create_instance(
        context_type_name: str,
        context_dict: dict,
        node_name: str,
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        child: tuple[CallbackStructValue, ...] | None
    ) -> MessageContext:
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
        subscription : SubscriptionStructValue | None
            Target subscription value.
        publisher: PublisherStructValue | None
            Target publisher.
        child : tuple[CallbackStructValue, ...] | None
            Child elements.

        Returns
        -------
        MessageContext
            Message context.

        """
        if context_type_name == str(MessageContextType.CALLBACK_CHAIN):
            return CallbackChain(node_name,
                                 context_dict,
                                 subscription,
                                 publisher, child)
        if context_type_name == str(MessageContextType.INHERIT_UNIQUE_STAMP):
            return InheritUniqueStamp(node_name,
                                      context_dict,
                                      subscription,
                                      publisher,
                                      child)
        if context_type_name == str(MessageContextType.USE_LATEST_MESSAGE):
            return UseLatestMessage(node_name,
                                    context_dict,
                                    subscription,
                                    publisher,
                                    child)
        if context_type_name == str(MessageContextType.TILDE):
            return Tilde(node_name,
                         context_dict,
                         subscription,
                         publisher,
                         child)

        raise UnsupportedTypeError(f'Failed to load message context. \
                                   message_context={context_type_name}')


class UseLatestMessage(MessageContext):
    TYPE_NAME = 'use_latest_message'

    """Use message context"""

    def verify(self) -> bool:
        """
        Get verify.

        Returns
        -------
        bool
            Verify or not.

        """
        return True

    @property
    def context_type(self) -> MessageContextType:
        """
        Get context type.

        Returns
        -------
        MessageContextType
            Message context type.

        """
        return MessageContextType.USE_LATEST_MESSAGE


class InheritUniqueStamp(MessageContext):
    TYPE_NAME = 'inherit_unique_stamp'

    """
    Inherit header timestamp.

    Latency is calculated for pub/sub messages with the same timestamp value.
    If the input timestamp is not unique, it may calculate an incorrect value.
    """

    @property
    def context_type(self) -> MessageContextType:
        """
        Get context type.

        Returns
        -------
        MessageContextType
            Message context type.

        """
        return MessageContextType.INHERIT_UNIQUE_STAMP

    def verify(self) -> bool:
        """
        Get verify.

        Returns
        -------
        bool
            Same or difference.

        """
        return False


class CallbackChain(MessageContext):
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
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        callbacks: tuple[CallbackStructValue, ...] | None
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
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        callbacks: tuple[CallbackStructValue, ...] | None
    ) -> bool:
        if not super().is_applicable_path(subscription, publisher, callbacks):
            return False
        return self.callbacks == callbacks

    def to_dict(self) -> dict:
        d = super().to_dict()
        if self.callbacks is not None:
            d['callbacks'] = [_.callback_name for _ in self.callbacks]
        return d

    def verify(self) -> bool:
        is_valid = True
        if self.callbacks is None or len(self.callbacks) == 0:
            is_valid = False

            # Check binding between callback and publisher
            if not self._pub:
                logger.warning(
                    'callback-chain is empty. '
                    'The callback is not associated with the publisher. '
                )
            elif not self._pub.summary['callbacks']:
                logger.warning(
                    'callback-chain is empty. '
                    'The callback is not associated with the publisher. '
                    f'publisher topic name: {self.publisher_topic_name}'
                )
            else:
                logger.warning(
                    'callback-chain is empty. variable_passings are not set. '
                    f'node name: {self.node_name}')

        return is_valid


class Tilde(MessageContext):
    TYPE_NAME = 'tilde'

    """
    Tilde.

    Latency is calculated from tilde.

    """

    def __init__(
        self,
        node_name: str,
        message_context_dict: dict,
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        callbacks: tuple[CallbackStructValue, ...] | None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        message_context_dict : dict
            Message context dict.
        subscription : SubscriptionStructValue | None
            Target subscription value.
        publisher : PublisherStructValue | None
            Target publisher.
        callbacks : tuple[CallbackStructValue, ...] | None
            Callbacks.

        """
        super().__init__(node_name,
                         message_context_dict,
                         subscription,
                         publisher,
                         callbacks)

    @property
    def context_type(self) -> MessageContextType:
        """
        Get context type.

        Returns
        -------
        MessageContextType
            Message context type.

        """
        return MessageContextType.TILDE

    def is_applicable_path(
        self,
        subscription: SubscriptionStructValue | None,
        publisher: PublisherStructValue | None,
        callbacks: tuple[CallbackStructValue, ...] | None
    ) -> bool:
        """
        Get applicable path.

        Parameters
        ----------
        subscription : SubscriptionStructValue | None
            Target subscription value.
        publisher : PublisherStructValue | None
            Target publisher value.
        callbacks : tuple[CallbackStructValue, ...] | None
            Target callbacks.

        Returns
        -------
        bool
            True if applicable path, false otherwise.

        """
        if not super().is_applicable_path(subscription, publisher, callbacks):
            return False
        return True

    def verify(self) -> bool:
        """
        Get verify.

        Returns
        -------
        bool
            Verify or not.

        """
        return True
