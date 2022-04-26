
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
from typing import Dict, Optional, Tuple, Union

from .callback import CallbackStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .transform import TransformFrameBroadcasterStructValue, TransformFrameBufferStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary
from ..exceptions import UnsupportedTypeError

logger = getLogger(__name__)


NodeInType = Union[TransformFrameBufferStructValue, SubscriptionStructValue]
NodeOutType = Union[TransformFrameBroadcasterStructValue, PublisherStructValue]


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
        self._type_name = type_name

    @property
    def type_name(self) -> str:
        return self._type_name

    def __str__(self) -> str:
        return self.type_name


MessageContextType.USE_LATEST_MESSAGE = MessageContextType('use_latest_message')
MessageContextType.INHERIT_UNIQUE_STAMP = MessageContextType('inherit_unique_stamp')
MessageContextType.CALLBACK_CHAIN = MessageContextType('callback_chain')
MessageContextType.TILDE = MessageContextType('tilde')


class MessageContext(ValueObject, Summarizable):
    """Structured message context value."""

    def __init__(
        self,
        node_name: str,
        node_in: Optional[NodeInType],
        node_out: Optional[NodeOutType],
        child: Optional[Tuple[CallbackStructValue, ...]],
    ) -> None:
        # Since it is used as a value object,
        # mutable types such as dict should not be used.
        self._node_name = node_name
        self._node_in = node_in
        self._node_out = node_out
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
    def node_in(self) -> Optional[NodeInType]:
        return self._node_in

    @property
    def node_out(self) -> Optional[NodeOutType]:
        return self._node_out

    @property
    def publisher(self) -> Optional[PublisherStructValue]:
        if isinstance(self.node_out, PublisherStructValue):
            return self.node_out
        return None

    @property
    def subscription(self) -> Optional[SubscriptionStructValue]:
        if isinstance(self._node_in, SubscriptionStructValue):
            return self._node_in
        return None

    @property
    def callbacks(
        self
    ) -> Optional[Tuple[CallbackStructValue, ...]]:
        return self._callbacks

    def to_dict(self) -> Dict:
        # return {
        #     'context_type': str(self.type_name),
        #     'subscription_topic_name': self.subscription_topic_name,
        #     'publisher_topic_name': self.publisher_topic_name
        # }

        d: Dict[str, Optional[str]] = {
            'context_type': self.context_type.type_name,
        }
        d['subscription_topic_name'] = self.subscription_topic_name
        if self.tf_frame_buffer is not None:
            d['subscription_topic_name'] = '/tf'
            d['lookup_frame_id'] = self.tf_frame_buffer.lookup_frame_id
            d['lookup_child_frame_id'] = self.tf_frame_buffer.lookup_child_frame_id
            d['listen_frame_id'] = self.tf_frame_buffer.listen_frame_id
            d['listen_child_frame_id'] = self.tf_frame_buffer.listen_child_frame_id

        d['publisher_topic_name'] = self.publisher_topic_name
        if self.tf_frame_broadcaster is not None:
            d['publisher_topic_name'] = '/tf'
            d['broadcast_frame_id'] = self.tf_frame_broadcaster.frame_id
            d['broadcast_child_frame_id'] = self.tf_frame_broadcaster.child_frame_id

        return d

    # def is_applicable_path(
    #     self,
    #     subscription: Optional[SubscriptionStructValue],
    #     publisher: Optional[PublisherStructValue],
    #     callbacks: Optional[Tuple[CallbackStructValue, ...]]
    # ) -> bool:
    #     return self._sub == subscription and self._pub == publisher

    @property
    def tf_frame_broadcaster(
        self
    ) -> Optional[TransformFrameBroadcasterStructValue]:
        if isinstance(self.node_out, TransformFrameBroadcasterStructValue):
            return self.node_out
        return None

    @property
    def tf_frame_buffer(
        self
    ) -> Optional[TransformFrameBufferStructValue]:
        if isinstance(self.node_in, TransformFrameBufferStructValue):
            return self.node_in
        return None

    @property
    def publisher_topic_name(self) -> Optional[str]:
        if self.publisher is not None:
            return self.publisher.topic_name
        return None

    @property
    def subscription_topic_name(self) -> Optional[str]:
        if self.subscription is not None:
            return self.subscription.topic_name
        return None

    @property
    def summary(self) -> Summary:
        raise NotImplementedError('')
        # return Summary({
        #     'subscription_topic_name': self.subscription_topic_name,
        #     'publisher_topic_name': self.publisher_topic_name,
        #     'type': str(self.type_name)
        # })

    @abstractmethod
    def verify(self) -> bool:
        pass

    @staticmethod
    def create_instance(
        context_type_name: str,
        context_dict: Dict,
        node_name: str,
        node_in: Optional[NodeInType],
        node_out: Optional[NodeOutType],
        child: Optional[Tuple[CallbackStructValue, ...]]
    ) -> MessageContext:
        if context_type_name == str(MessageContextType.CALLBACK_CHAIN):
            assert isinstance(node_in, SubscriptionStructValue)
            assert isinstance(node_out, PublisherStructValue)
            return CallbackChain(node_name, context_dict, node_in, node_out, child)
        if context_type_name == str(MessageContextType.INHERIT_UNIQUE_STAMP):
            assert False, 'deprecated'
            # return InheritUniqueStamp(node_name, context_dict, subscription, publisher, child)
        if context_type_name == str(MessageContextType.USE_LATEST_MESSAGE):
            return UseLatestMessage(node_name, node_in, node_out, child)
        if context_type_name == str(MessageContextType.TILDE):
            return Tilde(node_name, context_dict, node_in, node_out, child)

        raise UnsupportedTypeError(
            f'Failed to load message context. message_context={context_type_name}')


class UseLatestMessage(MessageContext):
    TYPE_NAME = 'use_latest_message'

    """Use messsage context"""

    def verify(self) -> bool:
        return True

    @property
    def context_type(self) -> MessageContextType:
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
        return MessageContextType.INHERIT_UNIQUE_STAMP

    def verify(self) -> bool:
        pass


class CallbackChain(MessageContext):
    TYPE_NAME = 'callback_chain'

    """
    Callback chain.

    Latency is calculated from callback durations in the node path.
    When a path within a node passes through multiple callbacks,
    it is assumed that messages are passed between callbacks by a buffer of queue size 1
    (ex. a member variable that stores a single message).
    If the queue size is larger than 1, the node latency may be calculated to be small.

    """

    def __init__(
        self,
        node_name: str,
        message_context_dict: Dict,
        subscription: Optional[SubscriptionStructValue],
        publisher: Optional[PublisherStructValue],
        callbacks: Optional[Tuple[CallbackStructValue, ...]]
    ) -> None:
        super().__init__(node_name, subscription, publisher, callbacks)

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.CALLBACK_CHAIN

    # def is_applicable_path(
    #     self,
    #     subscription: Optional[SubscriptionStructValue],
    #     publisher: Optional[PublisherStructValue],
    #     callbacks: Optional[Tuple[CallbackStructValue, ...]]
    # ) -> bool:
    #     if not super().is_applicable_path(subscription, publisher, callbacks):
    #         return False
    #     return self.callbacks == callbacks

    def to_dict(self) -> Dict:
        d = super().to_dict()
        if self.callbacks is not None:
            d['callbacks'] = [_.callback_name for _ in self.callbacks]
        return d

    def verify(self) -> bool:
        is_valid = True
        if self.callbacks is None or len(self.callbacks) == 0:
            is_valid = False
            logger.warning(
                'callback-chain is empty. variable_passings may be not set.'
                f'{self.node_name}')

        return is_valid


class Tilde(MessageContext):
    TYPE_NAME = 'tilde'

    """
    tilde.

    Latency is calculated from tilde.

    """

    def __init__(
        self,
        node_name: str,
        message_context_dict: Dict,
        node_in: Optional[NodeInType],
        node_out: Optional[NodeOutType],
        callbacks: Optional[Tuple[CallbackStructValue, ...]]
    ) -> None:
        super().__init__(node_name, node_in, node_out, callbacks)

    @property
    def context_type(self) -> MessageContextType:
        return MessageContextType.TILDE

    # def is_applicable_path(
    #     self,
    #     subscription: Optional[SubscriptionStructValue],
    #     publisher: Optional[PublisherStructValue],
    #     callbacks: Optional[Tuple[CallbackStructValue, ...]]
    # ) -> bool:
    #     if not super().is_applicable_path(subscription, publisher, callbacks):
    #         return False
    #     return True

    def verify(self) -> bool:
        return True
