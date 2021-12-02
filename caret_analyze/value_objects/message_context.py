
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
from typing import Dict, Optional, Tuple

from ..common import CustomDict
from .value_object import ValueObject
from ..exceptions import InvalidYamlFormatError
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue


class MessageContext(ValueObject):
    """Message context info."""
    pass

    @property
    @abstractmethod
    def type_name(self) -> str:
        pass

    @abstractmethod
    def to_dict(self) -> Dict:
        pass

    def __str__(self) -> str:
        return self.type_name

    @abstractmethod
    def is_applicable_path(
        self,
        subscription_info: SubscriptionStructValue,
        publisher_info: PublisherStructValue,
    ) -> bool:
        pass

    @abstractmethod
    def summary(self) -> CustomDict:
        pass

    @staticmethod
    def create_instance_from_dict(context_dict: Dict) -> MessageContext:
        context_type = context_dict['context_type']
        if context_type == UseLatestMessage.TYPE_NAME:
            return UseLatestMessage.create_instance_from_dict(context_dict)
        if context_type == InheritUniqueStamp.TYPE_NAME:
            return InheritUniqueStamp.create_instance_from_dict(context_dict)
        if context_type == CallbackChain.TYPE_NAME:
            return CallbackChain.create_instance_from_dict(context_dict)

        raise InvalidYamlFormatError(
            f'Failed to parse message context. {context_dict}')


class UseLatestMessage(MessageContext):
    TYPE_NAME = 'use_latest_message'

    """Use messsage context"""

    def __init__(
        self,
        subscription_topic_name: str,
        publisher_topic_name: str,
    ) -> None:
        self._sub_topic_name = subscription_topic_name
        self._pub_topic_name = publisher_topic_name

    @property
    def subscription_topic_name(self) -> str:
        return self._sub_topic_name

    @property
    def publisher_topic_name(self) -> str:
        return self._pub_topic_name

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    @property
    def summary(self) -> CustomDict:
        return CustomDict({
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name,
        })

    def is_applicable_path(
        self,
        subscription_value: SubscriptionStructValue,
        publisher_value: PublisherStructValue,
    ) -> bool:
        return subscription_value.topic_name == self._sub_topic_name and \
            publisher_value.topic_name == self._pub_topic_name

    def to_dict(self) -> CustomDict:
        return Custom({
            'context_type': self.TYPE_NAME,
            'subscription_topic_name': self.subscription_topic_name
        })

    @staticmethod
    def create_instance_from_dict(context_dict: Dict) -> MessageContext:
        return UseLatestMessage(
            context_dict['subscription_topic_name'],
            context_dict['publisher_topic_name'],
        )


class InheritUniqueStamp(MessageContext):
    TYPE_NAME = 'inherit_unique_stamp'

    """
    Inherit header timestamp.

    Latency is calculated for pub/sub messages with the same timestamp value.
    If the input timestamp is not unique, it may calculate an incorrect value.
    """

    def __init__(
        self,
        subscription_topic_name: str,
        publisher_topic_name: str,
    ) -> None:
        self._sub_topic_name = subscription_topic_name
        self._pub_topic_name = publisher_topic_name

    @property
    def publisher_topic_name(self) -> str:
        return self._pub_topic_name

    @property
    def subscription_topic_name(self) -> str:
        return self._sub_topic_name

    @property
    def summary(self) -> CustomDict:
        return CustomDict({
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name,
        })

    def is_applicable_path(
        self,
        subscription_value: SubscriptionStructValue,
        publisher_value: PublisherStructValue,
    ) -> bool:
        return subscription_value.topic_name == self._sub_topic_name and \
            publisher_value.topic_name == self._pub_topic_name

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    def to_dict(self) -> Dict:
        return {
            'context_type': self.TYPE_NAME,
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name
        }

    @staticmethod
    def create_instance_from_dict(context_dict: Dict) -> MessageContext:
        return InheritUniqueStamp(
            context_dict['subscription_topic_name'],
            context_dict['publisher_topic_name'],
        )


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
        subscription_topic_name: Optional[str],
        publisher_topic_name: Optional[str],
        callbacks: Tuple[str, ...]
    ) -> None:
        self._sub_topic_name = subscription_topic_name
        self._pub_topic_name = publisher_topic_name
        self._callbacks = callbacks

    @property
    def publisher_topic_name(self) -> Optional[str]:
        return self._pub_topic_name

    @property
    def callbacks(self) -> Tuple[str, ...]:
        return self._callbacks

    @property
    def subscription_topic_name(self) -> Optional[str]:
        return self._sub_topic_name

    @property
    def summary(self) -> CustomDict:
        return CustomDict({
            'subscription_topic_name': self.subscription_topic_name,
            'publisher_topic_name': self.publisher_topic_name,
            'callbacks': self.callbacks
        })

    def is_applicable_path(
        self,
        subscription_value: SubscriptionStructValue,
        publisher_value: PublisherStructValue,
    ) -> bool:
        return subscription_value.topic_name == self._sub_topic_name and \
            publisher_value.topic_name == self._pub_topic_name

    @property
    def type_name(self) -> str:
        return self.TYPE_NAME

    def to_dict(self) -> Dict:
        return {
            'context_type': self.TYPE_NAME,
        }

    @staticmethod
    def create_instance_from_dict(context_dict: Dict) -> MessageContext:
        return CallbackChain(
            context_dict['subscription_topic_name'],
            context_dict['publisher_topic_name'],
        )
