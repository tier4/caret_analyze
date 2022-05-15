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

from logging import getLogger
from typing import Any, Iterable, Iterator, Optional, Dict, Set

from caret_analyze.value_objects.callback import SubscriptionCallbackStructValue
from caret_analyze.value_objects.subscription import (
    IntraProcessBufferStructValue,
    SubscriptionValue,
)

from .struct_interface import (
    CallbacksStructInterface,
    CallbackStructInterface,
    PublisherStructInterface,
    SubscriptionStructInterface,
    SubscriptionsStructInterface,
)
from ...exceptions import ItemNotFoundError
from ...value_objects import (
    SubscriptionStructValue,
)

logger = getLogger(__name__)


class IntraProcessBufferStruct():

    def __init__(
        self,
        node_name: str,
        topic_name: str
    ) -> None:
        self._node_name = node_name
        self._topic_name = topic_name

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    def to_value(
        self,
    ) -> IntraProcessBufferStructValue:
        return IntraProcessBufferStructValue(
            self.node_name,
            self.topic_name
        )


class SubscriptionStruct(SubscriptionStructInterface):

    def __init__(
        self,
        callback_id: Optional[str],
        node_name: str,
        topic_name: str,
        buffer: Optional[IntraProcessBufferStruct],
    ) -> None:
        self._node_name: str = node_name
        self._topic_name: str = topic_name
        self._callback_id = callback_id
        self._is_transformed = False
        self._callback: Optional[CallbackStructInterface] = None
        self._buffer = buffer

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callback(self) -> Optional[CallbackStructInterface]:
        return self._callback

    @property
    def intra_process_buffer(self) -> Optional[IntraProcessBufferStruct]:
        return self._buffer

    def to_value(self) -> SubscriptionStructValue:
        self._is_transformed = True
        callback = None if self.callback is None else self.callback.to_value()
        assert callback is None or isinstance(callback, SubscriptionCallbackStructValue)
        buffer = None if self.intra_process_buffer is None \
            else self.intra_process_buffer.to_value()

        return SubscriptionStructValue(
            node_name=self.node_name,
            topic_name=self.topic_name,
            callback=callback,
            intra_process_buffer=buffer,
        )

    def is_pair(self, other: Any) -> bool:
        if isinstance(other, PublisherStructInterface):
            return other.topic_name == self.topic_name
        return False

    def assign_callback(self, callbacks: CallbacksStructInterface) -> None:
        if self._callback_id is not None:
            self._callback = callbacks.get_callback(self._callback_id)


class SubscriptionsStruct(SubscriptionsStructInterface, Iterable):
    def __init__(
        self,
    ) -> None:
        self._data: Dict[SubscriptionValue, SubscriptionStruct] = {}
        self._added_topics: Set[str] = set()
        self._is_transformed = False

    def __iter__(self) -> Iterator[SubscriptionStruct]:
        return iter(self._data.values())

    def add(self, subscription: SubscriptionStruct) -> None:
        assert self._is_transformed is False
        key = self._get_key(subscription.node_name, subscription.topic_name)
        if subscription.topic_name in self._added_topics:
            logger.warning('Topic %s already added', subscription.topic_name)
        self._added_topics.add(subscription.topic_name)
        self._data[key] = subscription

    def get(
        self,
        node_name: str,
        topic_name: str,
    ) -> SubscriptionStructInterface:
        key = self._get_key(node_name, topic_name)
        if key in self._data:
            return self._data[key]
        raise ItemNotFoundError('')

    def assign_callbacks(self, callbacks: CallbacksStructInterface):
        for sub in self:
            sub.assign_callback(callbacks)

    def _get_key(self, node_name: str, topic_name: str) -> SubscriptionValue:
        return SubscriptionValue(topic_name, node_name, None, None)
