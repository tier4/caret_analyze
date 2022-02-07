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

from typing import Optional, Tuple

from .callback import CallbackStructValue
from .node import NodeStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary, Util


class CommunicationStructValue(ValueObject, Summarizable):

    def __init__(
        self,
        node_publish: NodeStructValue,
        node_subscription: NodeStructValue,
        publisher_value: PublisherStructValue,
        subscription_value: SubscriptionStructValue,
        publish_callback_values: Optional[Tuple[CallbackStructValue, ...]],
        subscription_callback_value: Optional[CallbackStructValue],
    ) -> None:
        self._publisher_value = publisher_value
        self._subscription_value = subscription_value
        self._topic_name = subscription_value.topic_name
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._subscription_callback_value = subscription_callback_value
        self._publish_callbacks_value = publish_callback_values

    def _find_publish_value(self, node: NodeStructValue, topic_name: str):
        def is_target(pub: PublisherStructValue):
            return pub.topic_name == topic_name
        return Util.find_one(is_target, node.publishers)

    def _find_subscription_value(self, node: NodeStructValue, topic_name: str):
        def is_target(sub: SubscriptionStructValue):
            return sub.topic_name == topic_name
        return Util.find_one(is_target, node.subscriptions)

    @property
    def subscribe_node(self) -> NodeStructValue:
        return self._node_sub

    @property
    def publish_node(self) -> NodeStructValue:
        return self._node_pub

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def publish_callback_names(self) -> Optional[Tuple[str, ...]]:
        if self._publish_callbacks_value is None:
            return None
        return tuple(p.callback_name for p in self._publish_callbacks_value)

    @property
    def subscribe_callback_name(self) -> Optional[str]:
        if self._subscription_callback_value is None:
            return None
        return self._subscription_callback_value.callback_name

    @property
    def publisher(self) -> PublisherStructValue:
        return self._publisher_value

    @property
    def subscription(self) -> SubscriptionStructValue:
        return self._subscription_value

    @property
    def publish_callbacks(self) -> Optional[Tuple[CallbackStructValue, ...]]:
        return self._publish_callbacks_value

    @property
    def subscribe_callback(self) -> Optional[CallbackStructValue]:
        return self._subscription_callback_value

    @property
    def subscribe_node_name(self) -> str:
        return self._node_sub.node_name

    @property
    def publish_node_name(self) -> str:
        return self._node_pub.node_name

    @property
    def summary(self) -> Summary:
        return Summary({
            'topic_name': self.topic_name,
            'publish_node': self.publish_node_name,
            'subscirbe_node': self.subscribe_node_name,
        })
