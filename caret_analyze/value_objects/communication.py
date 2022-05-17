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

from .callback import SubscriptionCallbackStructValue
from .node import NodeStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .value_object import ValueObject
from ..common import Summary, Util


class CommunicationStructValue(ValueObject):

    def __init__(
        self,
        node_publish: NodeStructValue,
        node_subscription: NodeStructValue,
        publisher_value: PublisherStructValue,
        subscription_value: SubscriptionStructValue,
    ) -> None:
        self._publisher_value = publisher_value
        self._subscription_value = subscription_value
        self._topic_name = subscription_value.topic_name
        self._node_pub = node_publish
        self._node_sub = node_subscription

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
    def publisher(self) -> PublisherStructValue:
        return self._publisher_value

    @property
    def subscription(self) -> SubscriptionStructValue:
        return self._subscription_value

    @property
    def subscribe_node_name(self) -> str:
        return self._node_sub.node_name

    @property
    def subscription_callback_name(self) -> str:
        return self.subscription_callback.callback_name

    @property
    def subscription_callback(self) -> SubscriptionCallbackStructValue:
        assert self._subscription_value.callback is not None
        return self._subscription_value.callback

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
