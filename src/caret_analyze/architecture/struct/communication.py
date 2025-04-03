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

from .callback import CallbackStruct
from .node import NodeStruct
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from ...value_objects import CommunicationStructValue


class CommunicationStruct():

    def __init__(
        self,
        node_publish: NodeStruct,
        node_subscription: NodeStruct,
        publisher_value: PublisherStruct,
        subscription_value: SubscriptionStruct,
        publish_callback_values: list[CallbackStruct] | None,
        subscription_callback_value: CallbackStruct | None,
    ) -> None:
        self._publisher_value = publisher_value
        self._subscription_value = subscription_value
        self._topic_name = subscription_value.topic_name
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._subscription_callback_value = subscription_callback_value
        self._publish_callbacks_value = publish_callback_values

    @property
    def subscribe_node(self) -> NodeStruct:
        return self._node_sub

    @property
    def publish_node(self) -> NodeStruct:
        return self._node_pub

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def publish_callback_names(self) -> list[str] | None:
        if self._publish_callbacks_value is None:
            return None
        return [p.callback_name for p in self._publish_callbacks_value]

    @property
    def subscribe_callback_name(self) -> str | None:
        if self._subscription_callback_value is None:
            return None
        return self._subscription_callback_value.callback_name

    @property
    def publisher(self) -> PublisherStruct:
        return self._publisher_value

    @property
    def subscription(self) -> SubscriptionStruct:
        return self._subscription_value

    @property
    def publisher_construction_order(self) -> int | None:
        if self.publisher:
            return self.publisher.construction_order
        return None

    @property
    def subscription_construction_order(self) -> int | None:
        if self.subscription:
            return self.subscription.construction_order
        return None

    @property
    def publish_callbacks(self) -> list[CallbackStruct] | None:
        return self._publish_callbacks_value

    @property
    def subscribe_callback(self) -> CallbackStruct | None:
        return self._subscription_callback_value

    @property
    def subscribe_node_name(self) -> str:
        return self._node_sub.node_name

    @property
    def publish_node_name(self) -> str:
        return self._node_pub.node_name

    def to_value(self) -> CommunicationStructValue:
        """
        Get Communication struct value.

        Returns
        -------
        CommunicationStructValue
            Communication struct value instance.

        """
        return CommunicationStructValue(
            self.publish_node.to_value(),
            self.subscribe_node.to_value(),
            self.publisher.to_value(),
            self.subscription.to_value(),
            None if self.publish_callbacks is None
            else tuple(v.to_value() for v in self.publish_callbacks),
            None if self.subscribe_callback is None
            else self.subscribe_callback.to_value())

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
        self._node_pub.rename_node(src, dst)
        self._node_sub.rename_node(src, dst)
        self._publisher_value.rename_node(src, dst)
        self._subscription_value.rename_node(src, dst)
        if self._publish_callbacks_value:
            for p in self._publish_callbacks_value:
                p.rename_node(src, dst)
        if self._subscription_callback_value is not None:
            self._subscription_callback_value.rename_node(src, dst)

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
        if self.topic_name == src:
            self._topic_name = dst

        self._node_pub.rename_topic(src, dst)
        self._node_sub.rename_topic(src, dst)
        self._publisher_value.rename_topic(src, dst)
        self._subscription_value.rename_topic(src, dst)
        if self._publish_callbacks_value:
            for p in self._publish_callbacks_value:
                p.rename_topic(src, dst)
        if self._subscription_callback_value is not None:
            self._subscription_callback_value.rename_topic(src, dst)
