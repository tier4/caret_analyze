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

from .callback import CallbackStructValue
from .node import NodeStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class CommunicationStructValue(ValueObject, Summarizable):
    """Structured communication struct value."""

    def __init__(
        self,
        node_publish: NodeStructValue,
        node_subscription: NodeStructValue,
        publisher_value: PublisherStructValue,
        subscription_value: SubscriptionStructValue,
        publish_callback_values: tuple[CallbackStructValue, ...] | None,
        subscription_callback_value: CallbackStructValue | None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_publish : NodeStructValue
            Node struct value on the publish side.
        node_subscription : NodeStructValue
            Node struct value on the subscription side.
        publisher_value : PublisherStructValue
            Publisher struct value.
        subscription_value : SubscriptionStructValue
            Subscription struct value.
        publish_callback_values : tuple[CallbackStructValue, ...] | None
            Publisher callback struct values.
        subscription_callback_value : CallbackStructValue | None
            Subscription callback struct value.

        """
        self._publisher_value = publisher_value
        self._subscription_value = subscription_value
        self._topic_name = subscription_value.topic_name
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._subscription_callback_value = subscription_callback_value
        self._publish_callbacks_value = publish_callback_values

    @property
    def subscribe_node(self) -> NodeStructValue:
        """
        Get subscribe node.

        Returns
        -------
        NodeStructValue
            Node struct value on the subscription side.

        """
        return self._node_sub

    @property
    def publish_node(self) -> NodeStructValue:
        """
        Get publish node.

        Returns
        -------
        NodeStructValue
            Node struct value on the publish side.

        """
        return self._node_pub

    @property
    def topic_name(self) -> str:
        """
        Get topic name.

        Returns
        -------
        str
            Topic name.

        """
        return self._topic_name

    @property
    def publish_callback_names(self) -> tuple[str, ...] | None:
        """
        Get publish callback names.

        Returns
        -------
        tuple[str, ...] | None
            Publish callback names.

        """
        if self._publish_callbacks_value is None:
            return None
        return tuple(p.callback_name for p in self._publish_callbacks_value)

    @property
    def subscribe_callback_name(self) -> str | None:
        """
        Get subscribe callback names.

        Returns
        -------
        tuple[str, ...] | None
            Subscribe callback names.

        """
        if self._subscription_callback_value is None:
            return None
        return self._subscription_callback_value.callback_name

    @property
    def publisher(self) -> PublisherStructValue:
        """
        Get publisher struct value.

        Returns
        -------
        PublisherStructValue
            Publisher struct value.

        """
        return self._publisher_value

    @property
    def subscription(self) -> SubscriptionStructValue:
        """
        Get subscription struct value.

        Returns
        -------
        SubscriptionStructValue
            Subscription struct value.

        """
        return self._subscription_value

    @property
    def publish_callbacks(self) -> tuple[CallbackStructValue, ...] | None:
        """
        Get publish callback value.

        Returns
        -------
        tuple[CallbackStructValue, ...] | None
            Publish callback struct value.

        """
        return self._publish_callbacks_value

    @property
    def subscribe_callback(self) -> CallbackStructValue | None:
        """
        Get subscribe callback value.

        Returns
        -------
        tuple[CallbackStructValue, ...] | None
            Subscribe callback struct value.

        """
        return self._subscription_callback_value

    @property
    def subscribe_node_name(self) -> str:
        """
        Get subscribe node name.

        Returns
        -------
        str
            Subscribe node name.

        """
        return self._node_sub.node_name

    @property
    def publish_node_name(self) -> str:
        """
        Get publish node name.

        Returns
        -------
        str
            Publish node name.

        """
        return self._node_pub.node_name

    @property
    def publisher_construction_order(self) -> int:
        """
        Get publisher construction order.

        Returns
        -------
        int
            Publisher construction order.

        """
        return self._publisher_value.construction_order

    @property
    def subscription_construction_order(self) -> int:
        """
        Get subscription construction order.

        Returns
        -------
        int
            Subscription construction order.

        """
        return self._subscription_value.construction_order

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
            'topic_name': self.topic_name,
            'publish_node': self.publish_node_name,
            'subscribe_node': self.subscribe_node_name,
        })
