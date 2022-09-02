from typing import Optional, Tuple

from .callback import CallbackStruct
from .node import NodeStruct
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from ...common import Summarizable, Summary, Util
from ...value_objects import CommunicationStructValue


class CommunicationStruct(Summarizable):

    def __init__(
        self,
        node_publish: NodeStruct,
        node_subscription: NodeStruct,
        publisher_value: PublisherStruct,
        subscription_value: SubscriptionStruct,
        publish_callback_values: Optional[Tuple[CallbackStruct, ...]],
        subscription_callback_value: Optional[CallbackStruct],
    ) -> None:
        self._publisher_value = publisher_value
        self._subscription_value = subscription_value
        self._topic_name = subscription_value.topic_name
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._subscription_callback_value = subscription_callback_value
        self._publish_callbacks_value = publish_callback_values

    def _find_publish_value(self, node: NodeStruct, topic_name: str):
        def is_target(pub: PublisherStruct):
            return pub.topic_name == topic_name
        return Util.find_one(is_target, node.publishers)

    def _find_subscription_value(self, node: NodeStruct, topic_name: str):
        def is_target(sub: SubscriptionStruct):
            return sub.topic_name == topic_name
        return Util.find_one(is_target, node.subscriptions)

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
    def publisher(self) -> PublisherStruct:
        return self._publisher_value

    @property
    def subscription(self) -> SubscriptionStruct:
        return self._subscription_value

    @property
    def publish_callbacks(self) -> Optional[Tuple[CallbackStruct, ...]]:
        return self._publish_callbacks_value

    @property
    def subscribe_callback(self) -> Optional[CallbackStruct]:
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

    def to_value(self) -> CommunicationStructValue:
        CommunicationStructValue(self.publish_node.to_value(), self.subscribe_node.to_value(), self.publisher.to_value(),
        self.subscription.to_value(), tuple(v.to_value() for v in self.publish_callbacks), tuple(v.to_value() for v in self.subscribe_callback))
