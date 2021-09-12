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

from typing import List, Optional
import itertools

from caret_analyze.record.interface import (
    SubscriptionCallbackInterface,
    TimerCallbackInterface,
)

from caret_analyze.communication import Communication, VariablePassing
from .interface import ArchitectureImporter, PathAlias, IGNORE_TOPICS
from caret_analyze.record import AppInfoGetter, LatencyComposer
from caret_analyze.pub_sub import Publisher, Subscription
from caret_analyze.node import Node
from caret_analyze.callback import TimerCallback, SubscriptionCallback, CallbackBase
from caret_analyze.record.lttng import Lttng
from caret_analyze.util import Util


class LttngArchitectureImporter(ArchitectureImporter):
    def __init__(self, latency_composer: Optional[LatencyComposer]) -> None:
        self._latency_composer = latency_composer
        self._nodes: List[Node] = []
        self._path_aliases: List[PathAlias] = []
        self._communications: List[Communication] = []
        self._variable_passings: List[VariablePassing] = []

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    @property
    def path_aliases(self) -> List[PathAlias]:
        return self._path_aliases

    @property
    def communications(self) -> List[Communication]:
        return self._communications

    @property
    def variable_passings(self) -> List[VariablePassing]:
        return self._variable_passings

    @property
    def latency_composer(self):
        return self._latency_composer

    def _find_callback(self, node_name: str, callback_name: str) -> Optional[CallbackBase]:
        for node in self._nodes:
            for callback in node.callbacks:
                if callback.node_name == node_name and callback.callback_name == callback_name:
                    return callback
        return None

    def _create_communicateion(
        self, publish: Publisher, subscription: Subscription
    ) -> Optional[Communication]:
        publish_callback_name = publish.callback_name or ""
        callback_publish = self._find_callback(publish.node_name, publish_callback_name)
        callback_subscription = self._find_callback(
            subscription.node_name, subscription.callback_name
        )

        if callback_subscription is None:
            return None
        if not isinstance(callback_subscription, SubscriptionCallback):
            return None

        return Communication(
            self._latency_composer, callback_publish, callback_subscription, publish
        )

    def exec(self, path: str, ignore_topics: Optional[List[str]] = None) -> None:
        ignore_topics = ignore_topics or IGNORE_TOPICS
        container = Lttng(path)
        for node_name in container.get_node_names():
            node = self._create_node(node_name, container, ignore_topics)
            self._nodes.append(node)

        publishes = Util.flatten([node.publishes for node in self._nodes])
        subscriptions = Util.flatten([node.subscriptions for node in self._nodes])
        for publish, subscription in itertools.product(publishes, subscriptions):
            if publish.topic_name != subscription.topic_name:
                continue
            communication = self._create_communicateion(publish, subscription)
            if communication is None:
                continue
            self._communications.append(communication)

        return None

    def _to_timer_callback(self, timer_callback: TimerCallbackInterface):
        return TimerCallback(
            self._latency_composer,
            timer_callback.node_name,
            timer_callback.callback_name,
            timer_callback.symbol,
            timer_callback.period_ns,
        )

    def _to_subscription_callback(self, subscription_callback: SubscriptionCallbackInterface):
        return SubscriptionCallback(
            self._latency_composer,
            subscription_callback.node_name,
            subscription_callback.callback_name,
            subscription_callback.symbol,
            subscription_callback.topic_name,
        )

    def _create_node(self, node_name, container: AppInfoGetter, ignore_topics=[]) -> Node:

        node: Node = Node(node_name)

        for subscription_callback in container.get_subscription_callbacks(
            node_name=node.node_name
        ):
            if subscription_callback.topic_name in ignore_topics:
                continue
            node.callbacks.append(self._to_subscription_callback(subscription_callback))

        for timer_callback in container.get_timer_callbacks(node_name=node.node_name):
            node.callbacks.append(self._to_timer_callback(timer_callback))

        publish_topic_names = [_.topic_name for _ in container.get_publishers(node_name)]
        for pub_topic in publish_topic_names:
            if pub_topic in ignore_topics:
                continue

            if len(node.callbacks) == 1:
                # automatically assign if there is only one callback.
                callback = node.callbacks[0]
                publisher = Publisher(node_name, pub_topic, callback.callback_name)
                callback.publishes.append(publisher)
                continue

            publisher = Publisher(node_name, pub_topic, None)
            node.unlinked_publishes.append(publisher)

        return node
