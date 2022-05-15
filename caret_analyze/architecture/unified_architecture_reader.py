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
from functools import lru_cache

from typing import Dict, Optional, Sequence, List

from caret_analyze.value_objects.node_path import NodePathValue

from .reader_interface import ArchitectureReader
from ..value_objects import (
    CallbackGroupValue,
    ClientCallbackValue,
    ExecutorValue,
    NodeValue,
    PathValue,
    PublisherValue,
    ServiceCallbackValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerCallbackValue,
    TimerValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    TransformValue,
    VariablePassingValue,
)


class UnifiedArchitectureReader(ArchitectureReader):
    def __init__(
        self,
        main: ArchitectureReader,
        sub: ArchitectureReader,
    ) -> None:
        self._main = main
        self._sub = sub
        self._sub_nodes = {node.node_name for node in sub.get_nodes()}
        self._main_node_id_map: Dict[str, str] = {}

    @lru_cache
    def get_nodes(
        self
    ) -> Sequence[NodeValue]:
        return self._main.get_nodes()

    def get_node(
        self,
        node_name: str
    ) -> NodeValue:
        return self._main.get_node(node_name)

    def get_tf_frames(self) -> Sequence[TransformValue]:
        return self._main.get_tf_frames()

    def _get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        return self._main._get_timer_callbacks(node)

    def _get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        return self._main._get_service_callbacks(node)

    def _get_client_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ClientCallbackValue]:
        return self._main._get_client_callbacks(node)

    def _get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        main_subs = self._main.get_subscription_callbacks(node.node_name)
        sub_node_names = {node.node_name for node in self._sub.get_nodes()}
        sub_subs: Optional[Dict] = None
        if node.node_name in sub_node_names:
            sub_subs = {
                cb.id_value: cb
                for cb
                in self._sub.get_subscription_callbacks(node.node_name)
            }

        subs = []
        for sub in main_subs:
            if sub_subs is not None and sub.id_value in sub_subs:
                publish_topic_names = sub_subs[sub.id_value].publish_topic_names
                callback_name = sub_subs[sub.id_value].callback_name
            else:
                publish_topic_names = sub.publish_topic_names
                callback_name = None

            subs.append(
                SubscriptionCallbackValue(
                    sub.callback_id,
                    node.node_name,
                    node.node_id,
                    sub.symbol,
                    sub.subscribe_topic_name,
                    publish_topic_names,
                    callback_name=callback_name
                )
            )
        return subs

    def _get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValue]:
        main_pubs = self._main._get_publishers(node)
        sub_pubs = {
            pub.id_value: pub
            for pub
            in self._sub.get_publishers(node.node_name)
        }

        pubs = []
        for pub in main_pubs:
            if pub.id_value in sub_pubs:
                callback_ids = sub_pubs[pub.id_value].callback_ids
            else:
                callback_ids = pub.callback_ids
            pubs.append(

                PublisherValue(
                    pub.topic_name,
                    pub.node_name,
                    pub.node_id,
                    callback_ids
                )
            )

        return pubs

    def _get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        return self._main._get_timers(node)

    def _get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        return self._main._get_subscriptions(node)

    def get_paths(self) -> Sequence[PathValue]:
        return self._sub.get_paths()

    def _get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[Dict]:
        if node.node_name in self._sub_nodes:
            return self._sub._get_message_contexts(node)
        return []

    def _get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        if node.node_name in self._sub_nodes:
            sub_var_passes = self._sub._get_variable_passings(node)
            for sub_var_pass in sub_var_passes:
                raise NotImplementedError('')
        return []

    def get_executors(self) -> Sequence[ExecutorValue]:
        return self._main.get_executors()

    def _get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        return self._main._get_callback_groups(node)

    def _get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValue]:
        return self._main._get_tf_buffer(node)

    def _get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValue]:
        return self._main._get_tf_broadcaster(node)
