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

from functools import lru_cache
from itertools import product
from typing import Dict, List, Optional, Sequence, Union

from . import Lttng
from .value_objects import (
    PublisherValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from ...architecture.reader_interface import IGNORE_TOPICS, ArchitectureReader
from ...value_objects import (
    ClientCallbackValue,
    ExecutorValue,
    NodeValue,
    PathValue,
    PublisherValue,
    ServiceCallbackValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    TransformTreeValue,
    TransformValue,
    UseLatestMessage,
    VariablePassingValue,
    CallbackGroupValue,
)


class ArchitectureReaderLttng(ArchitectureReader):
    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        self._lttng = lttng

    def get_nodes(self) -> Sequence[NodeValue]:
        return self._lttng.get_nodes()

    def _get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValueLttng]:
        return self._lttng.get_timer_callbacks(node)

    def _get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        return []

    def _get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[Dict]:

        class TfFrameBroadcasterLocal:

            def __init__(self, transform: TransformValue) -> None:
                self.transform = transform

        class TfFrameBufferLocal:

            def __init__(
                self,
                listen_transform: TransformValue,
                lookup_transform: TransformValue,
            ) -> None:
                self.listen_transform = listen_transform
                self.lookup_transform = lookup_transform

        NodeOutType = Union[PublisherValue, TfFrameBroadcasterLocal]
        NodeInType = Union[SubscriptionValue, TfFrameBufferLocal]

        node = self.get_node(node.node_name)
        node_inputs: List[NodeInType] = []
        node_outputs: List[NodeOutType] = []

        for pub in self.get_publishers(node.node_name):
            if pub.topic_name in IGNORE_TOPICS:
                continue
            if pub.topic_name == '/tf':
                continue
            if pub.topic_name.endswith('/info/pub'):
                continue
            node_outputs.append(pub)

        for sub in self.get_subscriptions(node.node_name):
            if sub.topic_name.endswith('/info/pub'):
                continue
            if sub.topic_name in IGNORE_TOPICS:
                continue
            node_inputs.append(sub)

        tf_br = self.get_tf_broadcaster(node.node_name)
        tf_buff = self.get_tf_buffer(node.node_name)

        if isinstance(tf_buff, TransformBufferValue) and tf_buff.lookup_transforms is not None:
            tf_frames = self.get_tf_frames()
            if len(tf_frames) > 0:
                tf_tree = TransformTreeValue.create_from_transforms(tf_frames)
                for listen_tf, lookup_tf in product(tf_frames, tf_buff.lookup_transforms):
                    if not tf_tree.is_in(lookup_tf, listen_tf):
                        continue
                    node_inputs.append(TfFrameBufferLocal(listen_tf, lookup_tf))

        if isinstance(tf_br, TransformBroadcasterValue):
            for br_tf in tf_br.broadcast_transforms:
                node_outputs.append(TfFrameBroadcasterLocal(br_tf))

        contexts = []
        for node_in, node_out in product(node_inputs, node_outputs):
            context = {'context_type': UseLatestMessage.TYPE_NAME}
            if isinstance(node_in, SubscriptionValue):
                context['subscription_topic_name'] = node_in.topic_name
            elif isinstance(node_in, TfFrameBufferLocal):
                context['subscription_topic_name'] = '/tf'
                context['lookup_frame_id'] = node_in.lookup_transform.frame_id
                context['lookup_child_frame_id'] = node_in.lookup_transform.child_frame_id
                context['listen_frame_id'] = node_in.listen_transform.frame_id
                context['listen_child_frame_id'] = node_in.listen_transform.child_frame_id

            if isinstance(node_out, PublisherValue):
                context['publisher_topic_name'] = node_out.topic_name
            elif isinstance(node_out, TfFrameBroadcasterLocal):
                context['publisher_topic_name'] = '/tf'
                context['broadcast_frame_id'] = node_out.transform.frame_id
                context['broadcast_child_frame_id'] = node_out.transform.child_frame_id

            contexts.append(context)

        return contexts

    def get_executors(
        self
    ) -> Sequence[ExecutorValue]:
        return self._lttng.get_executors()

    def _get_service_callbacks(self, node: NodeValue) -> Sequence[ServiceCallbackValue]:
        return self._lttng.get_service_callbacks(node)

    def _get_client_callbacks(self, node: NodeValue) -> Sequence[ClientCallbackValue]:
        return self._lttng.get_client_callbacks(node)

    def _get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        return self._lttng.get_subscription_callbacks(node)

    def _get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValueLttng]:
        return self._lttng.get_publishers(node)

    def _get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        return self._lttng.get_timers(node)

    def _get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        return self._lttng.get_callback_groups(node)

    def get_paths(
        self
    ) -> Sequence[PathValue]:
        return []

    @lru_cache
    def get_tf_frames(
        self
    ) -> Sequence[TransformValue]:
        return self._lttng.get_tf_frames()

    def _get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        info: List[SubscriptionValue] = []
        for sub_cb in self._get_subscription_callbacks(node):
            topic_name = sub_cb.subscribe_topic_name
            assert topic_name is not None
            info.append(SubscriptionValue(
                topic_name,
                sub_cb.node_name,
                sub_cb.node_id,
                sub_cb.callback_id
            ))
        return info

    def _get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValueLttng]:
        return self._lttng.get_tf_broadcaster(node)

    def _get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValueLttng]:
        return self._lttng.get_tf_buffer(node)
