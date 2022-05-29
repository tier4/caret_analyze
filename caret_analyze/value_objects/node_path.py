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
from typing import Optional, Tuple, Union

from .callback import CallbackStructValue, SubscriptionCallbackStructValue
from .callback_path import CallbackPathStructValue
from .message_context import MessageContext, MessageContextType
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .transform import (
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
)
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue
from ..common import Summarizable, Summary, Util

logger = getLogger(__name__)

NodeOutputType = Union[TransformFrameBroadcasterStructValue, PublisherStructValue]
NodeInputType = Union[TransformFrameBufferStructValue, SubscriptionStructValue]


class NodePathValue(ValueObject):
    def __init__(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        broadcast_frame_id: Optional[str],
        broadcast_child_frame_id: Optional[str],
        buffer_listen_frame_id: Optional[str],
        buffer_listen_child_frame_id: Optional[str],
        buffer_lookup_source_frame_id: Optional[str],
        buffer_lookup_target_frame_id: Optional[str],
    ) -> None:
        self._node_name = node_name
        self._publish_topic_name = publish_topic_name
        self._subscribe_topic_name = subscribe_topic_name
        self._broadcast_frame_id = broadcast_frame_id
        self._broadcast_child_frame_id = broadcast_child_frame_id
        self._buffer_listen_frame_id = buffer_listen_frame_id
        self._buffer_listen_child_frame_id = buffer_listen_child_frame_id
        self._buffer_lookup_source_frame_id = buffer_lookup_source_frame_id
        self._buffer_lookup_target_frame_id = buffer_lookup_target_frame_id
        if buffer_lookup_source_frame_id is not None or buffer_lookup_target_frame_id is not None:
            assert buffer_lookup_source_frame_id is not None
            assert buffer_lookup_target_frame_id is not None
        # TODO(hsgwa) insert assertion here.

    @property
    def broadcast_frame_id(self) -> Optional[str]:
        return self._broadcast_frame_id

    @property
    def broadcast_child_frame_id(self) -> Optional[str]:
        return self._broadcast_child_frame_id

    @property
    def buffer_listen_frame_id(self) -> Optional[str]:
        return self._buffer_listen_frame_id

    @property
    def buffer_listen_child_frame_id(self) -> Optional[str]:
        return self._buffer_listen_child_frame_id

    @property
    def buffer_lookup_source_frame_id(self) -> Optional[str]:
        return self._buffer_lookup_source_frame_id

    @property
    def buffer_lookup_target_frame_id(self) -> Optional[str]:
        return self._buffer_lookup_target_frame_id

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def publish_topic_name(self) -> Optional[str]:
        return self._publish_topic_name

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self._subscribe_topic_name


class NodePathStructValue(ValueObject, Summarizable):

    def __init__(
        self,
        node_name: str,
        subscription: Optional[SubscriptionStructValue],
        publisher: Optional[PublisherStructValue],
        tf_frame_buffer: Optional[TransformFrameBufferStructValue],
        tf_frame_broadcaster: Optional[TransformFrameBroadcasterStructValue],
        child: Optional[CallbackPathStructValue],
        message_context: Optional[MessageContext],
    ) -> None:
        if tf_frame_broadcaster is not None:
            assert publisher is None

        self._node_name = node_name
        self._child = child
        self._subscription = subscription
        self._publisher = publisher
        self._context = message_context
        self._tf_frame_buffer = tf_frame_buffer
        self._tf_frame_broadcaster = tf_frame_broadcaster

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def callbacks(self) -> Optional[Tuple[CallbackStructValue, ...]]:
        if self._child is None:
            return None

        cb_values = Util.filter_items(
            lambda x: isinstance(x, CallbackStructValue),
            self._child
        )
        return tuple(cb_values)

    @property
    def tf_frame_buffer(self) -> Optional[TransformFrameBufferStructValue]:
        return self._tf_frame_buffer

    @property
    def tf_buffer_lookup_frame_id(self) -> Optional[str]:
        if self._tf_frame_buffer is None:
            return None
        return self._tf_frame_buffer.lookup_source_frame_id

    @property
    def tf_buffer_lookup_child_frame_id(self) -> Optional[str]:
        if self._tf_frame_buffer is None:
            return None
        return self._tf_frame_buffer.lookup_target_frame_id

    @property
    def tf_buffer_listen_frame_id(self) -> Optional[str]:
        if self._tf_frame_buffer is None:
            return None
        return self._tf_frame_buffer.listen_frame_id

    @property
    def tf_buffer_listen_child_frame_id(self) -> Optional[str]:
        if self._tf_frame_buffer is None:
            return None
        return self._tf_frame_buffer.listen_child_frame_id

    @property
    def tf_broadcast_frame_id(self) -> Optional[str]:
        if self._tf_frame_broadcaster is None:
            return None
        return self._tf_frame_broadcaster.frame_id

    @property
    def tf_broadcast_child_frame_id(self) -> Optional[str]:
        if self._tf_frame_broadcaster is None:
            return None
        return self._tf_frame_broadcaster.child_frame_id

    @property
    def tf_frame_broadcaster(self) -> Optional[TransformFrameBroadcasterStructValue]:
        return self._tf_frame_broadcaster

    @property
    def summary(self) -> Summary:
        context = None
        if self.message_context is not None:
            context = self.message_context.summary
        return Summary({
            'node': self.node_name,
            'message_context': context,
            'subscribe_topic_name': self.subscribe_topic_name,
            'publish_topic_name': self.publish_topic_name,
        })

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self.callbacks is None:
            return None

        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def variable_passings(self) -> Optional[Tuple[VariablePassingStructValue, ...]]:
        if self._child is None:
            return None

        cbs_info = Util.filter_items(
            lambda x: isinstance(x, VariablePassingStructValue),
            self._child
        )
        return tuple(cbs_info)

    @property
    def message_context(self) -> Optional[MessageContext]:
        return self._context

    @property
    def message_context_type(self) -> Optional[MessageContextType]:
        if self._context is None:
            return None

        return self._context.context_type

    @property
    def child(
        self,
    ) -> Optional[Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]]:
        if self._child is None:
            return None

        return tuple(self._child)

    @property
    def publisher(self) -> Optional[PublisherStructValue]:
        return self._publisher

    @property
    def subscription(self) -> Optional[SubscriptionStructValue]:
        return self._subscription

    @property
    def subscription_callback(self) -> Optional[SubscriptionCallbackStructValue]:
        if self._subscription is not None:
            return self._subscription.callback
        return None

    @property
    def publish_topic_name(self) -> Optional[str]:
        if self._publisher is not None:
            return self._publisher.topic_name

        if self._tf_frame_broadcaster is not None:
            return '/tf'

        return None

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        if self._subscription is not None:
            return self._subscription.topic_name

        if self._tf_frame_buffer is not None:
            return '/tf'

        return None
