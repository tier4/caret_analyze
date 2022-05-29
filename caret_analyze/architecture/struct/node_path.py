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
from typing import (
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from multimethod import multimethod as singledispatchmethod

from .callback_path import CallbackPathStruct
from .publisher import PublisherStruct
from .struct_interface import (
    NodeInputType,
    NodeOutputType,
    NodePathsStructInterface,
    NodePathStructInterface,
    NodeStructInterface,
    TransformFrameBroadcasterStructInterface,
    TransformFrameBufferStructInterface,
)
from .subscription import SubscriptionStruct
from .transform import (
    TransformFrameBroadcasterStruct,
    TransformFrameBufferStruct,
)
from ..reader_interface import ArchitectureReader
from ...exceptions import ItemNotFoundError
from ...value_objects import (
    MessageContext,
    NodePathStructValue,
    NodePathValue,
)

logger = getLogger(__name__)


class NodePathStruct(NodePathStructInterface):

    def __init__(
        self,
        node_name: str,
        node_input: Optional[NodeInputType],
        node_output: Optional[NodeOutputType],
        callback_path: Optional[CallbackPathStruct] = None,
        message_context: Optional[MessageContext] = None,
    ) -> None:
        self._node_name = node_name
        self._node_input = node_input
        self._node_output = node_output
        self._callback_path = callback_path
        self._message_context = message_context

    @property
    def publisher(self) -> Optional[PublisherStruct]:
        if isinstance(self._node_output, PublisherStruct):
            return self._node_output
        return None

    @property
    def node_input(self) -> Optional[NodeInputType]:
        return self._node_input

    @property
    def node_output(self) -> Optional[NodeOutputType]:
        return self._node_output

    @property
    def tf_frame_broadcaster(self) -> Optional[TransformFrameBroadcasterStructInterface]:
        if isinstance(self._node_output, TransformFrameBroadcasterStructInterface):
            return self._node_output
        return None

    @property
    def subscription(self) -> Optional[SubscriptionStruct]:
        if isinstance(self._node_input, SubscriptionStruct):
            return self._node_input
        return None

    @property
    def tf_frame_buffer(self) -> Optional[TransformFrameBufferStructInterface]:
        if isinstance(self._node_input, TransformFrameBufferStructInterface):
            return self._node_input
        return None

    def to_value(self) -> NodePathStructValue:
        assert self.node_name is not None
        # assert self.publisher is not None or self.tf_frame_broadcaster is not None
        if self.publisher is not None and self.publisher.topic_name == '/tf':
            assert None

        publisher = None if self.publisher is None else self.publisher.to_value()
        subscription = None if self.subscription is None else self.subscription.to_value()
        tf_frame_buffer = None if self.tf_frame_buffer is None else self.tf_frame_buffer.to_value()
        tf_frame_br = None if self.tf_frame_broadcaster is None \
            else self.tf_frame_broadcaster.to_value()
        msg_contxt = self.message_context
        child = self.callback_path.to_value() if self.callback_path is not None else None

        return NodePathStructValue(
            node_name=self.node_name,
            subscription=subscription,
            publisher=publisher,
            tf_frame_buffer=tf_frame_buffer,
            tf_frame_broadcaster=tf_frame_br,
            child=child,
            message_context=msg_contxt
        )

    @property
    def node_name(self) -> str:
        assert self._node_name is not None
        return self._node_name

    @property
    def message_context(self) -> Optional[MessageContext]:
        return self._message_context

    @message_context.setter
    def message_context(self, message_context: MessageContext):
        self._message_context = message_context

    @property
    def child(self) -> Optional[CallbackPathStruct]:
        return self.callback_path

    @property
    def callback_path(self) -> Optional[CallbackPathStruct]:
        return self._callback_path

    @callback_path.setter
    def callback_path(self, callback_path: CallbackPathStruct) -> None:
        self._callback_path = callback_path


class NodePathsStruct(NodePathsStructInterface, Iterable):

    def __init__(
        self,
    ) -> None:
        self._data: List[NodePathStruct] = []

    def insert(self, node_path: NodePathStruct) -> None:
        self._data.append(node_path)

    def to_value(self) -> Tuple[NodePathStructValue, ...]:
        return tuple(_.to_value() for _ in self._data)

    def __iter__(self) -> Iterator[NodePathStruct]:
        return iter(self._data)

    @singledispatchmethod
    def get(self, obj):
        raise NotImplementedError('Not implemented')

    @get.register
    def _get_struct(
        self,
        node_input: Optional[NodeInputType],
        node_output: Optional[NodeOutputType]
    ) -> NodePathStruct:
        node_name = None

        if isinstance(node_input, SubscriptionStruct):
            node_name = node_input.node_name if node_input is not None else None
        elif isinstance(node_input, TransformFrameBufferStruct):
            node_name = node_name or node_input.lookup_node_name \
                if node_input is not None else None
        if isinstance(node_output, PublisherStruct):
            node_name = node_name or node_output.node_name if node_output is not None else None
        elif isinstance(node_output, TransformFrameBroadcasterStruct):
            node_name = node_name or node_output.node_name if node_output is not None else None

        assert node_input is not None or node_output is not None
        assert node_name is not None

        for node_path in self:
            if node_path.node_input == node_input and \
                node_path.node_output == node_output and \
                    node_path.node_name == node_name:
                return node_path

        raise ItemNotFoundError('Failed to find node path')

    @get.register
    def _get_node_path(self, node_path: NodePathValue):
        return self._get_dict(
            node_name=node_path.node_name,
            subscribe_topic_name=node_path.subscribe_topic_name,
            publish_topic_name=node_path.publish_topic_name,
            broadcast_frame_id=node_path.broadcast_frame_id,
            broadcast_child_frame_id=node_path.broadcast_child_frame_id,
            buffer_listen_frame_id=node_path.buffer_listen_frame_id,
            buffer_listen_child_frame_id=node_path.buffer_listen_child_frame_id,
            buffer_lookup_frame_id=node_path.buffer_lookup_source_frame_id,
            buffer_lookup_child_frame_id=node_path.buffer_lookup_target_frame_id,
        )

    @get.register
    def _get_dict(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        broadcast_frame_id: Optional[str] = None,
        broadcast_child_frame_id: Optional[str] = None,
        buffer_listen_frame_id: Optional[str] = None,
        buffer_listen_child_frame_id: Optional[str] = None,
        buffer_lookup_frame_id: Optional[str] = None,
        buffer_lookup_child_frame_id: Optional[str] = None,
    ) -> NodePathStruct:
        # TODO(hsgwa): add assertion here.
        is_publisher = publish_topic_name is not None
        is_subscrioption = subscribe_topic_name is not None
        is_tf_br = broadcast_frame_id is not None or broadcast_child_frame_id is not None
        is_tf_buff = buffer_listen_frame_id is not None or buffer_listen_child_frame_id is not None

        for node_path in self:
            if node_path.node_name != node_name:
                continue

            node_out_matched = False
            node_in_matched = False

            if node_path.publisher is None and node_path.tf_frame_broadcaster is None and \
                    is_publisher is False and is_tf_br is False:
                node_out_matched = True

            if node_path.publisher is not None and \
                    node_path.publisher.topic_name == publish_topic_name:
                node_out_matched = True

            tf_br = node_path.tf_frame_broadcaster
            if tf_br is not None and \
                tf_br.frame_id == broadcast_frame_id and \
                    tf_br.child_frame_id == broadcast_child_frame_id:
                node_out_matched = True

            if node_path.subscription is None and node_path.tf_frame_broadcaster is None and \
                    is_subscrioption is False and is_tf_buff is False:
                node_in_matched = True

            if node_path.subscription is not None and \
                    node_path.subscription.topic_name == subscribe_topic_name:
                node_in_matched = True

            tf_buf = node_path.tf_frame_buffer
            if tf_buf is not None and \
                tf_buf.listen_frame_id == buffer_listen_frame_id and \
                    tf_buf.listen_child_frame_id == buffer_listen_child_frame_id and \
                    tf_buf.lookup_source_frame_id == buffer_lookup_frame_id and \
                    tf_buf.lookup_target_frame_id == buffer_lookup_child_frame_id:
                node_in_matched = True

            if node_in_matched and node_out_matched:
                return node_path

        raise ItemNotFoundError('Failed to find node path')

    def create(
        self,
        node_name: str,
        node_input: Optional[NodeInputType],
        node_output: Optional[NodeOutputType]
    ) -> None:
        node_path = NodePathStruct(node_name, node_input, node_output)
        self._data.append(node_path)

    @staticmethod
    def _search_node_paths(
        node: NodeStructInterface,
        reader: ArchitectureReader
    ) -> List[NodePathStruct]:
        raise NotImplementedError('')
