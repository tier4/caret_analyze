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

from itertools import product
from typing import (
    Collection,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
)

from multimethod import multimethod as singledispatchmethod

from .publisher import PublisherStructValue, PublisherValue
from .subscription import (
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
)
from .value_object import ValueObject
from ..exceptions import (
    InvalidArgumentError,
    ItemNotFoundError,
)


class TransformValue(ValueObject):
    def __init__(
        self,
        source_frame_id: str,
        target_frame_id: str,
    ) -> None:
        self._source_frame_id = source_frame_id
        self._target_frame_id = target_frame_id

    @property
    def source_frame_id(self) -> str:
        return self._source_frame_id

    @property
    def target_frame_id(self) -> str:
        return self._target_frame_id


class BroadcastedTransformValue(TransformValue):

    def __init__(
        self,
        frame_id: str,
        child_frame_id: str
    ) -> None:
        super().__init__(frame_id, child_frame_id)

    @property
    def frame_id(self) -> str:
        return self.source_frame_id

    @property
    def child_frame_id(self) -> str:
        return self.target_frame_id


class TransformRecords():

    def to_dataframe(self):
        raise NotImplementedError('')


class TransformBroadcasterValue(ValueObject):
    def __init__(
        self,
        pub: PublisherValue,
        broadcast_transforms: Tuple[BroadcastedTransformValue, ...],
        callback_ids: Optional[Tuple[str, ...]],
    ) -> None:
        self._pub = pub
        self._send_transforms = broadcast_transforms
        self._callback_ids = callback_ids

    @property
    def broadcast_transforms(self) -> Sequence[BroadcastedTransformValue]:
        return self._send_transforms

    @property
    def publisher(self) -> PublisherValue:
        return self._pub

    @property
    def node_name(self) -> str:
        return self._pub.node_name

    @property
    def callback_ids(self) -> Optional[Tuple[str, ...]]:
        return self._callback_ids


class TransformBroadcasterStructValue(ValueObject):
    """Structured transform broadcaster value."""

    def __init__(
        self,
        publisher: PublisherStructValue,
        transforms: Sequence[BroadcastedTransformValue]
    ):
        self._pub = publisher
        self._transforms = tuple(transforms)
        frame_broadcaster = []
        for transform in self._transforms:
            frame_broadcaster.append(
                TransformFrameBroadcasterStructValue(
                    publisher,
                    transform
                )
            )
        self._frame_broadcasters = tuple(frame_broadcaster)

    @property
    def transforms(self) -> Sequence[TransformValue]:
        return self._transforms

    @property
    def topic_name(self) -> str:
        return self._pub.topic_name

    def get(self, transform: TransformValue):
        for frame_broadcaster in self._frame_broadcasters:
            if frame_broadcaster.transform == transform:
                return frame_broadcaster
        raise ItemNotFoundError('')

    @property
    def callback_ids(self) -> Optional[Tuple[str, ...]]:
        return self._pub.callback_ids

    @property
    def node_name(self) -> str:
        return self._pub.node_name

    @property
    def publisher(self) -> PublisherStructValue:
        return self._pub

    @property
    def frame_broadcasters(self) -> Tuple[TransformFrameBroadcasterStructValue, ...]:
        return self._frame_broadcasters


class TransformFrameBroadcasterStructValue(ValueObject):
    """Structured transform broadcaster value."""

    def __init__(
        self,
        publisher: PublisherStructValue,
        transform: BroadcastedTransformValue
    ):
        self._transform = transform
        self._pub = publisher

    @property
    def publisher(self) -> PublisherStructValue:
        return self._pub

    @property
    def transform(self) -> BroadcastedTransformValue:
        return self._transform

    @property
    def frame_id(self) -> str:
        return self._transform.frame_id

    @property
    def child_frame_id(self) -> str:
        return self._transform.child_frame_id

    @property
    def node_name(self) -> str:
        return self._pub.node_name

    @property
    def topic_name(self) -> str:
        return self._pub.topic_name


class TransformBufferValue(ValueObject):
    """transform buffer info."""

    def __init__(
        self,
        lookup_node_name: str,
        lookup_node_id: str,
        listener_node_name: Optional[str],
        listener_node_id: Optional[str],
        lookup_transforms: Optional[Tuple[TransformValue, ...]],
        listen_transforms: Optional[Tuple[BroadcastedTransformValue, ...]]
    ) -> None:
        self._listener_node_name = listener_node_name
        self._listener_node_id = listener_node_id
        self._lookup_node_name = lookup_node_name
        self._lookup_node_id = lookup_node_id
        self._lookup_transforms = lookup_transforms
        self._listen_transforms = listen_transforms

    @property
    def lookup_node_id(self) -> str:
        return self._lookup_node_id

    @property
    def lookup_node_name(self) -> str:
        return self._lookup_node_name

    @property
    def listener_node_id(self) -> Optional[str]:
        return self._listener_node_id

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    @property
    def lookup_transforms(self) -> Optional[Tuple[TransformValue, ...]]:
        return self._lookup_transforms

    @property
    def listen_transforms(self) -> Optional[Tuple[BroadcastedTransformValue, ...]]:
        return self._listen_transforms


class TransformFrameBufferValue(ValueObject):
    """transform buffer info."""

    def __init__(
        self,
        lookup_node_name: str,
        lookup_node_id: str,
        listener_node_name: Optional[str],
        listener_node_id: Optional[str],
        lookup_transform: TransformValue,
        listen_transform: BroadcastedTransformValue,
    ) -> None:
        self._listener_node_name = listener_node_name
        self._listener_node_id = listener_node_id
        self._lookup_node_name = lookup_node_name
        self._lookup_node_id = lookup_node_id
        self._lookup_transform = lookup_transform
        self._listen_transform = listen_transform

    @property
    def lookup_node_id(self) -> str:
        return self._lookup_node_id

    @property
    def lookup_node_name(self) -> str:
        return self._lookup_node_name

    @property
    def listener_node_id(self) -> Optional[str]:
        return self._listener_node_id

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    @property
    def lookup_transform(self) -> TransformValue:
        return self._lookup_transform

    @property
    def listen_transform(self) -> BroadcastedTransformValue:
        return self._listen_transform


class TransformBufferStructValue(ValueObject):
    def __init__(
        self,
        lookup_node_name: str,
        listener_node_name: Optional[str],
        lookup_transforms: Tuple[TransformValue, ...],
        listen_transforms: Tuple[BroadcastedTransformValue, ...],
        listener_subscription: Optional[SubscriptionStructValue]
    ) -> None:
        self._lookup_node_name = lookup_node_name
        self._listener_node_name = listener_node_name
        self._lookup_transforms = lookup_transforms
        self._listen_transforms = listen_transforms
        self._frame_buffers: Optional[Tuple[TransformFrameBufferStructValue, ...]] = None
        frame_buffers = []
        for listen_transform, lookup_transform in product(listen_transforms, lookup_transforms):
            frame_buffers.append(
                TransformFrameBufferStructValue(
                    lookup_node_name,
                    listener_node_name,
                    listen_transform,
                    lookup_transform,
                    listener_subscription
                )
            )
        self._frame_buffers = tuple(frame_buffers)

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    @property
    def lookup_node_name(self) -> str:
        return self._lookup_node_name

    @property
    def lookup_transforms(self) -> Tuple[TransformValue, ...]:
        return self._lookup_transforms

    @property
    def listen_transforms(self) -> Tuple[BroadcastedTransformValue, ...]:
        return self._listen_transforms

    @property
    def frame_buffers(self) -> Optional[Tuple[TransformFrameBufferStructValue, ...]]:
        return self._frame_buffers

    @singledispatchmethod
    def get_frame_buffer(self, arg) -> TransformFrameBroadcasterStructValue:
        raise InvalidArgumentError('')

    @get_frame_buffer.register
    def _get_frame_buffer_tf_value(
        self,
        listen_transform: BroadcastedTransformValue,
        lookup_transform: TransformValue
    ) -> TransformFrameBufferStructValue:
        return self._get_frame_buffer_dict(
            listen_transform.source_frame_id,
            listen_transform.target_frame_id,
            lookup_transform.source_frame_id,
            lookup_transform.target_frame_id
        )

    @get_frame_buffer.register
    def _get_frame_buffer_dict(
        self,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_source_frame_id: str,
        lookup_target_frame_id: str
    ) -> TransformFrameBufferStructValue:
        for buff in self.frame_buffers or []:
            if buff.listen_frame_id == listen_frame_id and \
                    buff.listen_child_frame_id == listen_child_frame_id and \
                    buff.lookup_source_frame_id == lookup_source_frame_id and \
                    buff.lookup_target_frame_id == lookup_target_frame_id:
                return buff
        raise ItemNotFoundError('')


class TransformFrameBufferStructValue(ValueObject):
    def __init__(
        self,
        lookup_node_name: str,
        listener_node_name: Optional[str],
        listen_transform: BroadcastedTransformValue,
        lookup_transform: TransformValue,
        listener_subscription: Optional[SubscriptionStructValue],
    ) -> None:
        self._lookup_node_name = lookup_node_name
        self._listener_node_name = listener_node_name
        self._listen_transform = listen_transform
        self._lookup_transform = lookup_transform
        self._listener_subscription = listener_subscription

    @property
    def lookup_node_name(self) -> str:
        return self._lookup_node_name

    @property
    def lookup_transform(self) -> TransformValue:
        return self._lookup_transform

    @property
    def lookup_source_frame_id(self) -> str:
        return self._lookup_transform.source_frame_id

    @property
    def lookup_target_frame_id(self) -> str:
        return self._lookup_transform.target_frame_id

    @property
    def listen_transform(self) -> BroadcastedTransformValue:
        return self._listen_transform

    @property
    def listen_frame_id(self) -> str:
        return self._listen_transform.frame_id

    @property
    def listen_child_frame_id(self) -> str:
        return self._listen_transform.child_frame_id

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    @property
    def topic_name(self) -> str:
        return '/tf'

    @property
    def listener_subscription(self) -> Optional[SubscriptionStructValue]:
        return self._listener_subscription

    @property
    def listener_subscription_callback(self) -> Optional[SubscriptionCallbackStructValue]:
        if self.listener_subscription is not None:
            return self.listener_subscription.callback
        return None


class TransformCommunicationStructValue():
    def __init__(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
        buffer: TransformFrameBufferStructValue,
    ) -> None:
        self._broadcaster = broadcaster
        self._buffer = buffer

    @property
    def broadcast_node_name(self) -> str:
        return self._broadcaster.node_name

    @property
    def publish_node_name(self) -> str:
        return self._broadcaster.node_name

    @property
    def subscribe_node_name(self) -> Optional[str]:
        return self._buffer.listener_node_name

    @property
    def lookup_node_name(self) -> str:
        return self._buffer.lookup_node_name

    @property
    def broadcaster(self) -> TransformFrameBroadcasterStructValue:
        return self._broadcaster

    @property
    def buffer(self) -> TransformFrameBufferStructValue:
        return self._buffer

    @property
    def topic_name(self) -> str:
        return self._broadcaster.topic_name

    @property
    def lookup_transform(self) -> TransformValue:
        return self.buffer.lookup_transform

    @property
    def listen_transform(self) -> BroadcastedTransformValue:
        return self.buffer.listen_transform

    @property
    def lookup_frame_id(self) -> str:
        return self.lookup_transform.source_frame_id

    @property
    def lookup_target_frame_id(self) -> str:
        return self.lookup_transform.target_frame_id

    @property
    def broadcast_transform(self) -> TransformValue:
        return self.broadcaster.transform

    @property
    def broadcast_frame_id(self) -> str:
        return self.broadcaster.transform.frame_id

    @property
    def broadcast_child_frame_id(self) -> str:
        return self.broadcaster.transform.child_frame_id


class TransformTreeValue():

    def __init__(
        self,
        transforms: Collection[TransformValue]
    ) -> None:
        self._to_parent: Dict[str, str] = {}

        for transform in transforms:
            self._to_parent[transform.source_frame_id] = transform.target_frame_id

    def is_in(
        self,
        lookup_transform: TransformValue,
        target_transform: TransformValue
    ) -> bool:
        lookup_frame_ids = self._to_root(lookup_transform.source_frame_id)
        lookup_child_frme_ids = self._to_root(lookup_transform.target_frame_id)
        frames = set(lookup_frame_ids) ^ set(lookup_child_frme_ids)
        frames_set = {frame.source_frame_id for frame in frames} | \
            {frame.target_frame_id for frame in frames}
        target_frame_set = {target_transform.source_frame_id, target_transform.target_frame_id}
        return target_frame_set <= frames_set

    def _to_root(self, frame_id: str) -> List[TransformValue]:
        s: List[TransformValue] = []

        if frame_id not in self._to_parent:
            return s

        child_frame_id = self._to_parent[frame_id]
        s.append(TransformValue(frame_id, child_frame_id))
        while True:
            if child_frame_id not in self._to_parent:
                break
            child_frame_id, frame_id = self._to_parent[child_frame_id], child_frame_id
            s.append(TransformValue(frame_id, child_frame_id))

        return s
