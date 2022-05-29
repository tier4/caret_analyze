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
from typing import Any, Iterable, Iterator, List, Optional

from .publisher import PublisherStruct
from .struct_interface import (
    SubscriptionStructInterface,
    TransformBroadcasterStructInterface,
    TransformBufferStructInterface,
    TransformFrameBroadcasterStructInterface,
    TransformFrameBufferStructInterface,
)
from ...exceptions import ItemNotFoundError
from ...value_objects import (
    BroadcastedTransformValue,
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    TransformTreeValue,
    TransformValue,
)


class TransformFrameBroadcasterStruct(TransformFrameBroadcasterStructInterface):
    def __init__(
        self,
        publisher: Optional[PublisherStruct] = None,
        transform: Optional[BroadcastedTransformValue] = None,
    ) -> None:
        self._publisher = publisher
        self._transform = transform

    def to_value(
        self
    ) -> TransformFrameBroadcasterStructValue:
        return TransformFrameBroadcasterStructValue(
            publisher=self.publisher.to_value(),
            transform=self.transform,
        )

    @property
    def node_name(self) -> str:
        return self.publisher.node_name

    def is_pair(self, other: Any) -> bool:
        if isinstance(other, TransformFrameBufferStructInterface):
            other.is_pair(self)
        return False

    @property
    def publisher(self) -> PublisherStruct:
        assert self._publisher is not None
        return self._publisher

    @property
    def frame_id(self) -> str:
        return self.transform.source_frame_id

    @property
    def child_frame_id(self) -> str:
        return self.transform.target_frame_id

    @property
    def transform(self) -> BroadcastedTransformValue:
        assert self._transform is not None
        return self._transform


class TransformFrameBufferStruct(TransformFrameBufferStructInterface):

    def __init__(
        self,
        tf_tree: TransformTreeValue,
        listener_subscription: Optional[SubscriptionStructInterface],
        lookup_transform: Optional[TransformValue] = None,
        listen_transform: Optional[BroadcastedTransformValue] = None,
        lookup_node_name: Optional[str] = None,
        listener_node_name: Optional[str] = None,
    ) -> None:
        assert isinstance(tf_tree, TransformTreeValue)

        self._lookup_transform = lookup_transform
        self._listen_transform = listen_transform
        self._lookup_node_name = lookup_node_name
        self._listener_node_name = listener_node_name
        self._tf_tree = tf_tree
        self._listener_subscription = listener_subscription

    @property
    def lookup_source_frame_id(self) -> str:
        return self.lookup_transform.source_frame_id

    @property
    def lookup_target_frame_id(self) -> str:
        return self.lookup_transform.target_frame_id

    @property
    def listen_frame_id(self) -> str:
        return self.listen_transform.source_frame_id

    @property
    def listen_child_frame_id(self) -> str:
        return self.listen_transform.target_frame_id

    def to_value(self) -> TransformFrameBufferStructValue:
        listener_subscription = None if self.listener_subscription is None \
            else self.listener_subscription.to_value()
        return TransformFrameBufferStructValue(
            lookup_node_name=self.lookup_node_name,
            listener_node_name=self.listener_node_name,
            lookup_transform=self.lookup_transform,
            listen_transform=self.listen_transform,
            listener_subscription=listener_subscription,
        )

    def is_pair(self, other: Any) -> bool:
        if isinstance(other, TransformFrameBroadcasterStruct):
            return self._tf_tree.is_in(self.lookup_transform, other.transform) and \
                self.listen_transform == other.transform
        return False

    @property
    def lookup_node_name(self) -> str:
        assert self._lookup_node_name is not None
        return self._lookup_node_name

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    @property
    def listener_subscription(self) -> Optional[SubscriptionStructInterface]:
        return self._listener_subscription

    # @property
    # def publisher(self) -> PublisherStruct:
    #     assert self._publisher is not None
    #     return self._publisher

    @property
    def lookup_transform(self) -> TransformValue:
        assert self._lookup_transform is not None
        return self._lookup_transform

    @property
    def listen_transform(self) -> BroadcastedTransformValue:
        assert self._listen_transform is not None
        return self._listen_transform


class TransformFrameBroadcastersStruct(Iterable):

    def __init__(self) -> None:
        self._data: List[TransformFrameBroadcasterStruct] = []

    def as_list(self) -> List[TransformFrameBroadcasterStruct]:
        return list(self._data)

    def add(self, brs: TransformFrameBroadcastersStruct) -> None:
        for br in brs:
            self.insert(br)

    def __iter__(self) -> Iterator[TransformFrameBroadcasterStruct]:
        return iter(self._data)

    def insert(self, br: TransformFrameBroadcasterStruct) -> None:
        self._data.append(br)

    def get(
        self,
        node_name: str,
        frame_id: str,
        child_frame_id: str,
    ) -> TransformFrameBroadcasterStruct:
        for br in self:
            if br.node_name == node_name and \
                br.frame_id == frame_id and \
                    br.child_frame_id == child_frame_id:
                return br
        raise NotImplementedError('')


class TransformFrameBuffersStruct(Iterable):

    def __init__(self) -> None:
        self._data: List[TransformFrameBufferStruct] = []

    def add(self, brs: TransformFrameBuffersStruct) -> None:
        for br in brs:
            self.insert(br)

    def __iter__(self) -> Iterator[TransformFrameBufferStruct]:
        return iter(self._data)

    def as_list(self) -> List[TransformFrameBufferStruct]:
        return self._data

    def get(
        self,
        node_name: str,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str,
    ) -> TransformFrameBufferStruct:
        for buf in self:
            if buf.lookup_node_name == node_name and \
                buf.lookup_source_frame_id == lookup_frame_id and \
                    buf.lookup_target_frame_id == lookup_child_frame_id and \
                    buf.listen_frame_id == listen_frame_id and \
                    buf.listen_child_frame_id == listen_child_frame_id:
                return buf
        raise ItemNotFoundError(
            f'{node_name}: {lookup_frame_id}:{lookup_child_frame_id}:'
            f' {listen_frame_id}:{listen_child_frame_id}'
        )

    def insert(self, br: TransformFrameBufferStruct) -> None:
        self._data.append(br)


class TransformBroadcasterStruct(TransformBroadcasterStructInterface):
    def __init__(
        self,
        publisher: PublisherStruct,
        transforms: List[BroadcastedTransformValue],
    ) -> None:
        self._publisher = publisher
        self._transforms = transforms
        self._frame_brs = TransformFrameBroadcastersStruct()

        for tf in transforms:
            frame_br = TransformFrameBroadcasterStruct(
                publisher=publisher,
                transform=tf,
            )
            self._frame_brs.insert(frame_br)

    def to_value(self) -> Optional[TransformBroadcasterStructValue]:
        return TransformBroadcasterStructValue(
            publisher=self.publisher.to_value(),
            transforms=self.transforms
        )

    def get(
        self,
        frame_id: str,
        child_frame_id: str
    ) -> TransformFrameBroadcasterStructInterface:
        return self.frame_broadcasters.get(
            self.publisher.node_name, frame_id, child_frame_id)

    @property
    def publisher(self) -> PublisherStruct:
        assert self._publisher is not None
        return self._publisher

    @property
    def transforms(self) -> List[BroadcastedTransformValue]:
        assert self._transforms is not None
        return self._transforms

    @property
    def frame_broadcasters(self) -> TransformFrameBroadcastersStruct:
        return self._frame_brs


class TransformBufferStruct(TransformBufferStructInterface):

    def __init__(
        self,
        lookup_node_name: str,
        tf_tree: TransformTreeValue,
        lookup_transforms: List[TransformValue],
        listen_transforms: List[BroadcastedTransformValue],
        listener_subscription: Optional[SubscriptionStructInterface],
        listener_node_name: Optional[str] = None,
    ) -> None:
        self._lookup_node_name = lookup_node_name
        self._listener_node_name = listener_node_name
        self._lookup_transforms = lookup_transforms
        self._listen_transforms = listen_transforms
        self._frame_buffs = TransformFrameBuffersStruct()
        self._listener_subscription = listener_subscription
        tf_tree = TransformTreeValue(listen_transforms)
        for listen_tf, lookup_tf in product(listen_transforms, lookup_transforms):
            # if not tf_tree.is_in(lookup_tf, listen_tf):
            #     continue
            buf = TransformFrameBufferStruct(
                tf_tree=tf_tree,
                listener_subscription=listener_subscription,
                lookup_node_name=lookup_node_name,
                listener_node_name=listener_node_name,
                lookup_transform=lookup_tf,
                listen_transform=listen_tf
            )
            self._frame_buffs.insert(buf)

    @property
    def lookup_node_name(self) -> str:
        assert self._lookup_node_name is not None
        return self._lookup_node_name

    @property
    def lookup_transforms(self) -> List[TransformValue]:
        assert self._lookup_transforms is not None
        return self._lookup_transforms

    @property
    def listen_transforms(self) -> List[BroadcastedTransformValue]:
        assert self._listen_transforms is not None
        return self._listen_transforms

    @property
    def listener_node_name(self) -> Optional[str]:
        return self._listener_node_name

    def is_pair(self, other: Any) -> bool:
        if isinstance(other, TransformBroadcasterStruct):
            return True
        return False

    def get(
        self,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_source_frame_id: str,
        lookup_target_frame_id: str,
    ) -> TransformFrameBufferStructInterface:
        return self.frame_buffers.get(
            self.lookup_node_name,
            listen_frame_id,
            listen_child_frame_id,
            lookup_source_frame_id,
            lookup_target_frame_id)

    def to_value(self) -> TransformBufferStructValue:
        lookup_transforms = tuple(self.lookup_transforms)
        listen_transforms = tuple(self.listen_transforms)
        listener_subscription = None
        if self.listener_subscription is not None:
            listener_subscription = self.listener_subscription.to_value()

        return TransformBufferStructValue(
            lookup_node_name=self.lookup_node_name,
            listener_node_name=self.listener_node_name,
            lookup_transforms=lookup_transforms,
            listen_transforms=listen_transforms,
            listener_subscription=listener_subscription,
        )

    @property
    def listener_subscription(self) -> Optional[SubscriptionStructInterface]:
        return self._listener_subscription

    @property
    def frame_buffers(self) -> TransformFrameBuffersStruct:
        return self._frame_buffs
