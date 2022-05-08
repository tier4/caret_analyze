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

from copy import deepcopy
from multimethod import multimethod as singledispatchmethod
from itertools import groupby, product

from typing import Optional, Sequence, Set, Tuple


from .publisher import PublisherStructValue, PublisherValue
from ..common.util import Util
from ..value_objects.value_object import ValueObject
from ..value_objects.subscription import SubscriptionStructValue
from ..value_objects.callback import SubscriptionCallbackStructValue
from ..exceptions import ItemNotFoundError, InvalidArgumentError


class TransformValue(ValueObject):
    def __init__(
        self,
        frame_id: str,
        child_frame_id: str
    ) -> None:
        self._frame_id = frame_id
        self._child_frame_id = child_frame_id

    @property
    def frame_id(self) -> str:
        return self._frame_id

    @property
    def child_frame_id(self) -> str:
        return self._child_frame_id


class TransformRecords():

    def to_dataframe(self):
        raise NotImplementedError('')


class TransformBroadcasterValue(ValueObject):
    def __init__(
        self,
        pub: PublisherValue,
        broadcast_transforms: Tuple[TransformValue, ...],
        callback_ids: Optional[Tuple[str, ...]],
    ) -> None:
        self._pub = pub
        self._send_transforms = broadcast_transforms
        self._callback_ids = callback_ids

    @property
    def broadcast_transforms(self) -> Sequence[TransformValue]:
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
        transforms: Sequence[TransformValue]
    ):
        self._pub = publisher
        self._transforms = transforms
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
        transform: TransformValue,
    ):
        self._transform = transform
        self._pub = publisher

    @property
    def publisher(self) -> PublisherStructValue:
        return self._pub

    @property
    def transform(self) -> TransformValue:
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
        listen_transforms: Optional[Tuple[TransformValue, ...]]
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
    def listen_transforms(self) -> Optional[Tuple[TransformValue, ...]]:
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
        listen_transform: TransformValue,
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
    def listen_transforms(self) -> TransformValue:
        return self._listen_transform


class TransformBufferStructValue(ValueObject):
    def __init__(
        self,
        lookup_node_name: str,
        listener_node_name: Optional[str],
        lookup_transforms: Tuple[TransformValue, ...],
        listen_transforms: Tuple[TransformValue, ...],
        listener_subscription: Optional[SubscriptionStructValue]
    ) -> None:
        self._lookup_node_name = lookup_node_name
        self._listener_node_name = listener_node_name
        self._lookup_transforms = lookup_transforms
        self._listen_transforms = listen_transforms
        self._frame_buffers: Optional[Tuple[TransformFrameBufferStructValue, ...]] = None
        tf_tree = TransformTreeValue.create_from_transforms(listen_transforms)
        frame_buffers = []
        for listen_transform, lookup_transform in product(listen_transforms, lookup_transforms):
            if tf_tree.is_in(lookup_transform, listen_transform):
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
    def listen_transforms(self) -> Tuple[TransformValue, ...]:
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
        listen_transform: TransformValue,
        lookup_transform: TransformValue
    ) -> TransformFrameBufferStructValue:
        return self._get_frame_buffer_dict(
            listen_transform.frame_id,
            listen_transform.child_frame_id,
            lookup_transform.frame_id,
            lookup_transform.child_frame_id
        )

    @get_frame_buffer.register
    def _get_frame_buffer_dict(
        self,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str
    ) -> TransformFrameBufferStructValue:
        for buff in self.frame_buffers or []:
            if buff.listen_frame_id == listen_frame_id and \
                    buff.listen_child_frame_id == listen_child_frame_id and \
                    buff.lookup_frame_id == lookup_frame_id and \
                    buff.lookup_child_frame_id == lookup_child_frame_id:
                return buff
        raise ItemNotFoundError('')


class TransformFrameBufferStructValue(ValueObject):
    def __init__(
        self,
        lookup_node_name: str,
        listener_node_name: Optional[str],
        listen_transform: TransformValue,
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
    def lookup_frame_id(self) -> str:
        return self._lookup_transform.frame_id

    @property
    def lookup_child_frame_id(self) -> str:
        return self._lookup_transform.child_frame_id

    @property
    def listen_transform(self) -> TransformValue:
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
    def listen_transform(self) -> TransformValue:
        return self.buffer.listen_transform

    @property
    def lookup_frame_id(self) -> str:
        return self.lookup_transform.frame_id

    @property
    def lookup_child_frame_id(self) -> str:
        return self.lookup_transform.child_frame_id

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
        frame: TransformValue,
        child_trees: Sequence[TransformTreeValue]
    ) -> None:
        self.root_tf = frame
        self._childs = child_trees

    @staticmethod
    def create_from_transforms(
        transforms: Sequence[TransformValue]
    ) -> TransformTreeValue:
        assert len(transforms) > 0

        edge_trees = None
        tfs = set(deepcopy(transforms))

        while(True):
            edge_tfs = set(TransformTreeValue._get_edge_tfs(list(tfs)))
            tfs -= edge_tfs
            if edge_trees is not None and len(tfs) == 0:
                assert len(edge_trees) == 1
                return edge_trees[0]
            if edge_trees is None:
                edge_trees = [TransformTreeValue(
                    edge_tf, []) for edge_tf in edge_tfs]

            parent_trees = []
            for key, group in groupby(edge_trees, lambda x: x.transform.child_frame_id):
                child_trees = list(group)
                parent_tf = Util.find_one(
                    lambda x: x.frame_id == key, transforms)
                parent_tree = TransformTreeValue(parent_tf, child_trees)
                parent_trees.append(parent_tree)
            edge_trees = parent_trees

    def is_in(
        self,
        lookup_transform: TransformValue,
        target_transform: TransformValue
    ) -> bool:
        lookup_frame_ids = self._to_root(lookup_transform.frame_id)
        lookup_child_frme_ids = self._to_root(lookup_transform.child_frame_id)
        frames = lookup_frame_ids ^ lookup_child_frme_ids
        ret = {target_transform} <= frames
        return ret

    def _to_root(self, target_tf: str) -> Set[TransformValue]:
        s: Set[TransformValue] = set()

        def find_coord_local(tree: TransformTreeValue) -> bool:
            if tree.root_tf.frame_id == target_tf:
                s.add(tree.root_tf)
                return True
            for child in tree.childs:
                if find_coord_local(child):
                    s.add(tree.root_tf)
                    return True
            return False

        find_coord_local(self)

        return s

    @property
    def childs(self) -> Sequence[TransformTreeValue]:
        return self._childs

    @property
    def transform(self) -> TransformValue:
        return self.root_tf

    @staticmethod
    def _get_root_tf(
        transforms: Sequence[TransformValue]
    ) -> TransformValue:
        tf_root = transforms[0]

        def is_root(tf_target: TransformValue):
            return not any([tf.frame_id == tf_target.child_frame_id for tf in transforms])

        while not is_root(tf_root):
            tf_root = Util.find_one(
                lambda tf: tf.frame_id == tf_root.child_frame_id, transforms)

        return tf_root

    @staticmethod
    def _get_edge_tfs(
        transforms: Sequence[TransformValue]
    ) -> Sequence[TransformValue]:

        def is_edge(tf_target: TransformValue):
            return not any([tf_target.frame_id == tf.child_frame_id for tf in transforms])

        return Util.filter_items(is_edge, transforms)
