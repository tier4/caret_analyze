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

from typing import Tuple, List, Union
from itertools import product
from logging import getLogger
from multimethod import multimethod as singledispatchmethod

from .transform import (
    TransformFrameBroadcasterStruct,
    TransformFrameBufferStruct,
)
from .callback import CallbackStruct
from .node import NodeStruct, NodesStruct
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from ...common import Util
from ...exceptions import InvalidArgumentError, ItemNotFoundError
from ...common import Progress
from ...value_objects import (
    CommunicationStructValue,
    TransformCommunicationStructValue,
    NodePathValue,
)


logger = getLogger(__name__)

CommunicationType = Union[CommunicationStructValue, TransformCommunicationStructValue]


class CommunicationStruct():

    def __init__(
        self,
        nodes: NodesStruct,
        pub: PublisherStruct,
        sub: SubscriptionStruct,
        node_pub: NodeStruct,
        node_sub: NodeStruct,
    ) -> None:
        self._sub = sub
        self._pub = pub
        self._node_pub = node_pub
        self._node_sub = node_sub

    @property
    def node_pub(self) -> NodeStruct:
        return self._node_pub

    @property
    def node_sub(self) -> NodeStruct:
        return self._node_sub

    @property
    def publisher(self) -> PublisherStruct:
        return self._pub

    @property
    def publish_node_name(self) -> str:
        return self.node_pub.node_name

    @property
    def subscription(self) -> SubscriptionStruct:
        return self._sub

    @property
    def subscribe_node_name(self) -> str:
        return self.node_sub.node_name

    @property
    def topic_name(self) -> str:
        return self._pub.topic_name

    def to_value(self) -> Union[CommunicationStructValue, TransformCommunicationStructValue]:

        return CommunicationStructValue(
            self.node_pub.to_value(),
            self.node_sub.to_value(),
            self.publisher.to_value(),
            self.subscription.to_value())


class TransformCommunicationStruct():
    def __init__(
        self,
        broadcaster: TransformFrameBroadcasterStruct,
        buffer: TransformFrameBufferStruct,
    ) -> None:
        self._broadcaster = broadcaster
        self._buffer = buffer

    @property
    def buffer(self) -> TransformFrameBufferStruct:
        return self._buffer

    @property
    def broadcaster(self) -> TransformFrameBroadcasterStruct:
        return self._broadcaster

    def to_value(self) -> TransformCommunicationStructValue:
        return TransformCommunicationStructValue(
            broadcaster=self.broadcaster.to_value(),
            buffer=self.buffer.to_value(),
        )


class CommunicationsStruct():

    def __init__(
        self,
        nodes: NodesStruct
    ) -> None:
        data: List[Union[CommunicationStruct, TransformCommunicationStruct]] = []
        pub_sub_pair = product(nodes, nodes)

        node_recv: NodeStruct
        node_send: NodeStruct
        for node_recv, node_send in Progress.tqdm(pub_sub_pair, 'Searching communications.'):
            for node_input, node_output in product(node_recv.node_inputs, node_send.node_outputs):
                if not node_input.is_pair(node_output):
                    continue

                comm: Union[CommunicationStruct, TransformCommunicationStruct]

                if isinstance(node_input, TransformFrameBufferStruct) and \
                        isinstance(node_output, TransformFrameBroadcasterStruct):
                    comm = TransformCommunicationStruct(node_output, node_input)
                elif isinstance(node_input, SubscriptionStruct) and \
                        isinstance(node_output, PublisherStruct):
                    comm = CommunicationStruct(
                        nodes=nodes,
                        pub=node_output,
                        sub=node_input,
                        node_pub=node_send,
                        node_sub=node_recv
                    )
                else:
                    NotImplementedError(
                        f'Unsupported communication: {node_input} -> {node_output}')
                data.append(comm)

        self._data = data

    def to_value(self) -> Tuple[CommunicationStructValue, ...]:
        return tuple(_.to_value() for _ in self._data)

    @singledispatchmethod
    def get(self, arg):
        raise InvalidArgumentError('')

    @get.register
    def _get_node_paths(
        self,
        node_path_send: NodePathValue,
        node_path_recv: NodePathValue
    ) -> Union[CommunicationStruct, TransformCommunicationStruct]:
        topic_name = node_path_recv.subscribe_topic_name
        topic_name = topic_name or node_path_send.publish_topic_name

        assert topic_name is not None

        if topic_name == '/tf':
            assert node_path_send.broadcast_frame_id is not None
            assert node_path_send.broadcast_child_frame_id is not None
            assert node_path_recv.buffer_lookup_source_frame_id is not None
            assert node_path_recv.buffer_lookup_target_frame_id is not None

            return self._get_tf_comm_dict(
                node_path_send.node_name,
                node_path_recv.node_name,
                node_path_send.broadcast_frame_id,
                node_path_send.broadcast_child_frame_id,
                node_path_recv.buffer_lookup_source_frame_id,
                node_path_recv.buffer_lookup_target_frame_id,
            )

        return self._get_comm_dict(
            topic_name,
            node_path_send.node_name,
            node_path_recv.node_name
        )

    @get.register
    def _get_comm_dict(
        self,
        topic_name: str,
        publish_node_name: str,
        subscribe_node_name: str,
    ) -> CommunicationStruct:
        def is_target(comm: CommunicationType) -> bool:
            if isinstance(comm, CommunicationStruct):
                return comm.publish_node_name == publish_node_name and \
                    comm.subscribe_node_name == subscribe_node_name and \
                    comm.topic_name == topic_name

            return False
        try:
            return Util.find_one(is_target, self._data)
        except ItemNotFoundError:
            msg = 'Failed to find communication. '
            msg += f'topic_name: {topic_name}, '
            msg += f'publish_node_name: {publish_node_name}, '
            msg += f'subscribe_node_name: {subscribe_node_name}, '

            raise ItemNotFoundError(msg)

    @get.register
    def _get_tf_comm_dict(
        self,
        broadcast_node_name: str,
        lookup_node_name: str,
        broadcast_frame_id: str,
        broadcast_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str,
    ) -> TransformCommunicationStruct:
        def is_target(comm: CommunicationType) -> bool:
            if isinstance(comm, TransformCommunicationStruct):
                return comm.broadcaster.node_name == broadcast_node_name and \
                    comm.buffer.lookup_node_name == lookup_node_name and \
                    comm.broadcaster.frame_id == broadcast_frame_id and \
                    comm.broadcaster.child_frame_id == broadcast_child_frame_id and \
                    comm.buffer.lookup_source_frame_id == lookup_frame_id and \
                    comm.buffer.lookup_target_frame_id == lookup_child_frame_id
            return False

        try:
            return Util.find_one(is_target, self._data)
        except ItemNotFoundError:
            msg = 'Failed to find communication. '
            msg += f'broadcast_node_name: {broadcast_node_name}, '
            msg += f'lookup_node_name: {lookup_node_name}, '
            msg += f'broadcast_frame_id: {broadcast_frame_id}, '
            msg += f'broadcast_child_frame_id: {broadcast_child_frame_id}, '
            msg += f'lookup_frame_id: {lookup_frame_id}, '
            msg += f'lookup_child_frame_id: {lookup_child_frame_id}, '

            raise ItemNotFoundError(msg)

    class IsTargetPubCallback:

        def __init__(self, publish: PublisherStruct):
            self._publish = publish

        def __call__(self, callback: CallbackStruct) -> bool:
            if callback.publish_topic_names is None:
                return False
            return self._publish.topic_name in callback.publish_topic_names

    class IsTargetSubCallback:

        def __init__(self, subscription: SubscriptionStruct):
            self._subscription = subscription

        def __call__(self, callback: CallbackStruct) -> bool:
            if callback.subscribe_topic_name is None:
                return False
            return self._subscription.topic_name == callback.subscribe_topic_name
