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
from typing import Dict, List, Optional, Sequence, Set, Tuple

from itertools import product
from caret_analyze.value_objects.transform import TransformTreeValue
from .graph_search import CallbackPathSearcher
from .reader_interface import ArchitectureReader
from .struct import (
    CommunicationsStruct,
    ExecutorsStruct,
    PathsStruct,
    NodeStruct,
    PublishersStruct,
    PublisherStruct,
    NodesStruct,
    TimersStruct,
    SubscriptionsStruct,
    SubscriptionStruct,
    TransformBroadcasterStruct,
    TransformBufferStruct,
    CallbacksStruct,
    CallbackStruct,
    CallbackGroupStruct,
    CallbackGroupsStruct,
    NodePathsStruct,
    NodeOutputType,
    NodeInputType,
    VariablePassingStruct,
    VariablePassingsStruct,
)
from ..common import Progress, Util
from ..exceptions import InvalidReaderError, Error
from ..value_objects import (
    CallbackGroupValue,
    CommunicationStructValue,
    ExecutorStructValue,
    ExecutorValue,
    NodeStructValue,
    NodeValue,
    ClientCallbackValue,
    ServiceCallbackValue,
    CallbackValue,
    PathStructValue,
    PathValue,
    PublisherValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerCallbackValue,
    TimerValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    TransformValue,
    VariablePassingValue,
    MessageContext
)

logger = getLogger(__name__)


class ArchitectureLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: List[str]
    ) -> None:

        topic_ignored_reader = TopicIgnoredReader(reader, ignore_topics)

        nodes = create_nodes(topic_ignored_reader)
        self._nodes = nodes.to_value()

        executors = ExecutorsStruct.create_from_reader(topic_ignored_reader, nodes)
        self._executors = executors.to_value()

        comms = CommunicationsStruct(nodes)
        self._communications = comms.to_value()

        paths = PathsStruct(topic_ignored_reader, nodes, comms)
        self._paths = paths.to_value()

        return None

    @property
    def paths(self) -> Tuple[PathStructValue, ...]:
        return self._paths

    @property
    def executors(self) -> Tuple[ExecutorStructValue, ...]:
        return self._executors

    @property
    def nodes(self) -> Tuple[NodeStructValue, ...]:
        return self._nodes

    @property
    def communications(self) -> Tuple[CommunicationStructValue, ...]:
        return self._communications


class TopicIgnoredReader(ArchitectureReader):

    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: List[str],
    ) -> None:
        self._reader = reader
        self._ignore_topics = ignore_topics
        self._ignore_callback_ids = self._get_ignore_callback_ids(reader, ignore_topics)

    def _get_publishers(self, node: NodeValue) -> Sequence[PublisherValue]:
        publishers: List[PublisherValue] = []
        for publisher in self._reader._get_publishers(node):
            if publisher.topic_name in self._ignore_topics:
                continue
            publishers.append(publisher)
        return publishers

    def get_tf_frames(self) -> Sequence[TransformValue]:
        return self._reader.get_tf_frames()

    def _get_timers(self, node: NodeValue) -> Sequence[TimerValue]:
        timers: List[TimerValue] = []
        for timer in self._reader._get_timers(node):
            timers.append(timer)
        return timers

    def _get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        return [
            CallbackGroupValue(
                cbg.callback_group_type.type_name,
                cbg.node_name,
                cbg.node_id,
                tuple(set(cbg.callback_ids) - self._ignore_callback_ids),
                cbg.callback_group_id,
                callback_group_name=cbg.callback_group_name
            )
            for cbg
            in self._reader._get_callback_groups(node)
        ]

    def _get_client_callbacks(self, node: NodeValue) -> Sequence[ClientCallbackValue]:
        return self._reader._get_client_callbacks(node)

    def _get_service_callbacks(self, node: NodeValue) -> Sequence[ServiceCallbackValue]:
        return self._reader._get_service_callbacks(node)

    def get_executors(self) -> Sequence[ExecutorValue]:
        return self._reader.get_executors()

    def _get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[Dict]:
        return self._reader._get_message_contexts(node)

    def _get_tf_buffer(self, node: NodeValue) -> Optional[TransformBufferValue]:
        return self._reader._get_tf_buffer(node)

    def _get_tf_broadcaster(self, node: NodeValue) -> Optional[TransformBroadcasterValue]:
        return self._reader._get_tf_broadcaster(node)

    def _filter_callback_id(
        self,
        callback_ids: Tuple[str, ...]
    ) -> Tuple[str, ...]:
        def is_not_ignored(callback_id: str):
            return callback_id not in self._ignore_callback_ids

        return tuple(Util.filter_items(is_not_ignored, callback_ids))

    @staticmethod
    def _get_ignore_callback_ids(
        reader: ArchitectureReader,
        ignore_topics: List[str]
    ) -> Set[str]:
        ignore_callback_ids: List[str] = []
        ignore_topic_set = set(ignore_topics)

        nodes = reader.get_nodes()
        for node in Progress.tqdm(nodes, 'Loading callbacks'):

            sub = reader._get_subscription_callbacks(node)
            for sub_val in sub:
                if sub_val.subscribe_topic_name not in ignore_topic_set:
                    continue

                if sub_val.callback_id is None:
                    continue

                ignore_callback_ids.append(sub_val.callback_id)

        return set(ignore_callback_ids)

    def get_paths(self) -> Sequence[PathValue]:
        return self._reader.get_paths()

    def get_nodes(self) -> Sequence[NodeValue]:
        nodes = self._reader.get_nodes()
        nodes_: List[NodeValue] = []
        node_names = set()
        for node in nodes:
            if node.node_name not in node_names:
                node_names.add(node.node_name)
                nodes_.append(node)
        try:
            self._validate(nodes)
        except InvalidReaderError as e:
            logger.warn(e)
        nodes_ = sorted(nodes_, key=lambda x: x.node_name)

        return nodes_

    @staticmethod
    def _validate(nodes: Sequence[NodeValue]):
        from itertools import groupby

        # validate node name uniqueness.
        node_names = [n.node_name for n in nodes]
        duplicated: List[str] = []
        for node_name, group in groupby(node_names):
            if len(list(group)) >= 2:
                duplicated.append(node_name)
        if len(duplicated) >= 1:
            raise InvalidReaderError(f'Duplicated node name. {duplicated}. Use first node only.')

    def _get_subscriptions(self, node: NodeValue) -> List[SubscriptionValue]:
        subscriptions: List[SubscriptionValue] = []
        for subscription in self._reader._get_subscriptions(node):
            if subscription.topic_name in self._ignore_topics:
                continue
            subscriptions.append(subscription)
        return subscriptions

    def _get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        return self._reader._get_variable_passings(node)

    def _get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        return self._reader.get_timer_callbacks(node.node_name)

    def _get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        callbacks: List[SubscriptionCallbackValue] = []
        for subscription_callback in self._reader._get_subscription_callbacks(node):
            if subscription_callback.subscribe_topic_name in self._ignore_topics:
                continue
            callbacks.append(subscription_callback)
        return callbacks


def create_nodes(reader: ArchitectureReader) -> NodesStruct:
    nodes = NodesStruct()

    node_values = reader.get_nodes()
    for node in Progress.tqdm(node_values, 'Loading nodes.'):
        # try:
        node = create_node(node, reader)
        nodes.insert(node)
        # except Error as e:
        #     logger.warn(f'Failed to load node. node_name = {node.node_name}, {e}')

    return nodes


def create_node(node: NodeValue, reader: ArchitectureReader) -> NodeStruct:
    node_struct = NodeStruct(
        node_id=node.node_name,
        node_name=node.node_name
    )

    # load node inputs/outputs
    publishers = PublishersStruct.create_from_reader(reader, node)
    subscriptions = create_subscriptions(reader, node)

    transforms = reader.get_tf_frames()

    if len(transforms) > 0:
        tf_tree = TransformTreeValue.create_from_transforms(transforms)
        node_struct.tf_buffer = create_transform_buffer(reader, tf_tree, node)
        node_struct.tf_broadcaster = create_transform_broadcaster(reader, tf_tree, node)

    # load node
    callbacks = create_callbacks(reader, subscriptions, publishers, node)
    publishers.assign_callbacks(callbacks)
    subscriptions.assign_callbacks(callbacks)

    node_struct.callbacks = callbacks
    node_struct.publishers = publishers
    node_struct.subscriptions = subscriptions

    node_struct.timers = TimersStruct.create_from_reader(
        reader, callbacks, node)
    node_struct.callback_groups = create_callback_groups(reader, callbacks, node)
    node_struct.variable_passings = create_variable_passings(
        reader, callbacks, node)
    node_struct.node_paths = create_node_paths(reader, node_struct)

    return node_struct


def create_subscriptions(
    reader: ArchitectureReader,
    node: NodeValue
) -> SubscriptionsStruct:

    subscriptions = SubscriptionsStruct()
    subscription_values = reader.get_subscriptions(node.node_name)
    for sub_value in subscription_values:
        try:
            sub = SubscriptionStruct(sub_value.callback_id,
                                     sub_value.node_name,
                                     sub_value.topic_name)
            subscriptions.add(sub)
        except Error as e:
            logger.warning(e)
    return subscriptions


def create_transform_buffer(
        reader: ArchitectureReader,
        tf_tree: TransformTreeValue,
        node: NodeValue,
) -> Optional[TransformBufferStruct]:
    buffer = reader.get_tf_buffer(node.node_name)

    if buffer is None:
        return None

    assert buffer.lookup_transforms is not None and len(
        buffer.lookup_transforms) > 0
    assert buffer.listen_transforms is not None
    return TransformBufferStruct(
        tf_tree=tf_tree,
        lookup_node_name=buffer.lookup_node_name,
        listener_node_name=buffer.listener_node_name,
        lookup_transforms=list(buffer.lookup_transforms),
        listen_transforms=list(buffer.listen_transforms)
    )


def create_transform_broadcaster(
        reader: ArchitectureReader,
        tf_tree: TransformTreeValue,
        node: NodeValue,
) -> Optional[TransformBroadcasterStruct]:
    broadcaster = reader.get_tf_broadcaster(node.node_name)

    if broadcaster is None:
        return None

    assert broadcaster.broadcast_transforms is not None

    # TODO(hsgwa) : auto bind tf br and callback
    publisher = PublisherStruct(node.node_name, '/tf', None)

    return TransformBroadcasterStruct(
        publisher=publisher,
        transforms=list(broadcaster.broadcast_transforms)
    )


def create_callbacks(
    reader: ArchitectureReader,
    subscriptions: SubscriptionsStruct,
    publishers: PublishersStruct,
    node: NodeValue
) -> CallbacksStruct:
    callbacks = CallbacksStruct()

    callback_values: List[CallbackValue] = []
    callback_values += reader.get_timer_callbacks(node.node_name)
    callback_values += reader.get_subscription_callbacks(node.node_name)
    callback_values += reader.get_service_callbacks(node.node_name)
    callback_values += reader.get_client_callbacks(node.node_name)

    for i, callback_value in enumerate(callback_values):
        callback = CallbackStruct.create_instance(subscriptions, publishers, callback_value, i)
        callbacks.insert(callback)

    return callbacks


def create_callback_groups(
    reader: ArchitectureReader,
    callbacks: CallbacksStruct,
    node: NodeValue
) -> CallbackGroupsStruct:
    cbgs = CallbackGroupsStruct()
    cbgs.node = node

    for i, cbg_value in enumerate(reader.get_callback_groups(node.node_name)):
        cbg_name = cbg_value.callback_group_name or f'{node.node_name}/callback_group_{i}'

        cbg = CallbackGroupStruct(
            callback_group_type=cbg_value.callback_group_type,
            node_name=node.node_name,
            callbacks=callbacks.get_callback_by_cbg(cbg_value),
            callback_group_name=cbg_name,
            callback_group_id=cbg_value.callback_group_id,
        )

        cbgs.insert(cbg)

    return cbgs


def create_variable_passings(
    reader: ArchitectureReader,
    callbacks: CallbacksStruct,
    node: NodeValue,
) -> VariablePassingsStruct:

    var_passes = VariablePassingsStruct()

    for var_pass_value in reader.get_variable_passings(node.node_name):
        if var_pass_value.callback_id_read is None or \
                var_pass_value.callback_id_write is None:
            continue
        var_pass = VariablePassingStruct(
            node.node_name,
            callback_write=callbacks.get_callback(
                var_pass_value.callback_id_write),
            callback_read=callbacks.get_callback(
                var_pass_value.callback_id_read)
        )
        var_passes.add(var_pass)

    return var_passes


def create_node_paths(
    reader: ArchitectureReader,
    node: NodeStruct,
) -> NodePathsStruct:

    node_paths = NodePathsStruct()
    node.node_paths = node_paths

    for node_input in node.node_inputs:
        node_paths.create(node.node_name, node_input, None)

    for node_output in node.node_outputs:
        node_paths.create(node.node_name, None, node_output)

    for node_input, node_output in product(node.node_inputs, node.node_outputs):
        node_paths.create(node.node_name, node_input, node_output)

    # assign callback-graph paths
    logger.info('[callback_chain]')

    assign_callback_graph_paths(node, node_paths)

    # assign message contexts
    create_message_context(node_paths, reader, node)

    return node_paths


def assign_callback_graph_paths(node: NodeStruct, node_paths: NodePathsStruct):
    searcher = CallbackPathSearcher(node, node_paths)
    callbacks = node.callbacks

    if callbacks is None:
        return None

    for start_callback, end_callback in product(callbacks, callbacks):
        cb_paths = searcher.search(start_callback, end_callback)
        if len(cb_paths) == 0:
            continue
        if len(cb_paths) > 1:
            raise ValueError(f'[callback_chain] {len(cb_paths)} paths found')
        cb_path = cb_paths[0]
        node_inputs = start_callback.node_inputs
        node_outputs = end_callback.node_outputs
        is_assignable = node_inputs is not None and node_outputs is not None
        if not is_assignable:
            continue

        for node_input, node_output in product(node_inputs, node_outputs):
            node_path = node.get_node_path(node_input, node_output)
            node_path.callback_path = cb_path


def create_message_context(
    node_paths: NodePathsStruct,
    reader: ArchitectureReader,
    node: NodeStruct,
) -> None:

    # nodenode__path = node_paths.get(
    #     message_context.subscription_topic_name,
    #     message_context.publisher_topic_name
    # )
    # node_path.message_context = message_context
    # self._data: Tuple[MessageContext, ...]
    # data: List[MessageContext] = []

    context_dicts = reader.get_message_contexts(node.node_name)
    # pub_sub_pairs: List[Tuple[Optional[str], Optional[str]]] = []
    for context_dict in context_dicts:
        try:
            context_type = context_dict.get('context_type')
            if context_type is None:
                logger.info(
                    f'message context is None. {context_dict}')
                continue

            pub_topic_name = context_dict['publisher_topic_name']
            node_output: NodeOutputType
            if pub_topic_name == '/tf':
                assert node.tf_broadcaster is not None
                node_output = node.tf_broadcaster.get(
                    context_dict['broadcast_frame_id'],
                    context_dict['broadcast_child_frame_id'],
                )
            else:
                assert node.publishers is not None
                assert isinstance(pub_topic_name, str)
                node_output = node.publishers.get(node.node_name, pub_topic_name)

            sub_topic_name = context_dict.get('subscription_topic_name')
            node_input: NodeInputType
            if sub_topic_name == '/tf':
                assert node.tf_buffer is not None
                node_input = node.tf_buffer.get(
                    context_dict['listen_frame_id'],
                    context_dict['listen_child_frame_id'],
                    context_dict['lookup_frame_id'],
                    context_dict['lookup_child_frame_id'],
                )
            else:
                assert node.subscriptions is not None
                assert isinstance(sub_topic_name, str)
                node_input = node.subscriptions.get(node.node_name, sub_topic_name)

            node_path = node_paths.get(node_input, node_output)

            node_path.message_context = MessageContext.create_instance(
                context_type_name=context_type,
                context_dict=context_dict,
                node_name=node.node_name,
                node_in=node_input.to_value(),
                node_out=node_output.to_value(),
                child=None
            )
        except Error as e:
            logger.warning(e)
