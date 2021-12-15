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
from logging import getLogger
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union

from caret_analyze.value_objects.message_context import (CallbackChain,
                                                         MessageContext)

from ..common import Util
from ..exceptions import (Error, InvalidArgumentError, InvalidReaderError,
                          InvalidYamlFormatError, ItemNotFoundError,
                          MultipleItemFoundError, UnsupportedTypeError)
from ..value_objects import (CallbackGroupStructValue, CallbackGroupValue,
                             CallbackStructValue, CallbackValue,
                             CommunicationStructValue, ExecutorStructValue,
                             ExecutorValue, NodePathStructValue, NodePathValue,
                             NodeStructValue, NodeValue, NodeValueWithId,
                             PathStructValue, PathValue, PublisherStructValue,
                             PublisherValue, SubscriptionCallbackStructValue,
                             SubscriptionCallbackValue,
                             SubscriptionStructValue, SubscriptionValue,
                             TimerCallbackStructValue, TimerCallbackValue,
                             VariablePassingStructValue, VariablePassingValue)
from .reader_interface import UNDEFINED_STR, ArchitectureReader

logger = getLogger(__name__)


def indexed_name(base_name: str, i: int, num_digit: int):
    index_str = str(i).zfill(num_digit)
    return f'{base_name}_{index_str}'


class ArchitectureLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: List[str]
    ) -> None:

        topic_ignored_reader = TopicIgnoredReader(reader, ignore_topics)

        self._nodes: Tuple[NodeStructValue, ...]
        nodes_loaded = NodeValuesLoaded(topic_ignored_reader)

        self._nodes = nodes_loaded.data

        execs_loaded = ExecutorValuesLoaded(topic_ignored_reader, nodes_loaded)
        self._executors: Tuple[ExecutorStructValue, ...]
        self._executors = execs_loaded.data

        comms_loaded = CommValuesLoaded(nodes_loaded)
        self._communications: Tuple[CommunicationStructValue, ...]
        self._communications = comms_loaded.data

        paths_loaded = PathValuesLoaded(
            topic_ignored_reader, nodes_loaded, comms_loaded)
        self._named_paths: Tuple[PathStructValue, ...]
        self._paths = paths_loaded.data

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


class CommValuesLoaded():

    def __init__(
        self,
        nodes_loaded: NodeValuesLoaded
    ) -> None:
        from tqdm import tqdm
        node_values = nodes_loaded.data

        data: List[CommunicationStructValue] = []
        pub_sub_pair = product(node_values, node_values)

        node_pub: NodeStructValue
        node_sub: NodeStructValue
        for node_pub, node_sub in tqdm(pub_sub_pair, 'Loading communications.'):
            for pub, sub in product(node_pub.publishers, node_sub.subscriptions):
                if pub.topic_name != sub.topic_name:
                    continue
                data.append(
                    self._to_struct(nodes_loaded, pub, sub, node_pub, node_sub)
                )
        self._data = tuple(data)

    @staticmethod
    def _to_struct(
        nodes_loaded: NodeValuesLoaded,
        pub: PublisherStructValue,
        sub: SubscriptionStructValue,
        node_pub: NodeStructValue,
        node_sub: NodeStructValue,
    ) -> CommunicationStructValue:
        from ..common import Util

        try:
            callbacks_pub = None
            is_target_pub_cb = CommValuesLoaded.IsTargetPubCallback(pub)
            callback_values = nodes_loaded.get_callbacks(pub.node_name)
            callbacks_pub = tuple(Util.filter_items(is_target_pub_cb, callback_values))
        except ItemNotFoundError:
            logger.info(f'Failed to find publisher callback. {node_pub}. Skip loading')
        except MultipleItemFoundError:
            msg = 'Failed to identify subscription. Several candidates were found. Skip loading.'
            msg += f'node_name: {node_sub.node_name}, '
            msg += f'topic_name: {sub.topic_name}'
            logger.warning(msg)

        try:
            callback_sub = None
            is_target_sub_cb = CommValuesLoaded.IsTargetSubCallback(sub)
            callback_values = nodes_loaded.get_callbacks(sub.node_name)
            callback_sub = Util.find_one(is_target_sub_cb, callback_values)
        except ItemNotFoundError:
            logger.info(f'Failed to find publisher callback. {node_sub}. Skip loading')
        except MultipleItemFoundError:
            msg = 'Failed to identify subscription. Several candidates were found. Skip loading.'
            msg += f'node_name: {node_sub.node_name}, '
            msg += f'topic_name: {sub.topic_name}'
            logger.warning(msg)

        return CommunicationStructValue(
            node_pub, node_sub, pub, sub, callbacks_pub, callback_sub)

    @property
    def data(self) -> Tuple[CommunicationStructValue, ...]:
        return self._data

    def find_communication(
        self,
        topic_name: str,
        publish_node_name: str,
        subscribe_node_name: str,
    ) -> CommunicationStructValue:
        from ..common import Util

        def is_target(comm: CommunicationStructValue):
            return comm.publish_node_name == publish_node_name and \
                comm.subscribe_node_name == subscribe_node_name and \
                comm.topic_name == topic_name
        try:
            return Util.find_one(is_target, self.data)
        except ItemNotFoundError:
            msg = 'Failed to find communication. '
            msg += f'topic_name: {topic_name}, '
            msg += f'publish_node_name: {publish_node_name}, '
            msg += f'subscribe_node_name: {subscribe_node_name}, '

            raise ItemNotFoundError(msg)

    class IsTargetPubCallback:

        def __init__(self, publish: PublisherStructValue):
            self._publish = publish

        def __call__(self, callback: CallbackStructValue) -> bool:
            if callback.publish_topic_names is None:
                return False
            return self._publish.topic_name in callback.publish_topic_names

    class IsTargetSubCallback:

        def __init__(self, subscription: SubscriptionStructValue):
            self._subscription = subscription

        def __call__(self, callback: CallbackStructValue) -> bool:
            if callback.subscribe_topic_name is None:
                return False
            return self._subscription.topic_name == callback.subscribe_topic_name


class NodeValuesLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
    ) -> None:
        from tqdm import tqdm
        nodes_struct: List[NodeStructValue] = []
        self._cb_loaded: List[CallbacksLoaded] = []
        self._cbg_loaded: List[CallbackGroupsLoaded] = []

        nodes = reader.get_nodes()
        nodes = sorted(nodes, key=lambda x: x.node_name)
        try:
            self._validate(nodes)
        except InvalidReaderError as e:
            logger.warn(e)

        nodes = self._remove_duplicated(nodes)

        for node in tqdm(nodes, 'Loading nodes.'):
            try:
                node, cb_loaded, cbg_loaded = self._create_node(node, reader)
                nodes_struct.append(node)
                self._cb_loaded.append(cb_loaded)
                self._cbg_loaded.append(cbg_loaded)
            except Error as e:
                logger.warn(f'Failed to load node. node_name = {node.node_name}, {e}')

        nodes_struct = sorted(nodes_struct, key=lambda x: x.node_name)
        self._data = tuple(nodes_struct)

    @staticmethod
    def _remove_duplicated(nodes: Sequence[NodeValueWithId]) -> Sequence[NodeValueWithId]:
        nodes_: List[NodeValueWithId] = []
        node_names = set()
        for node in nodes:
            if node.node_name not in node_names:
                node_names.add(node.node_name)
                nodes_.append(node)
        return nodes_

    @staticmethod
    def _validate(nodes: Sequence[NodeValueWithId]):
        from itertools import groupby

        # validate node name uniqueness.
        node_names = [n.node_name for n in nodes]
        duplicated: List[str] = []
        for node_name, group in groupby(node_names):
            if len(list(group)) >= 2:
                duplicated.append(node_name)
        if len(duplicated) >= 1:
            raise InvalidReaderError(f'Duplicated node name. {duplicated}. Use first node only.')

    @property
    def data(self) -> Tuple[NodeStructValue, ...]:
        return self._data

    def get_callbacks(
        self,
        node_name: str
    ) -> Tuple[CallbackStructValue, ...]:
        from ..common import Util
        try:
            cb_loaded: CallbacksLoaded
            cb_loaded = Util.find_one(lambda x: x.node_name == node_name, self._cb_loaded)
            return cb_loaded.data
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def find_node(self, node_name: str) -> NodeStructValue:
        from ..common import Util
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self.data)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def find_node_path(
        self,
        node_path_value: NodePathValue,
    ) -> NodePathStructValue:
        from ..common import Util

        def is_target(value: NodePathStructValue):
            return value.publish_topic_name == node_path_value.publish_topic_name and \
                value.subscribe_topic_name == node_path_value.subscribe_topic_name

        node_value = self.find_node(node_path_value.node_name)
        try:
            return Util.find_one(is_target, node_value.paths)
        except ItemNotFoundError:
            msg = 'Failed to find node path value. '
            msg += f' node_name: {node_path_value.node_name}'
            msg += f' publish_topic_name: {node_path_value.publish_topic_name}'
            msg += f' subscribe_topic_name: {node_path_value.subscribe_topic_name}'
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError as e:
            raise MultipleItemFoundError(
                f'{e}'
                f' node_name: {node_path_value.node_name}'
                f' publish_topic_name: {node_path_value.publish_topic_name}'
                f' subscribe_topic_name: {node_path_value.subscribe_topic_name}'
            )

    def find_callback_group(
        self,
        callback_group_id: str
    ) -> CallbackGroupStructValue:
        for cbg_loaded in self._cbg_loaded:
            try:
                return cbg_loaded.find_callback_group(callback_group_id)
            except ItemNotFoundError:
                pass

        msg = f'Failed to find callback group. callback_group_id={callback_group_id}'
        raise ItemNotFoundError(msg)

    def find_callback(
        self,
        callback_id: str
    ) -> CallbackStructValue:
        for cb_loaded in self._cb_loaded:
            try:
                return cb_loaded.find_callback(callback_id)
            except ItemNotFoundError:
                pass
        raise ItemNotFoundError(f'Failed to find callback. callback_id={callback_id}')

    def find_callbacks(
        self,
        callback_ids: Tuple[str, ...]
    ) -> Tuple[CallbackStructValue, ...]:
        callbacks: List[CallbackStructValue] = []
        for cb_loaded in self._cb_loaded:
            callbacks += cb_loaded.search_callbacks(callback_ids)

        if len(callbacks) < len(callback_ids):
            raise ItemNotFoundError(f'Failed to find callback. callback_ids={callback_ids}')

        return tuple(callbacks)

    @staticmethod
    def _create_node(
        node: NodeValue,
        reader: ArchitectureReader,
    ) -> Tuple[NodeStructValue, CallbacksLoaded, CallbackGroupsLoaded]:

        callbacks_loaded = CallbacksLoaded(reader, node)

        publishers: Tuple[PublisherStructValue, ...]
        publishers = PublishersLoaded(reader, callbacks_loaded, node).data

        subscriptions: Tuple[SubscriptionStructValue, ...]
        subscriptions = SubscriptionsLoaded(reader, callbacks_loaded, node).data

        callback_groups: Tuple[CallbackGroupStructValue, ...]
        cbg_loaded = CallbackGroupsLoaded(reader, callbacks_loaded, node)
        callback_groups = cbg_loaded.data

        variable_passings: Tuple[VariablePassingStructValue, ...]
        variable_passings = VariablePassingsLoaded(
            reader, callbacks_loaded, node).data

        node_struct = NodeStructValue(
            node.node_name, publishers, subscriptions, (),
            callback_groups, variable_passings
        )

        try:
            node_paths = NodeValuesLoaded._search_node_paths(node_struct, reader)
            node_path_added = NodeStructValue(
                node_struct.node_name, node_struct.publishers,
                node_struct.subscriptions,
                tuple(node_paths), node_struct.callback_groups,
                node_struct.variable_passings
            )

            return node_path_added, callbacks_loaded, cbg_loaded
        except Error as e:
            # If the node path registration fails,
            # it returns with empty node path.
            logger.warning(e)
            return node_struct, callbacks_loaded, cbg_loaded

    @staticmethod
    def _search_node_paths(
        node: NodeStructValue,
        reader: ArchitectureReader
    ) -> Tuple[NodePathStructValue, ...]:

        node_paths: List[NodePathStructValue] = []

        # # add single callback paths
        # if node.callback_values is not None:
        #     for callback in node.callback_values:
        #         sub = None
        #         if callback.subscribe_topic_name is not None:
        #             sub = node.get_subscription_value(callback.subscribe_topic_name)

        #         pubs = None
        #         if callback.publish_topic_names is not None:
        #             for publish_topic_name in callback.publish_topic_names:
        #                 pubs = pubs or []
        #                 pubs.append(node.get_publisher_value(publish_topic_name))

        #         if pubs is None:
        #             path = NodePathStructValue(node.node_name, sub, None, (callback, ), None)
        #             node_paths.append(path)
        #             continue
        #         for pub in pubs:
        #             path = NodePathStructValue(node.node_name, sub, pub, (callback, ), None)
        #             node_paths.append(path)

        # add callback-graph paths
        logger.info('[callback_chain]')
        node_paths += list(CallbackPathSearched(node).data)

        # add pub-sub pair graph paths
        logger.info('\n[pub-sub pair]')
        pubs = node.publishers
        subs = node.subscriptions
        node_path_pub_sub_pairs = NodePathCreated(subs, pubs).data
        for node_path in node_path_pub_sub_pairs:
            added_pub_sub_pairs = [(n.publish_topic_name, n.subscribe_topic_name)
                                   for n in node_paths]
            pub_sub_pair = (node_path.publish_topic_name,
                            node_path.subscribe_topic_name)

            if pub_sub_pair not in added_pub_sub_pairs:
                node_paths.append(node_path)

                logger.info(
                    'Path Added: '
                    f'subscribe: {node_path.subscribe_topic_name}, '
                    f'publish: {node_path.publish_topic_name}, '
                )

        # add dummy node paths
        logger.info('\n[dummy paths]')
        for pub in node.publishers:
            added_pub_sub_pairs = [(n.publish_topic_name, n.subscribe_topic_name)
                                   for n in node_paths]
            node_path = NodePathStructValue(
                node.node_name,
                None,
                pub,
                None,
                None
            )
            pub_sub_pair = (node_path.publish_topic_name,
                            node_path.subscribe_topic_name)
            if pub_sub_pair not in added_pub_sub_pairs:
                node_paths.append(node_path)
                logger.info(
                    'Path Added: '
                    f'subscribe: {node_path.subscribe_topic_name}, '
                    f'publish: {node_path.publish_topic_name}, '
                )

        for sub in node.subscriptions:
            added_pub_sub_pairs = [(n.publish_topic_name, n.subscribe_topic_name)
                                   for n in node_paths]
            node_path = NodePathStructValue(
                node.node_name,
                sub,
                None,
                None,
                None
            )
            pub_sub_pair = (node_path.publish_topic_name,
                            node_path.subscribe_topic_name)
            if pub_sub_pair not in added_pub_sub_pairs:
                node_paths.append(node_path)
                logger.info(
                    'Path Added: '
                    f'subscribe: {node_path.subscribe_topic_name}, '
                    f'publish: {node_path.publish_topic_name}'
                )

        message_contexts: List[MessageContext] = []
        message_contexts += list(MessageContextsLoaded(reader, node, node_paths).data)

        # assign message context to each node paths
        node_paths = NodeValuesLoaded._message_context_assigned(
            node_paths, message_contexts)

        logger.info(f'\n{len(node_paths)} paths found in {node.node_name}.')

        logger.info('\n-----\n[message context assigned]')
        for path in node_paths:
            message_context = None
            if path.message_context is not None:
                message_context = path.message_context.type_name

            logger.info(
                f'subscribe: {path.subscribe_topic_name}, '
                f'publish: {path.publish_topic_name}, '
                f'message_context: {message_context}'
            )

        return tuple(node_paths)

    @staticmethod
    def _message_context_assigned(
        node_paths: Sequence[NodePathStructValue],
        message_contexts: Sequence[MessageContext],
    ) -> List[NodePathStructValue]:
        node_paths_ = list(node_paths)
        for i, node_path in enumerate(node_paths_):
            for context in message_contexts:
                if node_path.subscription is None or \
                        node_path.publisher is None:
                    continue

                if not context.is_applicable_path(
                    node_path.subscription,
                    node_path.publisher,
                    node_path.callbacks
                ):
                    continue

                node_paths_[i] = NodePathStructValue(
                    node_path.node_name,
                    node_path.subscription,
                    node_path.publisher,
                    node_path.child,
                    context
                )
        return node_paths_


class MessageContextsLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        node: NodeStructValue,
        node_paths: Sequence[NodePathStructValue]
    ) -> None:
        self._data: Tuple[MessageContext, ...]
        data: List[MessageContext] = []

        context_dicts = reader.get_message_contexts(NodeValue(node.node_name, None))
        pub_sub_pairs = []
        for context_dict in context_dicts:
            try:
                context_type = context_dict['context_type']
                if context_type == UNDEFINED_STR:
                    logger.info(f'message context is UNDEFINED. {context_dict}')
                    continue
                node_path = self.get_node_path(node_paths,
                                               context_dict['publisher_topic_name'],
                                               context_dict['subscription_topic_name'])
                data.append(self._to_struct(context_dict, node_path))
                pub_sub_pairs.append((node_path.publish_topic_name, node_path.subscribe_topic_name))
            except Error as e:
                logger.warning(e)

        for context in self._create_callack_chain(node_paths):
            pub_sub_pair = (context.publisher_topic_name, context.subscription_topic_name)
            if context not in data and pub_sub_pair not in pub_sub_pairs:
                data.append(context)

        self._data = tuple(data)

    @staticmethod
    def get_node_path(
        node_paths: Sequence[NodePathStructValue],
        publisher_topic_name: str,
        subscription_topic_name: str
    ) -> NodePathStructValue:
        def is_target(path: NodePathValue):
            return path.publish_topic_name == publisher_topic_name and \
                path.subscribe_topic_name == subscription_topic_name
        return Util.find_one(is_target, node_paths)

    @property
    def data(self) -> Tuple[MessageContext, ...]:
        return self._data

    @staticmethod
    def _create_callack_chain(
        node_paths: Sequence[NodePathStructValue]
    ) -> List[MessageContext]:
        chains: List[MessageContext] = []
        for path in node_paths:
            if path.callbacks is not None:
                chains.append(
                    CallbackChain(
                        path.node_name,
                        {},
                        path.subscription,
                        path.publisher,
                        path.callbacks)
                )
        return chains

    @staticmethod
    def _to_struct(
        context_dict: Dict,
        node_path: NodePathStructValue
    ) -> MessageContext:
        type_name = context_dict['context_type']

        try:
            return MessageContext.create_instance(
                type_name,
                context_dict,
                node_path.node_name,
                node_path.subscription,
                node_path.publisher,
                node_path.callbacks)
        except UnsupportedTypeError:
            raise InvalidYamlFormatError(
                'Failed to load message context. '
                f'node_name: {node_path.node_name}, '
                f'context: {context_dict}')


class NodePathCreated:
    def __init__(
        self,
        subscription_values: Tuple[SubscriptionStructValue, ...],
        publisher_values: Tuple[PublisherStructValue, ...],
    ) -> None:
        paths: List[NodePathStructValue] = []
        for sub, pub in product(subscription_values, publisher_values):
            paths.append(
                NodePathStructValue(sub.node_name, sub, pub, None, None)
            )

        self._data = tuple(paths)

    @property
    def data(self) -> Tuple[NodePathStructValue, ...]:
        return self._data


class PublishersLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        publisher_values = reader.get_publishers(node)
        self._data = tuple(self._to_struct(callbacks_loaded, pub)
                           for pub in publisher_values)

    @staticmethod
    def _to_struct(
        callbacks_loaded: CallbacksLoaded,
        publisher_value: PublisherValue
    ) -> PublisherStructValue:

        pub_callbacks: List[CallbackStructValue] = []
        if publisher_value.callback_ids is not None:
            for callback_id in publisher_value.callback_ids:
                if callback_id == UNDEFINED_STR:
                    continue
                pub_callbacks.append(
                    callbacks_loaded.find_callback(callback_id))

        callbacks = PublishersLoaded._get_callbacks(callbacks_loaded)
        if len(pub_callbacks) == 0 and len(callbacks) == 1:
            pub_callbacks.append(callbacks[0])

        for callback in callbacks:
            if callback.publish_topic_names is None:
                continue
            if publisher_value.topic_name in callback.publish_topic_names and \
                    callback not in pub_callbacks:
                pub_callbacks.append(callback)

        return PublisherStructValue(
            publisher_value.node_name,
            publisher_value.topic_name,
            callback_values=tuple(pub_callbacks),
        )

    @staticmethod
    def _get_callbacks(
        callbacks_loaded: CallbacksLoaded,
    ) -> List[CallbackStructValue]:
        def is_user_defined(callback: CallbackStructValue):
            if isinstance(callback, SubscriptionCallbackStructValue):
                if callback.subscribe_topic_name == '/parameter_events':
                    return False
            return True

        callbacks = callbacks_loaded.data
        return Util.filter_items(is_user_defined, callbacks)

    @property
    def data(self) -> Tuple[PublisherStructValue, ...]:
        return self._data


class SubscriptionsLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        subscription_values = reader.get_subscriptions(node)
        self._data = tuple(self._to_struct(callbacks_loaded, sub)
                           for sub in subscription_values)

    def _to_struct(
        self,
        callbacks_loaded: CallbacksLoaded,
        subscription_value: SubscriptionValue
    ) -> SubscriptionStructValue:
        sub_callback: Optional[CallbackStructValue] = None

        if subscription_value.callback_id is not None:
            sub_callback = callbacks_loaded.find_callback(
                subscription_value.callback_id)

        assert isinstance(sub_callback, SubscriptionCallbackStructValue)

        return SubscriptionStructValue(
            subscription_value.node_name,
            subscription_value.topic_name,
            sub_callback
        )

    @property
    def data(self) -> Tuple[SubscriptionStructValue, ...]:
        return self._data


class VariablePassingsLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue,
    ) -> None:
        data: List[VariablePassingStructValue] = []

        for var_pass in reader.get_variable_passings(node):
            if var_pass.callback_id_read == UNDEFINED_STR or\
                    var_pass.callback_id_write == UNDEFINED_STR:
                continue
            data.append(
                VariablePassingStructValue(
                    node.node_name,
                    callback_write=callbacks_loaded.find_callback(
                        var_pass.callback_id_write),
                    callback_read=callbacks_loaded.find_callback(
                        var_pass.callback_id_read)
                )
            )

        self._data: Tuple[VariablePassingStructValue, ...]
        self._data = tuple(data)

    @property
    def data(self) -> Tuple[VariablePassingStructValue, ...]:
        return self._data


class CallbackGroupsLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        self._data: Dict[str, CallbackGroupStructValue] = {}
        for i, cbg in enumerate(reader.get_callback_groups(node)):
            self._validate(cbg, node)
            cbg_name = cbg.callback_group_name or f'{node.node_name}/callback_group_{i}'

            cbg_struct = CallbackGroupStructValue(
                cbg.callback_group_type,
                node.node_name,
                self._get_callbacks(callbacks_loaded, cbg),
                cbg_name
            )

            self._data[cbg.callback_group_id] = cbg_struct

    @staticmethod
    def _validate(cbg: CallbackGroupValue, node: NodeValue):
        # TODO: add callback group id validation

        if len(cbg.callback_ids) != len(set(cbg.callback_ids)):
            raise InvalidReaderError(f'duplicated callback id. {node}, {cbg}')

    @property
    def data(self) -> Tuple[CallbackGroupStructValue, ...]:
        return tuple(self._data.values())

    def find_callback_group(self, callback_group_id: str):
        if callback_group_id in self._data:
            return self._data[callback_group_id]

        raise ItemNotFoundError('')

    def _get_callbacks(
        self,
        callbacks_loaded: CallbacksLoaded,
        callback_group: CallbackGroupValue,
    ) -> Tuple[CallbackStructValue, ...]:
        callback_structs: List[CallbackStructValue] = []
        for callback_id in callback_group.callback_ids:
            # Ensure that the callback information exists.
            callback_struct = callbacks_loaded.find_callback(callback_id)
            callback_structs.append(callback_struct)
        return tuple(callback_structs)


class CallbacksLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
        node: NodeValue,
    ) -> None:
        self._node = node
        callbacks: List[CallbackValue] = []
        callbacks += reader.get_timer_callbacks(node)
        callbacks += reader.get_subscription_callbacks(node)

        self._validate(callbacks)
        self._callbacks = callbacks

        self._callback_count: Dict[CallbackValue, int] = {}
        self._cb_dict: Dict[str, CallbackStructValue] = {}

        callback_num = Util.num_digit(len(callbacks))
        for callback in callbacks:
            if callback.callback_id is None:
                continue
            self._cb_dict[callback.callback_id] = self._to_struct(
                callback, callback_num)

    @property
    def node_name(self) -> str:
        return self._node.node_name

    @property
    def data(self) -> Tuple[CallbackStructValue, ...]:
        return tuple(self._cb_dict.values())

    def _to_struct(
        self,
        callback: CallbackValue,
        callback_num: int
    ) -> CallbackStructValue:

        if isinstance(callback, TimerCallbackValue):
            self._callback_count[callback] = self._callback_count.get(
                callback, len(self._callback_count))
            callback_count = self._callback_count[callback]
            indexed = indexed_name(
                f'{self.node_name}/callback', callback_count, callback_num)
            callback_name = callback.callback_name or indexed

            return TimerCallbackStructValue(
                node_name=callback.node_name,
                symbol=callback.symbol,
                period_ns=callback.period_ns,
                publish_topic_names=callback.publish_topic_names,
                callback_name=callback_name,
            )
        if isinstance(callback, SubscriptionCallbackValue):
            assert callback.subscribe_topic_name is not None
            self._callback_count[callback] = self._callback_count.get(
                callback, len(self._callback_count))
            callback_count = self._callback_count[callback]
            indexed = indexed_name(
                f'{self.node_name}/callback', callback_count, callback_num)
            callback_name = callback.callback_name or indexed
            return SubscriptionCallbackStructValue(
                node_name=callback.node_name,
                symbol=callback.symbol,
                subscribe_topic_name=callback.subscribe_topic_name,
                publish_topic_names=callback.publish_topic_names,
                callback_name=callback_name,
            )
        raise UnsupportedTypeError('Unsupported callback type')

    def _validate(self, callbacks: List[CallbackValue]) -> None:
        # check node name
        for callback in callbacks:
            if callback.node_id != self._node.node_id:
                msg = 'reader returns invalid callback value. '
                msg += f'get [{self._node.node_id}] value returns [{callback.node_id}]'
                raise InvalidReaderError(msg)

        # check callback name
        cb_names: List[str] = [
            cb.callback_name for cb in callbacks if cb.callback_name is not None]
        if len(cb_names) != len(set(cb_names)):
            msg = f'Duplicated callback names. node_name: {self._node.node_name}\n'
            for name in set(cb_names):
                if cb_names.count(name) >= 2:
                    msg += f'callback name: {name} \n'
            raise InvalidReaderError(msg)

        # check callback id
        cb_ids: List[str] = [
            cb.callback_id
            for cb
            in callbacks
            if cb.callback_id is not None
        ]
        if len(cb_names) != len(set(cb_names)):
            msg = f'Duplicated callback id. node_name: {self._node.node_name}\n'
            for cb_id in set(cb_ids):
                if cb_ids.count(cb_id) >= 2:
                    msg += f'callback id: {cb_id} \n'
            raise InvalidReaderError(msg)

    def find_callback(
        self,
        callback_id: str
    ) -> CallbackStructValue:
        if callback_id in self._cb_dict.keys():
            return self._cb_dict[callback_id]

        msg = 'Failed to find callback. '
        msg += f'node_name: {self._node.node_name}, '
        msg += f'callback_id: {callback_id}, '
        raise ItemNotFoundError(msg)

    def search_callbacks(
        self,
        callback_ids: Tuple[str, ...]
    ) -> Tuple[CallbackStructValue, ...]:
        """
        Search callbacks.

        Parameters
        ----------
        callback_ids : Tuple[str, ...]
            target callback ids

        Returns
        -------
        Tuple[CallbackStructValue, ...]
            If the callback is not found, it returns an empty tuple.

        """
        callbacks: List[CallbackStructValue] = []

        for callback_id in callback_ids:
            if callback_id not in self._cb_dict.keys():
                continue
            callbacks.append(self.find_callback(callback_id))

        return tuple(callbacks)


class ExecutorValuesLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
        nodes_loaded: NodeValuesLoaded,
    ) -> None:
        execs: List[ExecutorStructValue] = []

        exec_vals = reader.get_executors()
        num_digit = Util.num_digit(len(exec_vals))

        for i, executor in enumerate(exec_vals):
            executor_name = indexed_name('executor', i, num_digit)
            try:
                execs.append(
                    self._to_struct(executor_name, executor, nodes_loaded)
                )
            except Error as e:
                logger.warning(
                    'Failed to load executor. skip loading. '
                    f'executor_name = {executor_name}. {e}')

        self._data = tuple(execs)

    @staticmethod
    def _to_struct(
        executor_name: str,
        executor: ExecutorValue,
        nodes_loaded: NodeValuesLoaded,
    ) -> ExecutorStructValue:
        callback_group_values: List[CallbackGroupStructValue] = []

        for cbg_id in executor.callback_group_ids:
            try:
                callback_group_values.append(
                    ExecutorValuesLoaded._find_struct_callback_group(
                        cbg_id, nodes_loaded)
                )
            except Error as e:
                logger.info(
                    f'Failed to load executor. executor_name: {executor_name}')
                logger.warn(e)

        return ExecutorStructValue(
            executor.executor_type,
            tuple(callback_group_values),
            executor_name,
        )

    @staticmethod
    def _find_struct_callback_group(
        callback_group_id: str,
        nodes_loaded: NodeValuesLoaded,
    ) -> CallbackGroupStructValue:
        return nodes_loaded.find_callback_group(callback_group_id)

    @property
    def data(self) -> Tuple[ExecutorStructValue, ...]:
        return self._data


class PathValuesLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        nodes_loaded: NodeValuesLoaded,
        communications_loaded: CommValuesLoaded,
    ) -> None:
        paths: List[PathStructValue] = []
        for path in reader.get_paths():
            try:
                paths.append(
                    self._to_struct(path, nodes_loaded, communications_loaded)
                )
            except Error as e:
                logger.warning(f'Failed to load path. path_name={path.path_name}. {e}')

        self._data = tuple(paths)

    @staticmethod
    def _to_struct(
        path_info: PathValue,
        nodes_loaded: NodeValuesLoaded,
        comms_loaded: CommValuesLoaded,
    ) -> PathStructValue:
        node_paths_info = PathValuesLoaded._to_node_path_struct(
            path_info.node_path_values, nodes_loaded)

        child: List[Union[NodePathStructValue, CommunicationStructValue]] = []
        child.append(node_paths_info[0])
        for pub_node_path, sub_node_path in zip(node_paths_info[:-1], node_paths_info[1:]):
            topic_name = sub_node_path.subscribe_topic_name
            if topic_name is None:
                msg = 'topic name is None. '
                msg += f'publish_node: {pub_node_path.node_name}, '
                msg += f'subscribe_node: {sub_node_path.node_name}, '
                raise InvalidArgumentError(msg)
            comm_info = comms_loaded.find_communication(
                topic_name,
                pub_node_path.node_name,
                sub_node_path.node_name
            )

            child.append(comm_info)
            child.append(sub_node_path)

        return PathStructValue(path_info.path_name, tuple(child))

    @staticmethod
    def _to_node_path_struct(
        node_path_values: Tuple[NodePathValue, ...],
        nodes_loaded: NodeValuesLoaded,
    ) -> Tuple[NodePathStructValue, ...]:
        return tuple(nodes_loaded.find_node_path(_) for _ in node_path_values)

    @property
    def data(self) -> Tuple[PathStructValue, ...]:
        return self._data

    # serviceはactioに対応していないので、おかしな結果になってしまう。
    # def _insert_publishers_to_callbacks(
    #     self,
    #     publishers: List[PublisherInfo],
    #     callbacks: List[CallbackStructInfo]
    # ) -> List[CallbackStructInfo]:
    #     for publisher in publishers:
    #         if publisher.callback_name in [None, UNDEFINED_STR]:
    #             continue

    #         callback = Util.find_one(
    #             callbacks,
    #             lambda x: x.callback_name == publisher.callback_name)
    #         callback.publishers_info.append(publisher)

    #     # automatically assign if there is only one callback.
    #     if len(callbacks) == 1:
    #         callback = callbacks[0]
    #         publisher = PublisherInfo(
    #             publisher.node_name,
    #             publisher.topic_name,
    #             callback.callback_name,
    #         )
    #         callback.publishers_info.append(publisher)

    # def _find_callback(
    #     self,
    #     node_name: str,
    #     callback_name: str
    # ) -> CallbackStructInfo:
    #     for node in self.nodes:
    #         for callback in node.callbacks:
    #             if callback.node_name == node_name and callback.callback_name == callback_name:
    #                 return callback
    #     raise ItemNotFoundError(
    #         f'Failed to find callback. node_name: {node_name}, callback_name: {callback_name}')


class CallbackPathSearched():
    def __init__(
        self,
        node: NodeStructValue,
    ) -> None:
        from .graph_search import CallbackPathSearcher
        self._data: Tuple[NodePathStructValue, ...]

        searcher = CallbackPathSearcher(node)

        callbacks = node.callbacks
        paths: List[NodePathStructValue] = []

        if callbacks is not None:
            for write_callback, read_callback in product(callbacks, callbacks):
                searched_paths = searcher.search(write_callback, read_callback)
                for path in searched_paths:
                    msg = 'Path Added: '
                    msg += f'subscribe: {path.subscribe_topic_name}, '
                    msg += f'publish: {path.publish_topic_name}, '
                    msg += f'callbacks: {path.callback_names}'
                    logger.info(msg)
                paths += searched_paths

        self._data = tuple(paths)

    @property
    def data(self) -> Tuple[NodePathStructValue, ...]:
        return self._data


class TopicIgnoredReader(ArchitectureReader):
    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: List[str],
    ) -> None:
        self._reader = reader
        self._ignore_topics = ignore_topics
        self._ignore_callback_ids = self._get_ignore_callback_ids(reader, ignore_topics)

    def get_publishers(self, node: NodeValue) -> List[PublisherValue]:
        publishers: List[PublisherValue] = []
        for publisher in self._reader.get_publishers(node):
            if publisher.topic_name in self._ignore_topics:
                continue
            publishers.append(publisher)
        return publishers

    def get_callback_groups(
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
            in self._reader.get_callback_groups(node)
        ]

    def get_executors(self) -> Sequence[ExecutorValue]:
        return self._reader.get_executors()

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[Dict]:
        return self._reader.get_message_contexts(node)

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
        from tqdm import tqdm
        for node in tqdm(nodes, 'Loading callbacks'):
            node = NodeValue(node.node_name, node.node_id)

            sub = reader.get_subscription_callbacks(node)
            for sub_val in sub:
                if sub_val.subscribe_topic_name not in ignore_topic_set:
                    continue

                if sub_val.callback_id is None:
                    continue

                ignore_callback_ids.append(sub_val.callback_id)

        return set(ignore_callback_ids)

    def get_paths(self) -> Sequence[PathValue]:
        return self._reader.get_paths()

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        return self._reader.get_nodes()

    def get_subscriptions(self, node: NodeValue) -> List[SubscriptionValue]:
        subscriptions: List[SubscriptionValue] = []
        for subscription in self._reader.get_subscriptions(node):
            if subscription.topic_name in self._ignore_topics:
                continue
            subscriptions.append(subscription)
        return subscriptions

    def get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        return self._reader.get_variable_passings(node)

    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        return self._reader.get_timer_callbacks(node)

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        callbacks: List[SubscriptionCallbackValue] = []
        for subscription_callback in self._reader.get_subscription_callbacks(node):
            if subscription_callback.subscribe_topic_name in self._ignore_topics:
                continue
            callbacks.append(subscription_callback)
        return callbacks
