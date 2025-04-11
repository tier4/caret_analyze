# Copyright 2021 TIER IV, Inc.
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

from collections.abc import Sequence
from itertools import groupby, product
from logging import getLogger, INFO

from .graph_search import CallbackPathSearcher
from .reader_interface import ArchitectureReader, UNDEFINED_STR
from .struct import (CallbackChainStruct,
                     CallbackGroupStruct,
                     CallbackStruct,
                     CommunicationStruct,
                     ExecutorStruct,
                     MessageContextStruct,
                     NodePathStruct, NodeStruct, PathStruct,
                     PublisherStruct,
                     ServiceCallbackStruct, ServiceStruct,
                     SubscriptionCallbackStruct, SubscriptionStruct,
                     TimerCallbackStruct, TimerStruct,
                     VariablePassingStruct)
from ..common import Progress, Util
from ..exceptions import (Error, InvalidArgumentError, InvalidReaderError,
                          InvalidYamlFormatError, ItemNotFoundError,
                          MultipleItemFoundError, UnsupportedTypeError)
from ..value_objects import (CallbackGroupValue,
                             CallbackValue,
                             ExecutorValue,
                             NodePathValue, NodeValue,
                             NodeValueWithId, PathValue,
                             PublisherValue,
                             ServiceCallbackValue,
                             ServiceValue,
                             SubscriptionCallbackValue,
                             SubscriptionValue,
                             TimerCallbackValue,
                             TimerValue, VariablePassingValue,
                             )

logger = getLogger(__name__)


def indexed_name(base_name: str, i: int, num_digit: int):
    index_str = str(i).zfill(num_digit)
    return f'{base_name}_{index_str}'


class ArchitectureLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: list[str],
        max_callback_construction_order_on_path_searching: int
    ) -> None:

        topic_ignored_reader = TopicIgnoredReader(reader, ignore_topics)

        self._nodes: list[NodeStruct]
        nodes_loaded = NodeValuesLoaded(topic_ignored_reader,
                                        max_callback_construction_order_on_path_searching)

        self._nodes = nodes_loaded.data

        execs_loaded = ExecutorValuesLoaded(topic_ignored_reader, nodes_loaded)
        self._executors: list[ExecutorStruct]
        self._executors = execs_loaded.data

        comms_loaded = CommValuesLoaded(nodes_loaded)
        self._communications: list[CommunicationStruct]
        self._communications = comms_loaded.data

        paths_loaded = PathValuesLoaded(
            topic_ignored_reader, nodes_loaded, comms_loaded)
        self._named_paths: list[PathStruct]
        self._paths = paths_loaded.data

        # TODO(hsgwa): Workaround implementation to minimize merge conflicts. Need to refactor.
        # Remove caret_trace related nodes.
        self._nodes = [
            node
            for node
            in self._nodes
            if 'caret_trace_' not in node.node_name
        ]
        # Remove caret_trace related executors.
        self._executors = [
            executor
            for executor
            in self._executors
            if all('caret_trace_' not in cbg.node_name for cbg in executor.callback_groups)
        ]

        self._ignore_service()

        return None

    @property
    def paths(self) -> list[PathStruct]:
        """
        Get paths.

        Returns
        -------
        list[PathStruct]
            PathValuesLoaded data.

        """
        return self._paths

    @property
    def executors(self) -> list[ExecutorStruct]:
        """
        Get executors.

        Returns
        -------
        list[ExecutorStruct]
            ExecutorValuesLoaded data.

        """
        return self._executors

    @property
    def nodes(self) -> list[NodeStruct]:
        """
        Get nodes.

        Returns
        -------
        list[NodeStruct]
            NodeValuesLoaded data.

        """
        return self._nodes

    @property
    def communications(self) -> list[CommunicationStruct]:
        """
        Get communications.

        Returns
        -------
        list[NodeStruct]
            CommValuesLoaded data.

        """
        return self._communications

    def _ignore_service(self) -> None:
        # for node
        nodes = list(self._nodes)
        for n in nodes:
            ignored_callback_groups = []
            original_callback_groups = n.callback_groups
            if original_callback_groups is not None:
                for cbg in original_callback_groups:
                    if '/service_only_callback_group_'  \
                            in cbg.callback_group_name:
                        continue
                    cbg._callbacks = [cb for cb in cbg.callbacks
                                      if cb.service_name is None]
                    ignored_callback_groups.append(cbg)

            n._callback_groups = ignored_callback_groups

        self._nodes = nodes

        # for executor
        executors = list(self._executors)
        for executor in executors:
            cbg_values = [cbg for cbg in executor.callback_groups
                          if '/service_only_callback_group_'
                          not in cbg.callback_group_name]
            # for cbg in exec.callback_groups:
            executor._cbg_values = cbg_values


class CommValuesLoaded():

    def __init__(
        self,
        nodes_loaded: NodeValuesLoaded
    ) -> None:
        node_values = nodes_loaded.data

        data: list[CommunicationStruct] = []
        pub_sub_pair = product(node_values, node_values)

        node_pub: NodeStruct
        node_sub: NodeStruct
        for node_pub, node_sub in Progress.tqdm(pub_sub_pair, 'Searching communications.'):
            for pub, sub in product(node_pub.publishers, node_sub.subscriptions):
                if pub.topic_name != sub.topic_name:
                    continue
                data.append(
                    self._to_struct(nodes_loaded, pub, sub, node_pub, node_sub)
                )
        self._data = data

    @staticmethod
    def _to_struct(
        nodes_loaded: NodeValuesLoaded,
        pub: PublisherStruct,
        sub: SubscriptionStruct,
        node_pub: NodeStruct,
        node_sub: NodeStruct,
    ) -> CommunicationStruct:
        try:
            callbacks_pub = None
            is_target_pub_cb = CommValuesLoaded.IsTargetPubCallback(pub)
            callback_values = nodes_loaded.get_callbacks(pub.node_name)
            callbacks_pub = list(Util.filter_items(is_target_pub_cb, callback_values))
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

        return CommunicationStruct(
            node_pub, node_sub, pub, sub, callbacks_pub, callback_sub)

    @property
    def data(self) -> list[CommunicationStruct]:
        """
        Get data.

        Returns
        -------
        list[CommunicationStruct]
            Communication struct data.

        """
        return self._data

    def find_communication(
        self,
        topic_name: str,
        publish_node_name: str,
        publisher_construction_order: int | None,
        subscribe_node_name: str,
        subscription_construction_order: int | None,
    ) -> CommunicationStruct:
        """
        Find communication.

        Parameters
        ----------
        topic_name : str
            topic name.
        publish_node_name : str
            publish node name.
        publisher_construction_order : int | None
            construction order of publisher.
        subscribe_node_name : str
            subscribe node name.
        subscription_construction_order : int | None
            construction order of subscription.

        Returns
        -------
        CommunicationStruct
            Found communication struct.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        def is_target(comm: CommunicationStruct):
            return comm.publish_node_name == publish_node_name and \
                comm.subscribe_node_name == subscribe_node_name and \
                comm.topic_name == topic_name and \
                comm.publisher_construction_order == publisher_construction_order and \
                comm.subscription_construction_order == subscription_construction_order
        try:
            return Util.find_one(is_target, self.data)
        except ItemNotFoundError:
            msg = 'Failed to find communication. '
            msg += f'topic_name: {topic_name}, '
            msg += f'publish_node_name: {publish_node_name}, '
            msg += f'subscribe_node_name: {subscribe_node_name}, '
            msg += f'publisher_construction_order: {publisher_construction_order}, '
            msg += f'subscription_construction_order: {subscription_construction_order}, '

            raise ItemNotFoundError(msg)

    class IsTargetPubCallback:

        def __init__(self, publish: PublisherStruct):
            self._publish = publish

        def __call__(self, callback: CallbackStruct) -> bool:
            if callback.publish_topics is None:
                return False
            for publish_info in callback.publish_topics:
                if self._publish.topic_name == publish_info.topic_name and \
                        self._publish.construction_order == publish_info.construction_order:
                    return True
            return False

    class IsTargetSubCallback:

        def __init__(self, subscription: SubscriptionStruct):
            self._subscription = subscription

        def __call__(self, callback: CallbackStruct) -> bool:
            if callback.subscribe_topic_name is None:
                return False

            match = self._subscription.topic_name == callback.subscribe_topic_name
            if self._subscription.callback_name:
                match &= self._subscription.callback_name == callback.callback_name
            if self._subscription.callback:
                match &= self._subscription.callback.construction_order == \
                    callback.construction_order

            return match


class NodeValuesLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
        max_callback_construction_order_on_path_searching: int
    ) -> None:
        self._reader = reader
        nodes_struct: list[NodeStruct] = []
        self._cb_loaded: list[CallbacksLoaded] = []
        self._cbg_loaded: list[CallbackGroupsLoaded] = []

        nodes = reader.get_nodes()
        nodes = sorted(nodes, key=lambda x: x.node_name)
        try:
            self._validate(nodes)
        except InvalidReaderError as e:
            logger.warning(e)

        nodes = self._remove_duplicated(nodes)

        for node in Progress.tqdm(nodes, 'Loading nodes.'):
            try:
                node, cb_loaded, cbg_loaded = self._create_node(
                    node,
                    reader,
                    max_callback_construction_order_on_path_searching
                )
                nodes_struct.append(node)
                self._cb_loaded.append(cb_loaded)
                self._cbg_loaded.append(cbg_loaded)
            except Error as e:
                logger.warning(f'Failed to load node. node_name = {node.node_name}, {e}')

        nodes_struct = sorted(nodes_struct, key=lambda x: x.node_name)
        self._data = nodes_struct

    @staticmethod
    def _remove_duplicated(nodes: Sequence[NodeValueWithId]) -> Sequence[NodeValueWithId]:
        nodes_: list[NodeValueWithId] = []
        node_names: set[str] = set()
        node_ids: set[str] = set()
        for node in nodes:
            # remove if name and id are not unique
            if node.node_name not in node_names and node.node_id not in node_ids:
                node_names.add(node.node_name)
                node_ids.add(node.node_id)
                nodes_.append(node)
        return nodes_

    @staticmethod
    def _validate(nodes: Sequence[NodeValueWithId]):
        # validate node name uniqueness.
        node_names = [n.node_name for n in nodes]
        duplicated_name: list[str] = []
        duplicated_id: list[str] = []
        for node_name, group in groupby(node_names):
            if len(list(group)) >= 2:
                duplicated_name.append(node_name)
        if len(duplicated_name) >= 1:
            msg = f'Duplicated node name. {duplicated_name}. Use first node only.'
            raise InvalidReaderError(msg)

        # validate node id uniqueness.
        node_ids = [n.node_id for n in nodes]
        for node_id, group in groupby(node_ids):
            if len(list(group)) >= 2:
                duplicated_id.append(node_id)
        if len(duplicated_id) >= 1:
            msg = f'Duplicated node id. {duplicated_id}. Use first node only.'
            raise InvalidReaderError(msg)

    @property
    def data(self) -> list[NodeStruct]:
        """
        Get data.

        Returns
        -------
        list[NodeStruct]
            Node struct data.

        """
        return self._data

    def get_callbacks(
        self,
        node_name: str
    ) -> list[CallbackStruct]:
        """
        Get callbacks.

        Parameters
        ----------
        node_name : str
            node name.

        Returns
        -------
        list[CallbackStruct]
            Found callback struct.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        try:
            cb_loaded: CallbacksLoaded
            cb_loaded = Util.find_one(lambda x: x.node_name == node_name, self._cb_loaded)
            return cb_loaded.data
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def find_node(self, node_name: str) -> NodeStruct:
        """
        Find node.

        Parameters
        ----------
        node_name : str
            node name.

        Returns
        -------
        NodeStruct
            Found node struct.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self.data)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def find_node_path(
        self,
        node_path_value: NodePathValue,
    ) -> NodePathStruct:
        """
        Find node path.

        Parameters
        ----------
        node_path_value : NodePathValue
            node path value.

        Returns
        -------
        NodePathStruct
            Found node path struct.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        def is_target(value: NodePathStruct):
            return (value.publish_topic_name == node_path_value.publish_topic_name and
                    value.subscribe_topic_name == node_path_value.subscribe_topic_name and
                    value.publisher_construction_order ==
                    node_path_value.publisher_construction_order and
                    value.subscription_construction_order ==
                    node_path_value.subscription_construction_order
                    )

        node_value = self.find_node(node_path_value.node_name)
        try:
            return Util.find_one(is_target, node_value.paths)
        except ItemNotFoundError:
            msg = 'Failed to find node path value. '
            msg += f' node_name: {node_path_value.node_name}'
            msg += f' publish_topic_name: {node_path_value.publish_topic_name}'
            msg += f' subscribe_topic_name: {node_path_value.subscribe_topic_name}'
            msg += f' publisher_construction_order: \
                {node_path_value.publisher_construction_order}'
            msg += f' subscription_construction_order: \
                {node_path_value.subscription_construction_order}'
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
    ) -> CallbackGroupStruct:
        """
        Find callback group.

        Parameters
        ----------
        callback_group_id : str
            callback group id.

        Returns
        -------
        CallbackGroupStruct
            Found callback group struct.

        Raises
        ------
        ItemNotFoundError
            Failed to find callback group.

        """
        for cbg_loaded in self._cbg_loaded:
            try:
                return cbg_loaded.find_callback_group(callback_group_id)
            except ItemNotFoundError:
                pass

        msg = f'Failed to find callback group. callback_group_id: {callback_group_id}.\n'
        node_names_and_cb_symbols = self._reader.get_node_names_and_cb_symbols(callback_group_id)
        for i, node_name_and_cb_symbol in enumerate(node_names_and_cb_symbols):
            msg += f'\t|node name {i}| {node_name_and_cb_symbol[0]}.\n'
            msg += f'\t|callback symbol {i}| {node_name_and_cb_symbol[1]}.\n'
        raise ItemNotFoundError(msg)

    def find_callback(
        self,
        callback_id: str
    ) -> CallbackStruct:
        """
        Find callback.

        Parameters
        ----------
        callback_id : str
            callback id.

        Returns
        -------
        CallbackStruct
            Found callback struct.

        Raises
        ------
        ItemNotFoundError
            Failed to find callback.

        """
        for cb_loaded in self._cb_loaded:
            try:
                return cb_loaded.find_callback(callback_id)
            except ItemNotFoundError:
                pass
        raise ItemNotFoundError(f'Failed to find callback. callback_id={callback_id}')

    def find_callbacks(
        self,
        callback_ids: list[str]
    ) -> list[CallbackStruct]:
        """
        Find callbacks.

        Parameters
        ----------
        callback_ids : list[str]
            callback ids.

        Returns
        -------
        list[CallbackStruct]
            Found callback struct.

        Raises
        ------
        ItemNotFoundError
            Failed to find callback.

        """
        callbacks: list[CallbackStruct] = []
        for cb_loaded in self._cb_loaded:
            callbacks += cb_loaded.search_callbacks(callback_ids)

        if len(callbacks) < len(callback_ids):
            raise ItemNotFoundError(f'Failed to find callback. callback_ids={callback_ids}')

        return callbacks

    @staticmethod
    def _create_node(
        node: NodeValue,
        reader: ArchitectureReader,
        max_callback_construction_order_on_path_searching: int
    ) -> tuple[NodeStruct, CallbacksLoaded, CallbackGroupsLoaded]:

        callbacks_loaded = CallbacksLoaded(reader, node)

        publishers: list[PublisherStruct]
        publishers = PublishersLoaded(reader, callbacks_loaded, node).data

        subscriptions: list[SubscriptionStruct]
        subscriptions = SubscriptionsLoaded(reader, callbacks_loaded, node).data

        services: list[ServiceStruct]
        services = ServicesLoaded(reader, callbacks_loaded, node).data

        timers: list[TimerStruct]
        timers = TimersLoaded(reader, callbacks_loaded, node).data

        callback_groups: list[CallbackGroupStruct]
        cbg_loaded = CallbackGroupsLoaded(reader, callbacks_loaded, node)
        callback_groups = cbg_loaded.data

        variable_passings: list[VariablePassingStruct]
        variable_passings = VariablePassingsLoaded(
            reader, callbacks_loaded, node).data

        node_struct = NodeStruct(
            node.node_name, list(publishers), list(subscriptions), list(services),
            list(timers), [], list(callback_groups), list(variable_passings)
        )

        try:
            node_paths = \
                    NodeValuesLoaded._search_node_paths(
                        node_struct,
                        reader.get_message_contexts(node),
                        max_callback_construction_order_on_path_searching
                    )
            node_path_added = NodeStruct(
                node_struct.node_name, node_struct.publishers,
                node_struct.subscriptions,
                node_struct.services,
                node_struct.timers,
                list(node_paths), node_struct.callback_groups,
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
        node: NodeStruct,
        contexts: Sequence[dict],
        max_callback_construction_order: int
    ) -> list[NodePathStruct]:

        node_paths: list[NodePathStruct] = []

        # add callback-graph paths
        logger.info('[callback_chain]')
        node_paths += list(CallbackPathSearched(node, max_callback_construction_order).data)

        # add pub-sub pair graph paths
        logger.info('\n[pub-sub pair]')
        pubs = node.publishers
        subs = node.subscriptions
        node_path_pub_sub_pairs = NodePathCreated(subs, pubs).data
        for node_path in node_path_pub_sub_pairs:
            added_pub_sub_construction_order_pairs = [(
                n.publish_topic_name,
                n.publisher_construction_order,
                n.subscribe_topic_name,
                n.subscription_construction_order,
            ) for n in node_paths]
            pub_sub_construction_order_pair = (node_path.publish_topic_name,
                                               node_path.publisher_construction_order,
                                               node_path.subscribe_topic_name,
                                               node_path.subscription_construction_order)

            if pub_sub_construction_order_pair not in added_pub_sub_construction_order_pairs:
                node_paths.append(node_path)

                logger.info(
                    'Path Added: '
                    f'subscribe: {node_path.subscribe_topic_name}, '
                    f'subscription_order: {node_path.subscription_construction_order}, '
                    f'publish: {node_path.publish_topic_name}, '
                    f'publisher_order: {node_path.publisher_construction_order}, '
                )

        # add dummy node paths
        logger.info('\n[dummy paths]')
        for pub in node.publishers:
            added_pub_sub_pairs = [(n.publish_topic_name, n.subscribe_topic_name)
                                   for n in node_paths]
            node_path = NodePathStruct(
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
            node_path = NodePathStruct(
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

        message_contexts: list[MessageContextStruct] = []
        message_contexts += list(MessageContextsLoaded(contexts, node, node_paths).data)

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

        return node_paths

    @staticmethod
    def _message_context_assigned(
        node_paths: Sequence[NodePathStruct],
        message_contexts: Sequence[MessageContextStruct],
    ) -> list[NodePathStruct]:
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

                node_paths_[i] = NodePathStruct(
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
        context_dicts: Sequence[dict],
        node: NodeStruct,
        node_paths: Sequence[NodePathStruct]
    ) -> None:
        self._data: list[MessageContextStruct]
        data: list[MessageContextStruct] = []

        pub_sub_pairs: list[tuple[str | None, str | None]] = []
        for context_dict in context_dicts:
            try:
                context_type = context_dict['context_type']
                if context_type == UNDEFINED_STR:
                    logger.info(f'message context is UNDEFINED. {context_dict}')
                    continue
                publisher_topic_name = context_dict['publisher_topic_name']
                subscription_topic_name = context_dict['subscription_topic_name']
                subscription_construction_order = 0 if 'subscription_construction_order' \
                    not in context_dict else context_dict['subscription_construction_order']
                publisher_construction_order = 0 if 'publisher_construction_order' \
                    not in context_dict else context_dict['publisher_construction_order']
                node_path = self.get_node_path(node_paths,
                                               publisher_topic_name,
                                               subscription_topic_name,
                                               subscription_construction_order,
                                               publisher_construction_order)
                data.append(self._to_struct(context_dict, node_path))
                pair = (node_path.publish_topic_name, node_path.subscribe_topic_name)
                pub_sub_pairs.append(pair)
            except Error as e:
                logger.warning(e)

        for context in self._create_callback_chain(node_paths):
            pub_sub_pair = (context.publisher_topic_name, context.subscription_topic_name)
            if context not in data and pub_sub_pair not in pub_sub_pairs:
                data.append(context)

        self._data = data

    @staticmethod
    def get_node_path(
        node_paths: Sequence[NodePathStruct],
        publisher_topic_name: str,
        subscription_topic_name: str,
        subscription_construction_order: int,
        publisher_construction_order: int
    ) -> NodePathStruct:
        """
        Get node path.

        Parameters
        ----------
        node_paths : Sequence[NodePathStruct]
            node paths.
        publisher_topic_name : str
            publisher topic name.
        subscription_topic_name : str
            subscription topic name.
        publisher_construction_order : int
            construction order of publisher.
        subscription_construction_order : int
            construction order of subscription.

        Returns
        -------
        NodePathStruct
            Found node path struct.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        def is_target(path: NodePathValue):
            return path.publish_topic_name == publisher_topic_name and \
                path.subscribe_topic_name == subscription_topic_name and \
                path.subscription_construction_order == subscription_construction_order and \
                path.publisher_construction_order == publisher_construction_order
        return Util.find_one(is_target, node_paths)

    @property
    def data(self) -> list[MessageContextStruct]:
        """
        Get data.

        Returns
        -------
        list[MessageContextStruct]
            Message context struct.

        """
        return self._data

    @staticmethod
    def _create_callback_chain(
        node_paths: Sequence[NodePathStruct]
    ) -> list[MessageContextStruct]:
        chains: list[MessageContextStruct] = []
        for path in node_paths:
            if path.callbacks is not None:
                chains.append(
                    CallbackChainStruct(
                        path.node_name,
                        {},
                        path.subscription,
                        path.publisher,
                        path.callbacks)
                )
        return chains

    @staticmethod
    def _to_struct(
        context_dict: dict,
        node_path: NodePathStruct
    ) -> MessageContextStruct:
        type_name = context_dict['context_type']

        try:
            return MessageContextStruct.create_instance(
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
        subscription_values: list[SubscriptionStruct],
        publisher_values: list[PublisherStruct],
    ) -> None:
        paths: list[NodePathStruct] = []
        for sub, pub in product(subscription_values, publisher_values):
            paths.append(
                NodePathStruct(sub.node_name, sub, pub, None, None)
            )

        self._data = paths

    @property
    def data(self) -> list[NodePathStruct]:
        """
        Get data.

        Returns
        -------
        list[NodePathStruct]
            Node path struct.

        """
        return self._data


class PublishersLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        publisher_values = reader.get_publishers(node)
        self._data = [self._to_struct(callbacks_loaded, pub)
                      for pub in publisher_values]

    @staticmethod
    def _to_struct(
        callbacks_loaded: CallbacksLoaded,
        publisher_value: PublisherValue
    ) -> PublisherStruct:

        pub_callbacks: list[CallbackStruct] = []
        if publisher_value.callback_ids is not None:
            for callback_id in publisher_value.callback_ids:
                if callback_id == UNDEFINED_STR:
                    continue
                pub_callbacks.append(
                    callbacks_loaded.find_callback(callback_id))

        # May be assigned incorrectly for service or client-only nodes
        callbacks = PublishersLoaded._get_callbacks(callbacks_loaded)
        srv_ignored_callbacks = [c for c in callbacks if not isinstance(c, ServiceCallbackStruct)]
        if len(pub_callbacks) == 0 and len(srv_ignored_callbacks) == 1:
            pub_callbacks.append(callbacks[0])

        for callback in callbacks:
            if callback.publish_topics is None or len(callback.publish_topics) == 0:
                continue

            for publish_topic in callback.publish_topics:
                if publisher_value.topic_name == publish_topic.topic_name and \
                        callback not in pub_callbacks:
                    pub_callbacks.append(callback)

        return PublisherStruct(
            publisher_value.node_name,
            publisher_value.topic_name,
            callback_values=pub_callbacks,
            construction_order=publisher_value.construction_order
        )

    @staticmethod
    def _get_callbacks(
        callbacks_loaded: CallbacksLoaded,
    ) -> list[CallbackStruct]:
        def is_user_defined(callback: CallbackStruct):
            if isinstance(callback, SubscriptionCallbackStruct):
                if callback.subscribe_topic_name == '/parameter_events':
                    return False
            return True

        callbacks = callbacks_loaded.data
        return Util.filter_items(is_user_defined, callbacks)

    @property
    def data(self) -> list[PublisherStruct]:
        """
        Get data.

        Returns
        -------
        list[PublisherStruct]
            Publisher struct.

        """
        return self._data


class SubscriptionsLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        # duplicate check for callback_id
        subscription_values = reader.get_subscriptions(node)
        try:
            self._validate(subscription_values)
        except InvalidReaderError as e:
            logger.warning(e)

        subscription_values = self._remove_duplicated(subscription_values)

        self._data = [self._to_struct(callbacks_loaded, sub)
                      for sub in subscription_values]

    @staticmethod
    def _validate(subscriptions: Sequence[SubscriptionValue]):
        # validate callback id uniqueness.
        callback_ids: set[str] = set()
        duplicated: list[str] = []
        for subscription in subscriptions:
            if subscription.callback_id is not None:
                if subscription.callback_id not in callback_ids:
                    callback_ids.add(subscription.callback_id)
                else:
                    duplicated.append(subscription.callback_id)
        if len(duplicated) > 0:
            msg = f'Duplicated callback id. {duplicated}. Use first subscription only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(subscriptions: Sequence[SubscriptionValue]) -> list[SubscriptionValue]:
        ids_: list[SubscriptionValue] = []
        callback_ids: set[str] = set()
        for subscription in subscriptions:
            # remove if callback id are not unique
            if subscription.callback_id is None:
                ids_.append(subscription)
            elif subscription.callback_id not in callback_ids:
                callback_ids.add(subscription.callback_id)
                ids_.append(subscription)
        return ids_

    def _to_struct(
        self,
        callbacks_loaded: CallbacksLoaded,
        subscription_value: SubscriptionValue
    ) -> SubscriptionStruct:
        sub_callback: SubscriptionCallbackStruct | None = None

        if subscription_value.callback_id is not None:
            sub_callback = callbacks_loaded.find_callback(
                subscription_value.callback_id)  # type: ignore

            assert isinstance(sub_callback, SubscriptionCallbackStruct)

        return SubscriptionStruct(
            node_name=subscription_value.node_name,
            topic_name=subscription_value.topic_name,
            callback_info=sub_callback,
            construction_order=subscription_value.construction_order
        )

    @property
    def data(self) -> list[SubscriptionStruct]:
        """
        Get data.

        Returns
        -------
        list[SubscriptionStruct]
            Subscription struct.

        """
        return self._data


class ServicesLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        # duplicate check for callback_id
        services_values = reader.get_services(node)
        try:
            self._validate(services_values)
        except InvalidReaderError as e:
            logger.warning(e)

        services_values = self._remove_duplicated(services_values)

        self._data = [self._to_struct(callbacks_loaded, srv)
                      for srv in services_values]

    @staticmethod
    def _validate(services: Sequence[ServiceValue]):
        # validate callback id uniqueness.
        callback_ids: set[str] = set()
        duplicated: list[str] = []
        for service in services:
            if service.callback_id is not None:
                if service.callback_id not in callback_ids:
                    callback_ids.add(service.callback_id)
                else:
                    duplicated.append(service.callback_id)
        if len(duplicated) > 0:
            msg = f'Duplicated callback id. {duplicated}. Use first service only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(services: Sequence[ServiceValue]) -> list[ServiceValue]:
        ids_: list[ServiceValue] = []
        callback_ids: set[str] = set()
        for service in services:
            # remove if callback id are not unique
            if service.callback_id is None:
                ids_.append(service)
            elif service.callback_id not in callback_ids:
                callback_ids.add(service.callback_id)
                ids_.append(service)
        return ids_

    def _to_struct(
        self,
        callbacks_loaded: CallbacksLoaded,
        service_value: ServiceValue
    ) -> ServiceStruct:
        srv_callback: ServiceCallbackStruct | None = None

        if service_value.callback_id is not None:
            srv_callback = callbacks_loaded.find_callback(
                service_value.callback_id)  # type: ignore

            assert isinstance(srv_callback, ServiceCallbackStruct)

        return ServiceStruct(
            node_name=service_value.node_name,
            service_name=service_value.service_name,
            callback_info=srv_callback,
            construction_order=service_value.construction_order
        )

    @property
    def data(self) -> list[ServiceStruct]:
        """
        Get data.

        Returns
        -------
        list[ServiceStruct]
            Service struct.

        """
        return self._data


class TimersLoaded:
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        timer_values = reader.get_timers(node)

        # duplicate check for callback_id
        try:
            self._validate(timer_values)
        except InvalidReaderError as e:
            logger.warning(e)

        timer_values = self._remove_duplicated(timer_values)

        self._data = [self._to_struct(callbacks_loaded, timer)
                      for timer in timer_values]

    @staticmethod
    def _validate(timers: Sequence[TimerValue]):
        # validate callback id uniqueness.
        callback_ids: set[str] = set()
        duplicated: list[str] = []
        for timer in timers:
            if timer.callback_id is not None:
                if timer.callback_id not in callback_ids:
                    callback_ids.add(timer.callback_id)
                else:
                    duplicated.append(timer.callback_id)
        if len(duplicated) > 0:
            msg = f'Duplicated callback id. {duplicated}. Use first timer only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(timers: Sequence[TimerValue]) -> list[TimerValue]:
        ids_: list[TimerValue] = []
        callback_ids: set[str] = set()
        for timer in timers:
            # remove if callback id are not unique
            if timer.callback_id is None:
                ids_.append(timer)
            elif timer.callback_id not in callback_ids:
                callback_ids.add(timer.callback_id)
                ids_.append(timer)
        return ids_

    def _to_struct(
        self,
        callbacks_loaded: CallbacksLoaded,
        timer_value: TimerValue
    ) -> TimerStruct:

        timer_callback: TimerCallbackStruct | None = None

        if timer_value.callback_id is not None:
            timer_callback = callbacks_loaded.find_callback(
                timer_value.callback_id)  # type: ignore

        return TimerStruct(
            node_name=timer_value.node_name,
            period_ns=timer_value.period,
            callback_info=timer_callback,
            construction_order=timer_value.construction_order
        )

    @property
    def data(self) -> list[TimerStruct]:
        """
        Get data.

        Returns
        -------
        list[TimerStruct]
            Timer struct.

        """
        return self._data


class VariablePassingsLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue,
    ) -> None:
        data: list[VariablePassingStruct] = []

        for var_pass in reader.get_variable_passings(node):
            if var_pass.callback_id_read == UNDEFINED_STR or\
                    var_pass.callback_id_write == UNDEFINED_STR:
                continue
            data.append(
                VariablePassingStruct(
                    node.node_name,
                    callback_write=callbacks_loaded.find_callback(
                        var_pass.callback_id_write),
                    callback_read=callbacks_loaded.find_callback(
                        var_pass.callback_id_read)
                )
            )

        self._data: list[VariablePassingStruct]
        self._data = data

    @property
    def data(self) -> list[VariablePassingStruct]:
        """
        Get data.

        Returns
        -------
        list[VariablePassingStruct]
            Variable passing struct.

        """
        return self._data


class CallbackGroupsLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        callbacks_loaded: CallbacksLoaded,
        node: NodeValue
    ) -> None:
        self._data: dict[str, CallbackGroupStruct] = {}

        _cbg_dict: dict[CallbackGroupValue, int] = {}
        _srv_only_cbg_dict: dict[CallbackGroupValue, int] = {}

        # ignore callback_group containing only service callbacks
        def _is_service_only_callbackgroup(cbg: CallbackGroupValue):
            cbs = self._get_callbacks(callbacks_loaded, cbg)
            srv_cb_count = len([cb for cb in cbs
                                if isinstance(cb, ServiceCallbackStruct)])
            cb_count = len(cbs)
            return srv_cb_count == cb_count and srv_cb_count != 0

        # duplicate check for callback_group_id
        callback_groups = reader.get_callback_groups(node)
        try:
            self._validate_id(callback_groups)
        except InvalidReaderError as e:
            logger.warning(e)

        callback_groups = self._remove_duplicated(callback_groups)

        duplicated_names: set[str] = set()

        for cbg in callback_groups:
            self._validate(cbg, node)

            cbg_name: str

            if not _is_service_only_callbackgroup(cbg):
                if cbg.callback_group_name is None:
                    cbg_name = f'{node.node_name}/callback_group_{len(_cbg_dict)}'
                    _cbg_dict[cbg] = len(_cbg_dict)
                    # TODO(hsgwa): Handle duplicate callback names with existing callback names.
                else:
                    cbg_name = cbg.callback_group_name
            else:
                cbg_name = cbg.callback_group_name  \
                    or f'{node.node_name}/service_only_callback_group_' \
                    + f'{len(_srv_only_cbg_dict)}'
                _srv_only_cbg_dict[cbg] = len(_srv_only_cbg_dict)

            cbg_struct = CallbackGroupStruct(
                cbg.callback_group_type,
                node.node_name,
                self._get_callbacks(callbacks_loaded, cbg),
                cbg_name
            )

            try:
                self._validate_name(cbg_name, duplicated_names)
                duplicated_names.add(cbg_name)
                self._data[cbg.callback_group_id] = cbg_struct
            except InvalidReaderError as e:
                logger.warning(e)

    @staticmethod
    def _validate_name(name: str, names: set[str]):
        if name in names:
            msg = f'Duplicated callback group name. {name}. Use first callback group only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _validate_id(callback_groups: Sequence[CallbackGroupValue]):
        # validate callback group id uniqueness.
        callback_ids: set[str] = set()
        duplicated: list[str] = []
        for callback_group in callback_groups:
            if callback_group.callback_group_id not in callback_ids:
                callback_ids.add(callback_group.callback_group_id)
            else:
                duplicated.append(callback_group.callback_group_id)
        if len(duplicated) > 0:
            msg = f'Duplicated callback group id. {duplicated}. Use first callback group only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(callback_groups: Sequence[CallbackGroupValue]) \
            -> list[CallbackGroupValue]:
        ids_: list[CallbackGroupValue] = []
        callback_ids: set[str] = set()
        for callback_group in callback_groups:
            # remove if callback id are not unique
            if callback_group.callback_group_id not in callback_ids:
                callback_ids.add(callback_group.callback_group_id)
                ids_.append(callback_group)
        return ids_

    @staticmethod
    def _validate(cbg: CallbackGroupValue, node: NodeValue):
        # TODO: add callback group id validation

        if len(cbg.callback_ids) != len(set(cbg.callback_ids)):
            raise InvalidReaderError(f'duplicated callback id. {node}, {cbg}')

    @property
    def data(self) -> list[CallbackGroupStruct]:
        """
        Get data.

        Returns
        -------
        list[CallbackGroupStruct]
            Callback group struct.

        """
        return list(self._data.values())

    def find_callback_group(self, callback_group_id: str):
        """
        Find callback group.

        Parameters
        ----------
        callback_group_id : str
            callback group id.

        Raises
        ------
        ItemNotFoundError
            Failed to find callback group.

        """
        if callback_group_id in self._data:
            return self._data[callback_group_id]

        raise ItemNotFoundError('')

    def _get_callbacks(
        self,
        callbacks_loaded: CallbacksLoaded,
        callback_group: CallbackGroupValue,
    ) -> list[CallbackStruct]:
        callback_structs: list[CallbackStruct] = []
        for callback_id in callback_group.callback_ids:
            # Ensure that the callback information exists.
            callback_struct = callbacks_loaded.find_callback(callback_id)
            callback_structs.append(callback_struct)
        return callback_structs


class CallbacksLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
        node: NodeValue,
    ) -> None:
        self._node = node
        callbacks: list[CallbackValue] = []
        callbacks += reader.get_timer_callbacks(node)
        callbacks += reader.get_subscription_callbacks(node)
        callbacks += reader.get_service_callbacks(node)

        self._validate(callbacks)
        self._callbacks = callbacks

        self._callback_count: dict[CallbackValue, int] = {}
        # "_srv_callback_count" will be integrated
        # into "_callback_count" when the service is officially supported.
        self._srv_callback_count: dict[CallbackValue, int] = {}
        self._cb_dict: dict[str, CallbackStruct] = {}

        # TODO(hsgwa): Checking for unique constraints on id and name

        # Service callbacks are handled specially until formal support for the service is provided
        # callback_num = Util.num_digit(len(callbacks))
        callback_num = Util.num_digit(len(reader.get_timer_callbacks(node))
                                      + len(reader.get_subscription_callbacks(node)))
        srv_callback_num = Util.num_digit(len(reader.get_service_callbacks(node)))

        for callback in callbacks:
            if callback.callback_id is None:
                continue
            self._cb_dict[callback.callback_id] = self._to_struct(
                callback, callback_num, srv_callback_num)

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node.node_name

    @property
    def data(self) -> list[CallbackStruct]:
        """
        Get data.

        Returns
        -------
        list[CallbackStruct]
            Callback struct.

        """
        return list(self._cb_dict.values())

    def _to_struct(
        self,
        callback: CallbackValue,
        callback_num: int,
        srv_callback_num: int
    ) -> CallbackStruct:

        if isinstance(callback, TimerCallbackValue):
            self._callback_count[callback] = self._callback_count.get(
                callback, len(self._callback_count))
            callback_count = self._callback_count[callback]
            indexed = indexed_name(
                f'{self.node_name}/callback', callback_count, callback_num)
            callback_name = callback.callback_name or indexed
            # TODO(hsgwa): Handle duplicate callback names with existing callback names.

            return TimerCallbackStruct(
                node_name=callback.node_name,
                symbol=callback.symbol,
                period_ns=callback.period_ns,
                publish_topics=None if callback.publish_topics is None
                else list(callback.publish_topics),
                callback_name=callback_name,
                construction_order=callback.construction_order
            )
        if isinstance(callback, SubscriptionCallbackValue):
            self._callback_count[callback] = self._callback_count.get(
                callback, len(self._callback_count))
            callback_count = self._callback_count[callback]
            indexed = indexed_name(
                f'{self.node_name}/callback', callback_count, callback_num)
            callback_name = callback.callback_name or indexed
            # TODO(hsgwa): Handle duplicate callback names with existing callback names.
            return SubscriptionCallbackStruct(
                node_name=callback.node_name,
                symbol=callback.symbol,
                subscribe_topic_name=callback.subscribe_topic_name,
                publish_topics=None if callback.publish_topics is None
                else list(callback.publish_topics),
                callback_name=callback_name,
                construction_order=callback.construction_order
            )
        # Service callbacks support "read" only, not "export".
        # To avoid affecting exported files, special handling is done for service callbacks.
        # When the service is officially supported, the special processing will be removed.
        if isinstance(callback, ServiceCallbackValue):
            self._srv_callback_count[callback] = self._srv_callback_count.get(
                callback, len(self._srv_callback_count))
            callback_count = self._srv_callback_count[callback]
            indexed = indexed_name(
                f'{self.node_name}/service_callback', callback_count, srv_callback_num)
            callback_name = callback.callback_name or indexed
            return ServiceCallbackStruct(
                node_name=callback.node_name,
                symbol=callback.symbol,
                service_name=callback.service_name,
                publish_topics=None if callback.publish_topics is None
                else list(callback.publish_topics),
                callback_name=callback_name,
                construction_order=callback.construction_order
            )
        raise UnsupportedTypeError('Unsupported callback type')

    def _validate(self, callbacks: list[CallbackValue]) -> None:
        # check node name
        for callback in callbacks:
            if callback.node_id != self._node.node_id:
                msg = 'reader returns invalid callback value. '
                msg += f'get [{self._node.node_id}] value returns [{callback.node_id}]'
                raise InvalidReaderError(msg)

        # check callback name
        cb_names: list[str] = [
            cb.callback_name for cb in callbacks if cb.callback_name is not None]
        if len(cb_names) != len(set(cb_names)):
            msg = f'Duplicated callback names. node_name: {self._node.node_name}\n'
            for name in set(cb_names):
                if cb_names.count(name) >= 2:
                    msg += f'callback name: {name} \n'
            raise InvalidReaderError(msg)

        # check callback id
        cb_ids: list[str] = [
            cb.callback_id
            for cb
            in callbacks
            if cb.callback_id is not None
        ]
        if len(cb_ids) != len(set(cb_ids)):
            msg = f'Duplicated callback id. node_name: {self._node.node_name}\n'
            for cb_id in set(cb_ids):
                if cb_ids.count(cb_id) >= 2:
                    msg += f'callback id: {cb_id} \n'
            raise InvalidReaderError(msg)

    def find_callback(
        self,
        callback_id: str
    ) -> CallbackStruct:
        """
        Find callback.

        Parameters
        ----------
        callback_id : str
            callback id.

        Raises
        ------
        ItemNotFoundError
            Failed to find callback.

        """
        if callback_id in self._cb_dict.keys():
            return self._cb_dict[callback_id]

        msg = 'Failed to find callback. '
        msg += f'node_name: {self._node.node_name}, '
        msg += f'callback_id: {callback_id}, '
        raise ItemNotFoundError(msg)

    def search_callbacks(
        self,
        callback_ids: list[str]
    ) -> list[CallbackStruct]:
        """
        Search callbacks.

        Parameters
        ----------
        callback_ids : list[str, ...]
            target callback ids

        Returns
        -------
        list[CallbackStruct, ...]
            If the callback is not found, it returns an empty tuple.

        """
        callbacks: list[CallbackStruct] = []

        for callback_id in callback_ids:
            if callback_id not in self._cb_dict.keys():
                continue
            callbacks.append(self.find_callback(callback_id))

        return callbacks


class ExecutorValuesLoaded():

    def __init__(
        self,
        reader: ArchitectureReader,
        nodes_loaded: NodeValuesLoaded,
    ) -> None:
        execs: list[ExecutorStruct] = []

        exec_vals = reader.get_executors()

        # duplicate check for executor name
        try:
            self._validate(exec_vals)
        except InvalidReaderError as e:
            logger.warning(e)
        exec_vals = self._remove_duplicated(exec_vals)

        num_digit = Util.num_digit(len(exec_vals))
        name_index = 0

        for executor in exec_vals:
            executor_name: str
            if executor.executor_name is None:
                executor_name = indexed_name('executor', name_index, num_digit)
                name_index += 1
                # TODO(hsgwa): Handle duplicate callback names with existing callback names.
            else:
                executor_name = executor.executor_name

            try:
                execs.append(
                    self._to_struct(executor_name, executor, nodes_loaded)
                )
            except Error as e:
                logger.warning(
                    'Failed to load executor. skip loading. '
                    f'executor_name = {executor_name}. {e}')

        self._data = execs

    @staticmethod
    def _validate(executors: Sequence[ExecutorValue]):
        # validate executor name uniqueness.
        executor_names: set[str] = set()
        duplicated: list[str] = []
        for executor in executors:
            if executor.executor_name is not None:
                if executor.executor_name not in executor_names:
                    executor_names.add(executor.executor_name)
                else:
                    duplicated.append(executor.executor_name)
        if len(duplicated) > 0:
            msg = f'Duplicated executor name. {duplicated}. Use first executor only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(executors: Sequence[ExecutorValue]) -> list[ExecutorValue]:
        executors_: list[ExecutorValue] = []
        executor_names: set[str] = set()
        for executor in executors:
            # remove if name are not unique
            if executor.executor_name is None:
                executors_.append(executor)
            elif executor.executor_name not in executor_names:
                executor_names.add(executor.executor_name)
                executors_.append(executor)
        return executors_

    @staticmethod
    def _to_struct(
        executor_name: str,
        executor: ExecutorValue,
        nodes_loaded: NodeValuesLoaded,
    ) -> ExecutorStruct:
        callback_group_values: list[CallbackGroupStruct] = []

        for cbg_id in executor.callback_group_ids:
            try:
                callback_group_values.append(
                    ExecutorValuesLoaded._find_struct_callback_group(
                        cbg_id, nodes_loaded)
                )
            except Error as e:
                # This is caused by the data missing after rclcpp_callback_group_add_xxx
                # when filtering topics in "caret_trace".
                logger.info(
                    f'Failed to load executor. executor_name: {executor_name}')
                # This warning occurs frequently,
                # but currently does not significantly affect behavior.
                # Therefore, the log level is temporarily lowered.
                logger.log(INFO, e)

        return ExecutorStruct(
            executor.executor_type,
            callback_group_values,
            executor_name,
        )

    @staticmethod
    def _find_struct_callback_group(
        callback_group_id: str,
        nodes_loaded: NodeValuesLoaded,
    ) -> CallbackGroupStruct:
        return nodes_loaded.find_callback_group(callback_group_id)

    @property
    def data(self) -> list[ExecutorStruct]:
        """
        Get data.

        Returns
        -------
        list[ExecutorStruct]
            Executor struct.

        """
        return self._data


class PathValuesLoaded():
    def __init__(
        self,
        reader: ArchitectureReader,
        nodes_loaded: NodeValuesLoaded,
        communications_loaded: CommValuesLoaded,
    ) -> None:
        paths: list[PathStruct] = []

        # duplicate check for path name
        path_values = reader.get_paths()
        try:
            self._validate(path_values)
        except InvalidReaderError as e:
            logger.warning(e)

        path_values = self._remove_duplicated(path_values)

        for path in path_values:
            try:
                paths.append(
                    self._to_struct(path, nodes_loaded, communications_loaded)
                )
            except Error as e:
                logger.warning(f'Failed to load path. path_name={path.path_name}. {e}')

        self._data = paths

    @staticmethod
    def _validate(path_vals: Sequence[PathValue]):
        # validate path name uniqueness.
        path_names: set[str] = set()
        duplicated: list[str] = []
        for path in path_vals:
            if path.path_name not in path_names:
                path_names.add(path.path_name)
            else:
                duplicated.append(path.path_name)
        if len(duplicated) > 0:
            msg = f'Duplicated path name. {duplicated}. Use first path only.'
            raise InvalidReaderError(msg)

    @staticmethod
    def _remove_duplicated(path_vals: Sequence[PathValue]) -> list[PathValue]:
        paths_: list[PathValue] = []
        path_names: set[str] = set()
        for path in path_vals:
            # remove if name and id are not unique
            if path.path_name not in path_names:
                path_names.add(path.path_name)
                paths_.append(path)
        return paths_

    @staticmethod
    def _to_struct(
        path_info: PathValue,
        nodes_loaded: NodeValuesLoaded,
        comms_loaded: CommValuesLoaded,
    ) -> PathStruct:
        node_paths_info = PathValuesLoaded._to_node_path_struct(
            list(path_info.node_path_values), nodes_loaded)

        child: list[NodePathStruct | CommunicationStruct] = []
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
                pub_node_path.publisher_construction_order,
                sub_node_path.node_name,
                sub_node_path.subscription_construction_order
            )

            child.append(comm_info)
            child.append(sub_node_path)

        return PathStruct(path_info.path_name, child)

    @staticmethod
    def _to_node_path_struct(
        node_path_values: list[NodePathValue],
        nodes_loaded: NodeValuesLoaded,
    ) -> list[NodePathStruct]:
        return [nodes_loaded.find_node_path(_) for _ in node_path_values]

    @property
    def data(self) -> list[PathStruct]:
        """
        Get data.

        Returns
        -------
        list[PathStruct]
            Path struct.

        """
        return self._data


class CallbackPathSearched():
    def __init__(
        self,
        node: NodeStruct,
        max_callback_construction_order: int
    ) -> None:
        self._data: list[NodePathStruct]

        searcher = CallbackPathSearcher(node, max_callback_construction_order)

        callbacks = node.callbacks
        paths: list[NodePathStruct] = []

        if callbacks is not None:
            skip_count = 0
            max_ignored_construction_order = 0
            for write_callback, read_callback in product(callbacks, callbacks):
                if max_callback_construction_order != 0:
                    if write_callback.construction_order > max_callback_construction_order or \
                       read_callback.construction_order > max_callback_construction_order:
                        skip_count += 1
                        max_ignored_construction_order = max(
                            max_ignored_construction_order,
                            write_callback.construction_order
                        )
                        continue
                searched_paths = searcher.search(write_callback, read_callback, node)
                for path in searched_paths:
                    msg = 'Path Added: '
                    msg += f'subscribe: {path.subscribe_topic_name}, '
                    msg += f'publish: {path.publish_topic_name}, '
                    msg += f'callbacks: {path.callback_names}'
                    logger.info(msg)
                paths += searched_paths

            if skip_count:
                logger.warning(
                    f'{node.node_name} '
                    f'contains callbacks whose construction_order are greater than '
                    f'{max_callback_construction_order}. '
                    f'{skip_count} paths are ignored as a result. '
                    f'(max construction_order: {max_ignored_construction_order})'
                )

        self._data = paths

    @property
    def data(self) -> list[NodePathStruct]:
        """
        Get data.

        Returns
        -------
        list[NodePathStruct]
            Node path struct.

        """
        return self._data


class TopicIgnoredReader(ArchitectureReader):
    def __init__(
        self,
        reader: ArchitectureReader,
        ignore_topics: list[str],
    ) -> None:
        self._reader = reader
        self._ignore_topics = ignore_topics
        self._ignore_callback_ids = self._get_ignore_callback_ids(reader, ignore_topics)

    def get_publishers(self, node: NodeValue) -> list[PublisherValue]:
        """
        Get publishers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[PublisherValue]
            Publisher value.

        """
        publishers: list[PublisherValue] = []
        for publisher in self._reader.get_publishers(node):
            if publisher.topic_name in self._ignore_topics:
                continue
            publishers.append(publisher)
        return publishers

    def get_timers(self, node: NodeValue) -> list[TimerValue]:
        """
        Get timers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[TimerValue]
            Timer value.

        """
        timers: list[TimerValue] = []
        for timer in self._reader.get_timers(node):
            timers.append(timer)
        return timers

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback groups.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[CallbackGroupValue]
            Callback group value.

        """
        return [
            CallbackGroupValue(
                cbg.callback_group_type.type_name,
                cbg.node_name,
                cbg.node_id,
                tuple(
                    callback_id
                    for callback_id
                    in cbg.callback_ids
                    if callback_id not in self._ignore_callback_ids),
                cbg.callback_group_id,
                callback_group_name=cbg.callback_group_name
            )
            for cbg
            in self._reader.get_callback_groups(node)
        ]

    def get_executors(self) -> Sequence[ExecutorValue]:
        """
        Get executors.

        Returns
        -------
        Sequence[ExecutorValue]
            Executor value.

        """
        return self._reader.get_executors()

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[dict]:
        """
        Get message contexts.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[dict]
            Message contexts.

        """
        return self._reader.get_message_contexts(node)

    def _filter_callback_id(
        self,
        callback_ids: list[str]
    ) -> list[str]:
        def is_not_ignored(callback_id: str):
            return callback_id not in self._ignore_callback_ids

        return list(Util.filter_items(is_not_ignored, callback_ids))

    @staticmethod
    def _get_ignore_callback_ids(
        reader: ArchitectureReader,
        ignore_topics: list[str]
    ) -> set[str]:
        ignore_callback_ids: list[str] = []
        ignore_topic_set = set(ignore_topics)

        nodes = reader.get_nodes()
        for node in Progress.tqdm(nodes, 'Loading callbacks'):
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
        """
        Get paths.

        Returns
        -------
        Sequence[PathValue]
            Path value.

        """
        return self._reader.get_paths()

    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[tuple[str | None, str | None]]:
        """
        Get node names and callback symbols.

        Parameters
        ----------
        callback_group_id : str
            Target callback group id.

        Returns
        -------
        Sequence[tuple[str | None, str | None]]
            Node names and callback symbols.

        """
        return self._reader.get_node_names_and_cb_symbols(callback_group_id)

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            Node value with id.

        """
        return self._reader.get_nodes()

    def get_subscriptions(self, node: NodeValue) -> list[SubscriptionValue]:
        """
        Get subscriptions.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[SubscriptionValue]
            Subscription value.

        """
        subscriptions: list[SubscriptionValue] = []
        for subscription in self._reader.get_subscriptions(node):
            if subscription.topic_name in self._ignore_topics:
                continue
            subscriptions.append(subscription)
        return subscriptions

    def get_services(self, node: NodeValue) -> list[ServiceValue]:
        """
        Get services.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[ServiceValue]
            Service value.

        """
        return list(self._reader.get_services(node))

    def get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        """
        Get variable passings.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[VariablePassingValue]
            Variable passing value.

        """
        return self._reader.get_variable_passings(node)

    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        """
        Get timer callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[TimerCallbackValue]
            Timer callback value.

        """
        return self._reader.get_timer_callbacks(node)

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        """
        Get subscription callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[SubscriptionCallbackValue]
            Subscription callback value.

        """
        callbacks: list[SubscriptionCallbackValue] = []
        for subscription_callback in self._reader.get_subscription_callbacks(node):
            if subscription_callback.subscribe_topic_name in self._ignore_topics:
                continue
            callbacks.append(subscription_callback)
        return callbacks

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        """
        Get service callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[ServiceCallbackValue]
            Service callback value.

        """
        return self._reader.get_service_callbacks(node)
