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

from __future__ import annotations, unicode_literals

import fnmatch

from logging import getLogger

from typing import List, Optional, Set, Union

from .callback import CallbackBase
from .callback_group import CallbackGroup
from .communication import Communication
from .executor import Executor
from .node import Node
from .path import Path
from .publisher import Publisher
from .subscription import Subscription
from ..architecture import Architecture
from ..common import Summarizable, Summary, Util
from ..exceptions import Error, InvalidArgumentError, UnsupportedTypeError
from ..infra.infra_base import InfraBase
from ..infra.interface import RecordsProvider, RuntimeDataProvider
from ..infra.lttng.lttng import Lttng
from ..infra.lttng.records_provider_lttng import RecordsProviderLttng
from ..value_objects import NodePathStructValue

logger = getLogger(__name__)


class Application(Summarizable):
    """A class that represents the entire application to be measured."""

    def __init__(
        self,
        architecture: Architecture,
        infra: InfraBase,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        architecture : Architecture
            Application architecture to be evaluated.
        infra : InfraBase
            Measurement results.

        Raises
        ------
        UnsupportedTypeError
            Occurs when the invalid infra is given.

        """
        from .runtime_loaded import RuntimeLoaded

        provider: Union[RecordsProvider, RuntimeDataProvider]

        if isinstance(infra, Lttng):
            provider = RecordsProviderLttng(infra)
        else:
            raise UnsupportedTypeError('')

        loaded = RuntimeLoaded(architecture, provider)

        self._nodes: List[Node] = loaded.nodes
        self._executors: List[Executor] = loaded.executors
        self._communications: List[Communication] = loaded.communications
        self._paths: List[Path] = loaded.paths

    @property
    def executors(self) -> List[Executor]:
        """
        Get executors.

        Returns
        -------
        List[Executor]
            All executors defined in the architecture.

        """
        return sorted(self._executors, key=lambda x: x.executor_name)

    @property
    def nodes(self) -> List[Node]:
        """
        Get nodes.

        Returns
        -------
        List[Node]
            All nodes defined in the architecture.

        """
        return sorted(self._nodes, key=lambda x: x.node_name)

    @property
    def communications(self) -> List[Communication]:
        """
        Get communications.

        Returns
        -------
        List[Communication]
            All communications defined in the architecture.

        """
        return sorted(self._communications, key=lambda x: x.topic_name)

    @property
    def publishers(self) -> List[Publisher]:
        """
        Get publishers.

        Returns
        -------
        List[Publisher]
            All publishers defined in the architecture.

        """
        publishers = Util.flatten(_.publishers for _ in self.nodes)
        return sorted(publishers, key=lambda x: x.topic_name)

    @property
    def subscriptions(self) -> List[Subscription]:
        """
        Get subscriptions.

        Returns
        -------
        List[Subscription]
            All subscriptions defined in the architecture.

        """
        subscriptions = Util.flatten(_.subscriptions for _ in self.nodes)
        return sorted(subscriptions, key=lambda x: x.topic_name)

    @property
    def paths(self) -> List[Path]:
        """
        Get paths.

        Returns
        -------
        List[Path]
            All paths defined in the architecture.

        """
        return sorted(self._paths, key=lambda x: x.path_name or '')

    @property
    def callbacks(self) -> List[CallbackBase]:
        """
        Get callbacks.

        Returns
        -------
        List[CallbackBase]
            All callbacks defined in the architecture.

        """
        cbs: List[CallbackBase] = []
        for node in self.nodes:
            if node.callbacks is not None:
                cbs += list(node.callbacks)
        return sorted(cbs, key=lambda x: x.callback_name)

    def get_path(self, path_name: str) -> Path:
        """
        Get a path that matches the condition.

        Parameters
        ----------
        path_name : str
            path name to get.
            paths and their names are defined in the architecture.

        Returns
        -------
        Path
            A path that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(path_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def get_name(x):
            return x.path_name

        return Util.find_similar_one(path_name,
                                     self.paths,
                                     get_name)

    def get_executor(
        self,
        executor_name: str
    ) -> Executor:
        """
        Get an executor that matches the condition.

        Parameters
        ----------
        executor_name : str
            executor name to get.
            The name is defined in the architecture file (ex: executor_0).

        Returns
        -------
        Executor
            executor that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        if not isinstance(executor_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def get_name(x):
            return x.executor_name

        return Util.find_similar_one(executor_name,
                                     self.executors,
                                     get_name)

    @property
    def callback_groups(
        self,
    ) -> List[CallbackGroup]:
        """
        Get callback groups.

        Returns
        -------
        List[CallbackGroup]
            All callback groups defined in the architecture.

        """
        callback_groups: List[CallbackGroup] = []
        for node in self.nodes:
            if node.callback_groups is None:
                continue
            callback_groups += node.callback_groups
        return sorted(callback_groups, key=lambda x: x.callback_group_name)

    def get_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackGroup:
        """
        Get a callback group that matches the condition.

        Parameters
        ----------
        callback_group_name : str
            callback group name to get.

        Returns
        -------
        CallbackBase
            callback group that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        if not isinstance(callback_group_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def get_name(x):
            return x.callback_group_name

        return Util.find_similar_one(callback_group_name,
                                     self.callback_groups,
                                     get_name)

    def get_communication(
        self,
        publisher_node_name: str,
        subscription_node_name: str,
        topic_name: str
    ) -> Communication:
        """
        Get communication that matches the condition.

        Parameters
        ----------
        publisher_node_name : str
            node name that publishes the topic.
        subscription_node_name : str
            node name that subscribes to the topic.
        topic_name : str
            topic name.

        Returns
        -------
        Communication
            communication that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        if not isinstance(publisher_node_name, str) or \
                not isinstance(subscription_node_name, str) or \
                not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        target_names = {'publisher_node_name': publisher_node_name,
                        'subscription_node_name': subscription_node_name,
                        'topic_name': topic_name}

        def get_names(x):
            return {'publisher_node_name': x.publish_node_name,
                    'subscription_node_name': x.subscribe_node_name,
                    'topic_name': x.topic_name}

        return Util.find_similar_one_multi_keys(target_names,
                                                self.communications,
                                                get_names)

    @property
    def topic_names(self) -> List[str]:
        """
        Get topic names.

        Returns
        -------
        List[str]
            All topic names defined in architecture.

        """
        topic_names: Set[str]
        topic_names |= {_.topic_name for _ in self.publishers}
        topic_names |= {_.topic_name for _ in self.subscriptions}
        return sorted(topic_names)

    @property
    def executor_names(self) -> List[str]:
        """
        Get executor names.

        Returns
        -------
        List[str]
            All executor names defined in the architecture.

        """
        return sorted(_.executor_name for _ in self.executors)

    @property
    def callback_group_names(self) -> List[str]:
        """
        Get callback group names.

        Returns
        -------
        List[str]
            All callback group names defined in the architecture.

        """
        return sorted(_.callback_group_name for _ in self.callback_groups)

    @property
    def path_names(self) -> List[str]:
        """
        Get path names.

        Returns
        -------
        List[str]
            App path names defined in the architecture.

        """
        return sorted(_.path_name for _ in self.paths)

    @property
    def callback_names(self) -> List[str]:
        """
        Get callback names.

        Returns
        -------
        List[str]
            All callback names defined in the architecture.

        """
        return sorted(_.callback_name for _ in self.callbacks)

    def get_node_path(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str]
    ) -> NodePathStructValue:
        """
        Get a node path that matches the condition.

        Parameters
        ----------
        node_name : str
            node name to get.
        subscribe_topic_name : Optional[str]
            topic name which the node subscribes.
        publish_topic_name : Optional[str]
            topic name which the node publishes.

        Returns
        -------
        NodePathStructValue
            node path that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(node_name, str) or \
                not isinstance(subscribe_topic_name, str) or \
                not isinstance(publish_topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        target_name = {'node_name': node_name,
                       'subscribe_topic_name': subscribe_topic_name,
                       'publish_topic_name': publish_topic_name}

        def get_names(x):
            return {'node_name': x.node_name,
                    'subscribe_topic_name': x.subscribe_topic_name,
                    'publish_topic_name': x.publish_topic_name}

        return Util.find_similar_one_multi_keys(target_name,
                                                self.node_paths,
                                                get_names)

    def get_communications(
        self,
        topic_name: str
    ) -> List[Communication]:
        """
        Get communication that matches the condition.

        Parameters
        ----------
        topic_name : str
            topic name to get.

        Returns
        -------
        List[Communication]
            communications that match the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.

        """
        if not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        comms = Util.filter_items(
            lambda x: x.topic_name == topic_name,
            self.communications
        )
        if (len(comms) == 0):
            Util.find_similar_one(topic_name,
                                  self.communications,
                                  lambda x: x.topic_name)

        return sorted(comms, key=lambda x: x.topic_name)

    def get_publishers(
        self,
        topic_name: str
    ) -> List[Publisher]:
        """
        Get publishers that match the condition.

        Parameters
        ----------
        topic_name : str
            topic name to get.

        Returns
        -------
        List[Publisher]
            publishers that match the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.

        """
        if not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        comms = self.get_communications(topic_name)
        pubs = [comm.publisher for comm in comms]

        return sorted(pubs, key=lambda x: x.topic_name)

    def get_subscriptions(
        self,
        topic_name: str
    ) -> List[Subscription]:
        """
        Get subscriptions that match the condition.

        Parameters
        ----------
        topic_name : str
            topic name to get.

        Returns
        -------
        List[Publisher]
            subscriptions that match the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Failed to find an item that matches the condition.

        """
        if not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        comms = self.get_communications(topic_name)
        subs = [comm.subscription for comm in comms]

        return sorted(subs, key=lambda x: x.topic_name)

    def get_node_paths(
        self,
        node_name: str,
    ) -> List[NodePathStructValue]:
        """
        Get node paths.

        Parameters
        ----------
        node_name : str
            node name to get.

        Returns
        -------
        List[NodePathStructValue]
            node paths that match the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.

        """
        if not isinstance(node_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        node_paths = Util.filter_items(
            lambda x: x.node_name == node_name,
            self.node_paths
        )
        if (len(node_paths) == 0):
            Util.find_similar_one(node_name,
                                  self.node_paths,
                                  lambda x: x.node_name)

        return sorted(node_paths, key=lambda x: x.node_name)

    @property
    def node_paths(self) -> List[NodePathStructValue]:
        """
        Get paths.

        Returns
        -------
        List[NodePathStructValue]
            app node paths defined in the entire application.

        """
        return Util.flatten([_.paths for _ in self.nodes])

    def get_node(self, node_name: str) -> Node:
        """
        Get a node that matches the condition.

        Parameters
        ----------
        node_name : str
            node name to get.

        Returns
        -------
        Node
            A node that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(node_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def get_name(x):
            return x.node_name

        return Util.find_similar_one(node_name,
                                     self.nodes,
                                     get_name)

    def get_callback(self, callback_name: str) -> CallbackBase:
        """
        Get a callback that matches the condition.

        Parameters
        ----------
        callback_name : str
            callback name to get.

        Returns
        -------
        CallbackBase
            callback that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(callback_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def get_name(x):
            return x.callback_name

        return Util.find_similar_one(callback_name,
                                     self.callbacks,
                                     get_name)

    def get_callbacks(self, *callback_names: str) -> List[CallbackBase]:
        """
        Get callbacks that match the condition.

        Parameters
        ----------
        callback_names : Tuple[str, ...]
            callback names to get.

        Returns
        -------
        List[CallbackBase]
            callbacks that match the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        def is_match_regex(callback: CallbackBase):
            return fnmatch.fnmatch(callback.callback_name, callback_name)

        callbacks = []
        for callback_name in callback_names:
            try:
                if '*' in callback_name or '?' in callback_name:
                    callbacks += Util.filter_items(is_match_regex, self.callbacks)
                else:
                    callbacks.append(self.get_callback(callback_name))
            except Error:
                msg = 'Failed to identify callback. Skip loading.'
                msg += f'callback_name: {callback_name}'
                logger.warning(msg)

        return callbacks

    @property
    def node_names(self) -> List[str]:
        """
        Get node names.

        Returns
        -------
        List[str]
            All node names defined in the architecture.

        """
        return sorted(c.node_name for c in self.nodes)

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            Summary info.

        """
        return Summary({
            'nodes': self.node_names
        })
