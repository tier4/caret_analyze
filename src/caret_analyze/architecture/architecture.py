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

import logging
from typing import Callable, Collection, Dict, List, Optional, Tuple, Union


from .architecture_exporter import ArchitectureExporter
from .reader_interface import IGNORE_TOPICS
from .struct import (CommunicationStruct, ExecutorStruct,
                     NodeStruct)
from .struct.callback import TimerCallbackStruct
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError, ItemNotFoundError
from ..value_objects import (CallbackGroupStructValue, CallbackStructValue,
                             CommunicationStructValue, ExecutorStructValue,
                             NodeStructValue, PathStructValue, PublisherStructValue,
                             SubscriptionStructValue)


class Architecture(Summarizable):

    def __init__(
        self,
        file_type: str,
        file_path: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        file_type : str
            Architecture file type ['yaml', 'lttng']
        file_path : str
            File path.

        """
        from .architecture_reader_factory import ArchitectureReaderFactory
        from .architecture_loaded import ArchitectureLoaded

        # /parameter events and /rosout measurements are not yet supported.
        ignore_topics: List[str] = IGNORE_TOPICS

        reader = ArchitectureReaderFactory.create_instance(
            file_type, file_path)
        loaded = ArchitectureLoaded(reader, ignore_topics)

        self._nodes: Tuple[NodeStruct, ...] = loaded.nodes
        self._communications: Tuple[CommunicationStruct, ...] = loaded.communications
        self._executors: Tuple[ExecutorStruct, ...] = loaded.executors
        self._path_manager = NamedPathManager(tuple(v.to_value() for v in loaded.paths))
        self._verify(self._nodes)

    def get_node(self, node_name: str) -> NodeStructValue:
        """
        Get a node that matches the condition.

        Parameters
        ----------
        node_name : str
            Node name to get.

        Returns
        -------
        NodeStructValue
            A node that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self.nodes)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def get_executor(self, executor_name: str) -> ExecutorStructValue:
        """
        Get an executor that matches the condition.

        Parameters
        ----------
        executor_name : str
            executor name to get.
            The name is defined in the architecture file (ex: executor_0).

        Returns
        -------
        ExecutorStructValue
            executor that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        return Util.find_one(lambda x: x.executor_name == executor_name, self.executors)

    def get_callback_group(self, callback_group_name: str) -> CallbackGroupStructValue:
        """
        Get a callback group that matches the condition.

        Parameters
        ----------
        callback_group_name : str
            callback group name to get.

        Returns
        -------
        CallbackGroupStructValue
            callback group that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        return Util.find_one(lambda x: x.callback_group_name == callback_group_name,
                             self.callback_groups)

    @property
    def callback_groups(self) -> Tuple[CallbackGroupStructValue, ...]:
        """
        Get callback groups.

        Returns
        -------
        Tuple[CallbackGroupStructValue, ...]
            All callback groups defined in the architecture.

        """
        return tuple(Util.flatten(_.callback_groups for _ in self.executors))

    @property
    def callback_group_names(self) -> Tuple[str, ...]:
        """
        Get callback group names.

        Returns
        -------
        Tuple[str, ...]
            All callback group names defined in the architecture.

        """
        return tuple(sorted(_.callback_group_name for _ in self.callback_groups))

    @property
    def topic_names(self) -> Tuple[str, ...]:
        """
        Get topic names.

        Returns
        -------
        Tuple[str, ...]
            All topic names defined in architecture.

        """
        return tuple(sorted({_.topic_name for _ in self.communications}))

    def get_callback(self, callback_name: str) -> CallbackStructValue:
        """
        Get a callback that matches the condition.

        Parameters
        ----------
        callback_name : str
            callback name to get.

        Returns
        -------
        CallbackStructValue
            callback that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.
        """
        return Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)

    @property
    def callbacks(self) -> Tuple[CallbackStructValue, ...]:
        """
        Get callbacks.

        Returns
        -------
        Tuple[CallbackStructValue, ...]
            All callbacks defined in the architecture.

        """
        return tuple(Util.flatten(_.callbacks for _ in self.callback_groups))

    def get_communication(
        self,
        publisher_node_name: str,
        subscription_node_name: str,
        topic_name: str
    ) -> CommunicationStructValue:
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
        CommunicationStructValue
            communication that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Failed to find an item that matches the condition.
        MultipleItemFoundError
            Failed to identify an item that matches the condition.

        """
        def is_target_comm(comm: CommunicationStructValue):
            return comm.publish_node_name == publisher_node_name and \
                comm.subscribe_node_name == subscription_node_name and \
                comm.topic_name == topic_name

        return Util.find_one(is_target_comm, self.communications)

    def get_path(self, path_name: str) -> PathStructValue:
        """
        Get a path that matches the condition.

        Parameters
        ----------
        path_name : str
            path name to get.
            paths and their names are defined in the architecture.

        Returns
        -------
        PathStructValue
            A path that matches the condition.

        Raises
        ------
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        return self._path_manager.get_named_path(path_name)

    def add_path(self, path_name: str, path_info: PathStructValue) -> None:
        """
        Add path to be evaluated.

        Parameters
        ----------
        path_name : str
            Path name to identify.
        path_info : PathStructValue
            Target path definition.

        """
        self._path_manager.add_named_path(path_name, path_info)

    def remove_path(self, path_name: str) -> None:
        """
        Remove path to be evaluated.

        Parameters
        ----------
        path_name : str
            Path name to remove.

        """
        self._path_manager.remove_named_path(path_name)

    def update_path(self, path_name: str, path: PathStructValue) -> None:
        """
        Update path name.

        Parameters
        ----------
        path_name : str
            updated path name.
        path : PathStructValue
            Path definition to be updated.

        """
        self._path_manager.update_named_path(path_name, path)

    @property
    def nodes(self) -> Tuple[NodeStructValue, ...]:
        """
        Get nodes.

        Returns
        -------
        Tuple[NodeStructValue, ...]
            All nodes defined in the architecture.

        """
        return tuple(v.to_value() for v in self._nodes)

    @property
    def node_names(self) -> Tuple[str, ...]:
        """
        Get node names.

        Returns
        -------
        Tuple[str, ...]
            All node names defined in the architecture.

        """
        return tuple(sorted(_.node_name for _ in self._nodes))

    @property
    def executors(self) -> Tuple[ExecutorStructValue, ...]:
        """
        Get executors.

        Returns
        -------
        Tuple[ExecutorStructValue, ...]
            All executors defined in the architecture.

        """
        return tuple(v.to_value() for v in self._executors)

    @property
    def executor_names(self) -> Tuple[str, ...]:
        """
        Get executor names.

        Returns
        -------
        Tuple[str, ...]
            All executor names defined in the architecture.

        """
        return tuple(sorted(_.executor_name for _ in self._executors))

    @property
    def paths(self) -> Tuple[PathStructValue, ...]:
        """
        Get paths.

        Returns
        -------
        Tuple[PathStructValue, ...]
            All paths defined in the architecture.

        """
        return self._path_manager.named_paths

    @property
    def path_names(self) -> Tuple[str, ...]:
        """
        Get path names.

        Returns
        -------
        Tuple[str, ...]
            App path names defined in the architecture.

        """
        return tuple(sorted(_.path_name for _ in self._path_manager.named_paths))

    @property
    def communications(self) -> Tuple[CommunicationStructValue, ...]:
        """
        Get communications.

        Returns
        -------
        Tuple[CommunicationStructValue, ...]
            All communications defined in the architecture.

        """
        return tuple(v.to_value() for v in self._communications)

    @property
    def publishers(self) -> Tuple[PublisherStructValue, ...]:
        """
        Get publishers.

        Returns
        -------
        Tuple[PublisherStructValue, ...]
            All publishers defined in the architecture.

        """
        publishers = Util.flatten(_.publishers for _ in self.nodes)
        return tuple(sorted(publishers, key=lambda x: x.topic_name))

    @property
    def subscriptions(self) -> Tuple[SubscriptionStructValue, ...]:
        """
        Get subscriptions.

        Returns
        -------
        Tuple[SubscriptionStructValue, ...]
            All subscriptions defined in the architecture.

        """
        subscriptions = Util.flatten(_.subscriptions for _ in self.nodes)
        return tuple(sorted(subscriptions, key=lambda x: x.topic_name))

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

    def export(self, file_path: str, force: bool = False):
        """
        Export architecture file.

        Parameters
        ----------
        file_path : str
            Destination file path.
        force : bool, optional
            If True, overwriting is allowed, by default False.

        """
        exporter = ArchitectureExporter(
            self.nodes, self.executors, self.paths, force)
        exporter.execute(file_path)

    def search_paths(
        self,
        *node_names: str,
        max_node_depth: Optional[int] = None,
        node_filter: Optional[Callable[[str], bool]] = None,
        communication_filter: Optional[Callable[[str], bool]] = None,
    ) -> List[PathStructValue]:
        """
        Search paths to be evaluated.

        Paths are searched between specified nodes in a one stroke traversal on the node graph.

        Parameters
        ----------
        node_names : str
            The name of the node through which the path passes.
            At least two node names are required: the starting node name and the end node name.
            More than three names can be input to specify the nodes to be passed through.
        max_node_depth : Optional[int], optional
            Maximum search depth between specified nodes.
            If a relay node is specified, it is used as the depth between relay nodes.
            Default value is None.
        node_filter : Optional[Callable[[str], bool]], optional
            Function to exclude from path search by node name.
            Nodes that are True are included in the search and nodes that are False are excluded
            from the search. Default value is None.
        communication_filter : Optional[Callable[[str], bool]], optional
            Function to exclude from path search by topic name.
            Topics that are True are included in the search and topics that are False are excluded
            from the search. Default value is None.

        Returns
        -------
        List[PathStructValue]
            List of paths for search results.

        Raises
        ------
        ItemNotFoundError
            Occurs when the specified node name does not exist.

        """
        from .graph_search import NodePathSearcher
        for node_name in node_names:
            if node_name not in self.node_names:
                raise ItemNotFoundError(f'Failed to find node. {node_name}')

        path_searcher = NodePathSearcher(
            self._nodes, self._communications, node_filter, communication_filter)
        return [
            v.to_value() for v in path_searcher.search(*node_names, max_node_depth=max_node_depth)]

    @staticmethod
    def _verify(nodes: Collection[NodeStruct]) -> None:
        from collections import Counter

        # verify callback parameter uniqueness
        for node in nodes:
            callbacks = node.callbacks
            if callbacks is None:
                continue

            callback_params: List[Tuple[str, Union[str, int]]] = []
            for callback in callbacks:
                cb_type = callback.callback_type_name
                cb_param: Union[str, int]
                if isinstance(callback, TimerCallbackStruct):
                    cb_param = callback.period_ns
                else:
                    continue
                callback_params.append((cb_type, cb_param))

            counter = Counter(callback_params)

            for uniqueness_violated in [param for param, count in counter.items() if count >= 2]:
                logging.warning(
                    ('Duplicate parameter callback found. '
                     f'node_name: {node.node_name}, '
                     f'callback_type: {uniqueness_violated[0]}'
                     f'period_ns: {uniqueness_violated[1]}'))


"""
    def rename_callback(src: str, dest: str):
        raise NotImplementedError('')

    def rename_node(src: str, dest: str):
        raise NotImplementedError('')

    def rename_path(src: str, dest: str):
        raise NotImplementedError('')

    def rename_executor(src: str, dest: str):
        raise NotImplementedError('')

    def rename_topic(src: str, dest: str):
        raise NotImplementedError('')
"""


class NamedPathManager():

    def __init__(self, paths: Tuple[PathStructValue, ...]) -> None:
        self._named_paths: Dict[str, PathStructValue] = {}
        for path in paths:
            if path.path_name is None:
                continue
            self._named_paths[path.path_name] = path

    @property
    def named_paths(self) -> Tuple[PathStructValue, ...]:
        return tuple(self._named_paths.values())

    def get_named_path(self, path_name: str) -> PathStructValue:
        if path_name not in self._named_paths.keys():
            raise InvalidArgumentError(f'Failed to get named path. {path_name} not exist.')
        return self._named_paths[path_name]

    def add_named_path(self, path_name: str, path_info: PathStructValue):
        if path_name in self._named_paths.keys():
            raise InvalidArgumentError('Failed to add named path. Duplicate path name.')
        named_path_info = PathStructValue(path_name, path_info.child)
        self._named_paths[path_name] = named_path_info

    def remove_named_path(self, path_name: str):
        if path_name not in self._named_paths.keys():
            raise InvalidArgumentError(f'Failed to remove named path. {path_name} not exist.')
        del self._named_paths[path_name]

    def update_named_path(self, path_name: str, path_info: PathStructValue):
        if path_info.path_name is None:
            raise InvalidArgumentError('path_info.path_name is None')

        self.remove_named_path(path_info.path_name)
        self.add_named_path(path_name, path_info)
