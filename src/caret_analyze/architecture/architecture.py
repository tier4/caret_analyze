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
        try:
            return Util.find_one(lambda x: x.node_name == node_name, self.nodes)
        except ItemNotFoundError:
            msg = 'Failed to find node. '
            msg += f'node_name: {node_name}'
            raise ItemNotFoundError(msg)

    def get_executor(self, executor_name: str) -> ExecutorStructValue:
        return Util.find_one(lambda x: x.executor_name == executor_name, self.executors)

    def get_callback_group(self, callback_group_name: str) -> CallbackGroupStructValue:
        return Util.find_one(lambda x: x.callback_group_name == callback_group_name,
                             self.callback_groups)

    @property
    def callback_groups(self) -> Tuple[CallbackGroupStructValue, ...]:
        return tuple(Util.flatten(_.callback_groups for _ in self.executors))

    @property
    def callback_group_names(self) -> Tuple[str, ...]:
        return tuple(sorted(_.callback_group_name for _ in self.callback_groups))

    @property
    def topic_names(self) -> Tuple[str, ...]:
        topic_names = {_.topic_name for _ in self.publishers}
        topic_names |= {_.topic_name for _ in self.subscriptions}
        return tuple(sorted(topic_names))

    def get_callback(self, callback_name: str) -> CallbackStructValue:
        return Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)

    @property
    def callbacks(self) -> Tuple[CallbackStructValue, ...]:
        return tuple(Util.flatten(_.callbacks for _ in self.callback_groups))

    def get_communication(
        self,
        publisher_node_name: str,
        subscription_node_name: str,
        topic_name: str
    ) -> CommunicationStructValue:
        def is_target_comm(comm: CommunicationStructValue):
            return comm.publish_node_name == publisher_node_name and \
                comm.subscribe_node_name == subscription_node_name and \
                comm.topic_name == topic_name

        return Util.find_one(is_target_comm, self.communications)

    def get_path(self, path_name: str) -> PathStructValue:
        return self._path_manager.get_named_path(path_name)

    def add_path(self, path_name: str, path_info: PathStructValue) -> None:
        self._path_manager.add_named_path(path_name, path_info)

    def remove_path(self, path_name: str) -> None:
        self._path_manager.remove_named_path(path_name)

    def update_path(self, path_name: str, path: PathStructValue) -> None:
        self._path_manager.update_named_path(path_name, path)

    @property
    def nodes(self) -> Tuple[NodeStructValue, ...]:
        return tuple(v.to_value() for v in self._nodes)

    @property
    def node_names(self) -> Tuple[str, ...]:
        return tuple(sorted(_.node_name for _ in self._nodes))

    @property
    def executors(self) -> Tuple[ExecutorStructValue, ...]:
        return tuple(v.to_value() for v in self._executors)

    @property
    def executor_names(self) -> Tuple[str, ...]:
        return tuple(sorted(_.executor_name for _ in self._executors))

    @property
    def paths(self) -> Tuple[PathStructValue, ...]:
        return self._path_manager.named_paths

    @property
    def path_names(self) -> Tuple[str, ...]:
        return tuple(sorted(_.path_name for _ in self._path_manager.named_paths))

    @property
    def communications(self) -> Tuple[CommunicationStructValue, ...]:
        return tuple(v.to_value() for v in self._communications)

    @property
    def publishers(self) -> Tuple[PublisherStructValue, ...]:
        publishers = Util.flatten(_.publishers for _ in self.nodes)
        return tuple(sorted(publishers, key=lambda x: x.topic_name))

    @property
    def subscriptions(self) -> Tuple[SubscriptionStructValue, ...]:
        subscriptions = Util.flatten(_.subscriptions for _ in self.nodes)
        return tuple(sorted(subscriptions, key=lambda x: x.topic_name))

    @property
    def summary(self) -> Summary:
        return Summary({
            'nodes': self.node_names
        })

    def export(self, file_path: str, force: bool = False):
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
        from .graph_search import NodePathSearcher
        for node_name in node_names:
            if node_name not in self.node_names:
                raise ItemNotFoundError(f'Failed to find node. {node_name}')

        default_depth = 15  # When the depth is 15, the process takes only a few seconds.
        max_node_depth = max_node_depth or default_depth

        # Print message before search
        msg_detail_page = (
            'For details, '
            'see https://tier4.github.io/CARET_doc/latest/configuration/inter_node_data_path/.'
        )
        if max_node_depth > default_depth:
            msg = (
                f"Argument 'max_node_depth' greater than {default_depth} is not recommended "
                'because it significantly increases the search time '
                'and the number of returned paths. '
            )
            msg += (
                f'If you are searching for paths that exceeds the depth {default_depth}, '
                'consider specifying an intermediate node. '
            )
            msg += msg_detail_page
            print(msg)

        # Search
        path_searcher = NodePathSearcher(
            self._nodes, self._communications, node_filter, communication_filter)
        paths = [v.to_value() for v in
                 path_searcher.search(*node_names, max_node_depth=max_node_depth)]

        # Print message after search
        msg = f'A search up to depth {max_node_depth} has been completed. '
        msg += (
            'If the paths you want to measure cannot be found, '
            'consider specifying intermediate nodes. '
        )
        msg += 'Also, if the number of paths is too large, consider filtering node/topic names. '
        msg += msg_detail_page
        print(msg)

        return paths

    def combine_path(
        self,
        path_left: PathStructValue,
        path_right: PathStructValue
        )-> PathStructValue:

        # Error processing
        if len(path_left.child)==0 or len(path_right.child)==0:
            msg = 'input path cannot be null.'
            raise InvalidArgumentError(msg)
        elif len(path_left.node_paths)==0 and len(path_right.node_paths)==0:
            msg = 'Communication and Communication cannot be combined.'
            raise InvalidArgumentError(msg)
        elif len(path_left.communications)==0 and len(path_right.communications)==0:
            msg = 'nodepath and nodepath cannot be combined.'
            raise InvalidArgumentError(msg)
        if len(path_left.node_paths)==0 or len(path_right.node_paths)==0:
            new_path = PathStructValue(None, (path_left.child, path_right.child))
            return new_path
 
        node_name = path_left.child[-1].node_name
        # if (node_name != path_right.child[0].node_name):
        #     msg = 'Last node of arg1 and first node of arg2 are difference.'
        #     raise InvalidArgumentError(msg)

        # # Add one node or comm
        # if len(path_left.child) == 1 or len(path_right.child) == 1:
        #     left_child = path_left.child[-1]
        #     right_child = path_right.child[0]
        #     # Left is node and Right is comm
        #     if isinstance(left_child, NodeStructValue) and isinstance(right_child, CommunicationStructValue):
        #         node_name = left_child.node_name
        #         node_paths = self.get_node(node_name).paths
        #         for node_path in node_paths:
        #             if node_path.

        left_child = path_left.child[-1]
        right_child = path_right.child[0]
        # Left = Node, Right = Node
        if isinstance(left_child, NodeStructValue) and isinstance(right_child, NodeStructValue):
            if len(path_left.child) == 1 or len(path_right.child) == 1:
                msg = 'Cannot combine Node and Node.'
                raise InvalidArgumentError(msg)

            node_name = path_left.child[-1].node_name
            if (node_name != path_right.child[0].node_name):
                msg = 'Last node of arg1 and first node of arg2 are difference.'
                raise InvalidArgumentError(msg)
    
            node_paths = self.get_node(node_name).paths
            left_node_pub = path_left.child[-1].publish_topic_name
            right_node_sub = path_right.child[0].subscribe_topic_name
            for node_path in node_paths:
                if (node_path.publish_topic_name == left_node_pub and 
                    node_path.subscribe_topic_name == right_node_sub):
                    break

            new_path = PathStructValue(None, (path_left.child[0:-1], node_path, path_right.chidl[1:]))
            return new_path

        elif isinstance(left_child, NodeStructValue) and isinstance(right_child, CommunicationStructValue):
            node_name = path_left.child[-1].node_name
            node_paths = self.get_node(node_name).paths
            left_node_pub = left_child.publish_topic_name
            right_comm = right_child.topic_name

            for node_path in node_paths:
                if (node_path.publish_topic_name == left_node_pub and 
                    node_path.subscribe_topic_name == right_comm):
                    break
            new_path = PathStructValue(None, (path_left.child[0:-1], node_path, path_right.chidl[1:]))
            return new_path

        elif isinstance(left_child, CommunicationStructValue) and isinstance(right_child, NodeStructValue):
            pass
        elif isinstance(left_child, CommunicationStructValue) and isinstance(right_child, CommunicationStructValue):
        

        # Combine paths



        # Core processing
        # node_name=path_left.child[-1].node_name
        # for node_path in self.get_node(node_name).paths:
        #     if node_path.publish_topic_name==path_right.child[0].publish_topic_name and node_path.subscribe_topic_name==path_left.child[-1].subscribe_topic_name:
        #         break
        # new_path = PathStructValue(None, (path_left.child[0:-1], node_path, path_right.child[1:]))
        # return new_path
        # node_paths = self.get_node(node_name).paths
        # left_node_pub = path_left.child[-1].publish_topic_name
        # # left_node_sub = path_left.child[-1].subscribe_topic_name
        # # right_node_pub = path_right.child[0].publish_topic_name
        # right_node_sub = path_right.child[0].subscribe_topic_name
        # for node_path in node_paths:
        #     if (node_path.publish_topic_name == left_node_pub and 
        #         node_path.subscribe_topic_name == right_node_sub):
        #         new_path = PathStructValue(None, (path_left.child[0:-1], node_path, path_right.chidl[1:]))
        #         return new_path
        
        # raise ItemNotFoundError()
        


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
