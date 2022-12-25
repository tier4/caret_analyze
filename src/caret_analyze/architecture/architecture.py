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
from typing import Callable, Collection, List, Optional, Tuple, Union

from multimethod import multimethod as singledispatchmethod

from .architecture_exporter import ArchitectureExporter

from .reader_interface import IGNORE_TOPICS
from .struct import (CommunicationStruct, ExecutorStruct,
                     NodePathStruct, NodeStruct, PathStruct)
from .struct.callback import CallbackStruct, TimerCallbackStruct
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError, ItemNotFoundError, UnsupportedTypeError
from ..value_objects import (CallbackGroupStructValue, CallbackStructValue,
                             CommunicationStructValue, ExecutorStructValue,
                             NodePathStructValue, NodeStructValue, PathStructValue,
                             PublisherStructValue, ServiceStructValue, SubscriptionStructValue)


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

        self._nodes: List[NodeStruct] = loaded.nodes
        self._communications: List[CommunicationStruct] = loaded.communications
        self._executors: List[ExecutorStruct] = loaded.executors
        self._paths = loaded.paths
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
        if path_name not in self.path_names:
            raise InvalidArgumentError(f'Failed to get named path. {path_name} not exist.')

        named_path: PathStruct = Util.find_one(lambda x: x._path_name == path_name, self._paths)
        return named_path.to_value()

    def add_path(self, path_name: str, path_info: PathStructValue) -> None:
        if path_name in self.path_names:
            raise InvalidArgumentError('Failed to add named path. Duplicate path name.')

        child: List[Union[NodePathStruct, CommunicationStruct]] = []

        for c in path_info.child:
            if isinstance(c, NodePathStructValue):
                node_name = c.node_name
                publish_topic_name = c.publish_topic_name
                subscribe_topic_name = c.subscribe_topic_name

                def is_target_node(node: NodeStruct):
                    return node_name == node.node_name

                def is_target_node_path(node_path: NodePathStruct):
                    return publish_topic_name == node_path.publish_topic_name and \
                        subscribe_topic_name == node_path.subscribe_topic_name

                node: NodeStruct = Util.find_one(is_target_node, self._nodes)
                node_path: NodePathStruct = Util.find_one(is_target_node_path, node.paths)
                child.append(node_path)

            elif isinstance(c, CommunicationStructValue):
                publish_node_name = c.publish_node_name
                subscribe_node_name = c.subscribe_node_name
                topic_name = c.topic_name

                def is_target_comm(comm: CommunicationStruct):
                    return publish_node_name == comm.publish_node_name and \
                        subscribe_node_name == comm.subscribe_node_name and \
                        topic_name == comm.topic_name

                comm: CommunicationStruct = \
                    Util.find_one(is_target_comm, self._communications)
                child.append(comm)

            else:
                raise UnsupportedTypeError('')

        named_path_info = PathStruct(path_name, child)
        self._paths.append(named_path_info)

    def remove_path(self, path_name: str) -> None:
        if path_name not in self.path_names:
            raise InvalidArgumentError(f'Failed to remove named path. {path_name} not exist.')

        idx = None
        for i, p in enumerate(self._paths):
            if p.path_name == path_name:
                idx = i

        if idx is not None:
            self._paths.pop(idx)

    def update_path(self, path_name: str, path: PathStructValue) -> None:
        if path.path_name is None:
            raise InvalidArgumentError('path_info.path_name is None')

        self.remove_path(path.path_name)
        self.add_path(path_name, path)

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
        return tuple([v.to_value() for v in self._paths])

    @property
    def path_names(self) -> Tuple[str, ...]:
        return tuple(sorted(v.path_name for v in self._paths if v.path_name is not None))

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
    def services(self) -> Tuple[ServiceStructValue, ...]:
        services = Util.flatten(_.services for _ in self.nodes)
        return tuple(sorted(services, key=lambda x: x.service_name))

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
            tuple(self._nodes), tuple(self._communications), node_filter, communication_filter)
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
    ) -> PathStructValue:

        def get_node(node_name: str) -> NodeStructValue:
            return self.get_node(node_name)

        def get_communication(
            publish_node_name: str,
            subscribe_node_name: str,
            topic_name: str
        ) -> CommunicationStructValue:
            return self.get_communication(publish_node_name, subscribe_node_name, topic_name)

        combine_path = CombinePath(get_node, get_communication)

        """
Architectureクラスが神クラスになるのを防ぐ
そのために、CombinePathは他のクラスにだして、モジュールも、テストコードもArchitectureとは分ける。

Q:Architecture.combine_pathは引き続きテストが必要？
やってもイチケースあれば十分

        """

        return combine_path.combine(path_left, path_right)

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

    def rename_callback(self, src: str, dst: str) -> None:
        cb_s: List[CallbackStruct] =\
            Util.flatten(cb_g.callbacks for cb_g in
                         Util.flatten([e.callback_groups for e in self._executors]))
        c: CallbackStruct = Util.find_similar_one(src, cb_s, lambda x: x.callback_name)
        c.callback_name = dst

    def rename_node(self, src: str, dst: str) -> None:
        for n in self._nodes:
            n.rename_node(src, dst)

        for e in self._executors:
            e.rename_node(src, dst)

        for c in self._communications:
            c.rename_node(src, dst)

    def rename_path(self, src: str, dst: str) -> None:
        p: PathStruct = Util.find_similar_one(src, self._paths, lambda x: x.path_name)
        p.path_name = dst

    def rename_executor(self, src: str, dst: str) -> None:
        e: ExecutorStruct = Util.find_similar_one(src, self._executors, lambda x: x.executor_name)
        e.executor_name = dst

    def rename_topic(self, src: str, dst: str) -> None:
        for n in self._nodes:
            n.rename_topic(src, dst)

        for e in self._executors:
            e.rename_topic(src, dst)

        for c in self._communications:
            c.rename_topic(src, dst)


class CombinePath():
    def __init__(
        self,
        get_node: Callable[[str], NodeStructValue],
        get_communication: Callable[[str, str, str], CommunicationStructValue]
    ):
        self._get_node = get_node
        self._get_communication = get_communication

    def __can_combine(self, name1: Optional[str], name2: Optional[str]) -> bool:
        return name1 is None or name2 is None or name1 == name2

    def __validate_topic_name(
        self, left_topic_name: Optional[str], right_topic_name: Optional[str]
    ) -> None:
        # TODO:  left_topic_name,  right_topic_nameをOptionalに変更。
        # NOTE:  たぶん、_check_none_or_sameは消して良くなる気がします。
        if self.__can_combine(left_topic_name, right_topic_name):
            return
        msg = 'Not matched topic name.'
        raise InvalidArgumentError(msg)

    def __validate_node_name(
        self, left_node_name: Optional[str], right_node_name: Optional[str]
    ) -> None:
        # if left_node_name != right_node_name:
        if self.__can_combine(left_node_name, right_node_name):
            return
        msg = 'Not matched node name.'
        raise InvalidArgumentError(msg)

    @singledispatchmethod
    def _validate_child(
        self,
        left_last_child: Union[NodePathStructValue, CommunicationStructValue],
        right_first_child: Union[NodePathStructValue, CommunicationStructValue]
    ) -> None:
        raise NotImplementedError('')

    @_validate_child.register
    def _validate_child_node_node(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: NodePathStructValue,
    ) -> None:
        if left_last_child.node_name != right_first_child.node_name:
            msg = 'Not matched left tail node and right head node name.'
            raise InvalidArgumentError(msg)
        # TODO: 何を確認しているか？関数名を変える。
        # ex:  if not can_combine():
        # can 「プログラミング　動詞」
        # isとか、booleanを返すことが期待される。
        self.__validate_topic_name(
            left_last_child.publish_topic_name,
            right_first_child.publish_topic_name)
        self.__validate_topic_name(
            left_last_child.subscribe_topic_name,
            right_first_child.subscribe_topic_name)
        # self.__validate_node_name(left_last_child.node_name, right_first_child.node_name)

    @_validate_child.register
    def _validate_child_node_comm(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: CommunicationStructValue,
    ) -> None:
        self.__validate_topic_name(
            left_last_child.publish_topic_name,
            right_first_child.topic_name)
        self.__validate_node_name(
            left_last_child.node_name,
            right_first_child.publish_node_name)

    @_validate_child.register
    def _validate_child_comm_node(
        self,
        left_last_child: CommunicationStructValue,
        right_first_child: NodePathStructValue,
    ) -> None:
        self.__validate_topic_name(
            right_first_child.subscribe_topic_name,
            left_last_child.topic_name)
        self.__validate_node_name(
            right_first_child.node_name,
            left_last_child.subscribe_node_name)

    @_validate_child.register
    def _validate_child_comm_comm(
        self,
        left_last_child: CommunicationStructValue,
        right_first_child: CommunicationStructValue,
    ) -> None:
        self.__validate_topic_name(
            left_last_child.topic_name,
            right_first_child.topic_name)
        self.__validate_node_name(
            left_last_child.subscribe_node_name,
            right_first_child.subscribe_node_name)
        self.__validate_node_name(
            left_last_child.publish_node_name,
            right_first_child.publish_node_name)

    # 必要であればこんな感じの関数を作って個別にテストすると良い。
    # @staticmethod # 入力にたいして出力が一意に決まる関数にする。selfは使わない。
    # def _can_combine() -> bool:
    #     # テストしておきたい処理達, ex: if分が多いとか。
    #     # caret_analyzeの中で、_validate関数はそんな感じ。
    #     #  1. _validateだけ、中身のチェックとしてprotectedだけども、直接テストする
    #     #  2. _validateが引数に関わらずTrue／False／Noneを返すモックとして、他の処理をテストする。
    #     raise NotImplemented('')

    def _validate(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> None:
        if len(left_path.child) == 0 or len(right_path.child) == 0:
            msg = 'Input paths have no child. '
            raise InvalidArgumentError(msg)

        left_last_child: Union[NodePathStructValue, CommunicationStructValue] = \
            left_path.child[-1]
        right_first_child: Union[NodePathStructValue, CommunicationStructValue] = \
            right_path.child[0]
        self._validate_child(left_last_child, right_first_child)

    @staticmethod
    def _find_one_node_path(
        node_path: NodePathStructValue,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str]
    ) -> bool:
        return node_path.subscribe_topic_name == subscribe_topic_name and \
               node_path.publish_topic_name == publish_topic_name

    @singledispatchmethod
    def _find_node_path_core(
        self,
        left_last_child: Union[NodePathStructValue, CommunicationStructValue],
        right_first_child: Union[NodePathStructValue, CommunicationStructValue],
        node_paths: NodePathStructValue
    ) -> NodePathStructValue:
        raise NotImplementedError('')

    @_find_node_path_core.register
    def _find_node_path_node_node(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: NodePathStructValue,
        node_paths: Tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        for node_path in node_paths:
            if (self._find_one_node_path(
                    node_path,
                    left_last_child.subscribe_topic_name,
                    right_first_child.publish_topic_name)):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    @_find_node_path_core.register
    def _find_node_path_node_comm(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: CommunicationStructValue,
        node_paths: Tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        for node_path in node_paths:
            if (self._find_one_node_path(
                    node_path,
                    left_last_child.subscribe_topic_name,
                    right_first_child.topic_name)):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    @_find_node_path_core.register
    def _find_node_path_comm_node(
        self,
        left_last_child: CommunicationStructValue,
        right_first_child: NodePathStructValue,
        node_paths: Tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        for node_path in node_paths:
            if (self._find_one_node_path(
                    node_path,
                    left_last_child.topic_name,
                    right_first_child.publish_topic_name)):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    def _find_node_path(
        self,
        left_last_child: Union[NodePathStructValue, CommunicationStructValue],
        right_first_child: Union[NodePathStructValue, CommunicationStructValue],
        node_paths: Tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        return self._find_node_path_core(left_last_child, right_first_child, node_paths)

    def _get_node_name(
        self,
        left_last_child: Union[NodePathStructValue, CommunicationStructValue],
        right_first_child: Union[NodePathStructValue, CommunicationStructValue]
    ) -> str:
        if (isinstance(left_last_child, NodePathStructValue)):
            return left_last_child.node_name
        if (isinstance(right_first_child, NodePathStructValue)):
            return right_first_child.node_name
        msg = "'combine_path.get_node_name' does not support communication. "
        raise UnsupportedTypeError(msg)

    def __create_path_core(
        self,
        *children: Tuple[Union[NodePathStructValue, CommunicationStructValue], ...]
    ) -> PathStructValue:
        new_child: Tuple[Union[NodePathStructValue, CommunicationStructValue], ...] = ()
        for child in children:
            new_child += tuple(child)
        return PathStructValue(None, (new_child))

    def _create_path(
        self,
        left_path: PathStructValue,
        middle_path: Union[NodePathStructValue, CommunicationStructValue],
        right_path: PathStructValue,
    ) -> PathStructValue:
        # インデックスアクセスは、全て、要素が足りないlen(left_path.child) == 0 ケースも考える。
        left_last_child: Union[NodePathStructValue, CommunicationStructValue] = \
            left_path.child[-1]
        right_first_child: Union[NodePathStructValue, CommunicationStructValue] = \
            right_path.child[0]

        if isinstance(left_last_child, NodePathStructValue) and \
                isinstance(right_first_child, NodePathStructValue):
            return self.__create_path_core(
                left_path.child[0:-1],
                (middle_path, ),
                right_path.child[1:])

        if isinstance(left_last_child, NodePathStructValue) and \
                isinstance(right_first_child, CommunicationStructValue):
            if (len(left_path.child) == 1):
                return self.__create_path_core((middle_path, ), right_path.child)
            else:
                return self.__create_path_core(
                    left_path.child[0:-1],
                    (middle_path, ),
                    right_path.child)

        if isinstance(left_last_child, CommunicationStructValue) and \
                isinstance(right_first_child, NodePathStructValue):
            if (len(right_path.child) == 1):
                return self.__create_path_core(left_path.child, (middle_path, ))
            else:
                return self.__create_path_core(
                    left_path.child,
                    (middle_path, ),
                    right_path.child[1:])

        if isinstance(left_last_child, CommunicationStructValue) and \
                isinstance(right_first_child, CommunicationStructValue):
            return self.__create_path_core(
                left_path.child[0:-1],
                (middle_path, ),
                right_path.child[1:])

        msg = 'Input type is wrong'
        raise InvalidArgumentError(msg)

    def combine(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> PathStructValue:
        self._validate(left_path, right_path)

        left_last_child: Union[NodePathStructValue, CommunicationStructValue] = \
            left_path.child[-1]
        right_first_child: Union[NodePathStructValue, CommunicationStructValue] = \
            right_path.child[0]

        # Left, Right = (Node, Node) or (Node, Comm) or (Comm, Node)
        if isinstance(left_last_child, NodePathStructValue) or \
                isinstance(right_first_child, NodePathStructValue):
            node_name: str = self._get_node_name(left_last_child, right_first_child)
            node_paths: Tuple[NodePathStructValue, ...] = self._get_node(node_name).paths
            target_node_path: NodePathStructValue = \
                self._find_node_path(left_last_child, right_first_child, node_paths)

            return self._create_path(left_path, target_node_path, right_path)

        # Left, Right = (Comm, Comm)
        if isinstance(left_last_child, CommunicationStructValue) and \
                isinstance(right_first_child, CommunicationStructValue):
            comm_path: CommunicationStructValue = \
                self._get_communication(
                    left_last_child.publish_node_name,
                    right_first_child.subscribe_node_name,
                    left_last_child.topic_name)

            if (comm_path is None):
                msg = 'No matched topic'
                raise InvalidArgumentError(msg)

            return self._create_path(left_path, comm_path, right_path)

        msg = 'Input type is wrong'
        raise InvalidArgumentError(msg)
