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

from typing import Callable, Optional, Tuple, Union

from multimethod import multimethod as singledispatchmethod

from ..exceptions import InvalidArgumentError, UnsupportedTypeError
from ..value_objects import (CommunicationStructValue, NodePathStructValue,
                             NodeStructValue, PathStructValue)



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
        if self.__can_combine(left_topic_name, right_topic_name):
            return
        msg = 'Not matched topic name.'
        raise InvalidArgumentError(msg)

    def __validate_node_name(
        self, left_node_name: Optional[str], right_node_name: Optional[str]
    ) -> None:
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
        assert left_last_child.node_name is not None
        assert right_first_child.node_name is not None

        self.__validate_topic_name(
            left_last_child.publish_topic_name,
            right_first_child.publish_topic_name)
        self.__validate_topic_name(
            left_last_child.subscribe_topic_name,
            right_first_child.subscribe_topic_name)
        self.__validate_node_name(left_last_child.node_name, right_first_child.node_name)

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
        comm_path: CommunicationStructValue = \
            self._get_communication(
                left_last_child.publish_node_name,
                right_first_child.subscribe_node_name,
                left_last_child.topic_name)

        if comm_path is None:
            msg = 'No matched topic'
            raise InvalidArgumentError(msg)

        return self._create_path(left_path, comm_path, right_path)
