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

from collections.abc import Callable

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
        """
        Construct CombinePath.

        Parameters
        ----------
        get_node : Callable[[str], NodeStructValue]
            get_node function
        get_communication : Callable[[str, str, str], CommunicationStructValue]
            get_communication function

        """
        self._get_node = get_node
        self._get_communication = get_communication

    @staticmethod
    def __can_combine(name1: str | None, name2: str | None) -> bool:
        """
        Validate to be able to combine.

        Parameters
        ----------
        name1 : str | None
            node or topic name
        name2 : str | None
            node or topic name

        Returns
        -------
        bool
            True if combine is available, false otherwise.

        """
        if name1 is None and name2 is None:
            return False
        return name1 is None or name2 is None or name1 == name2

    def __validate_topic_name(
        self, left_topic_name: str | None, right_topic_name: str | None
    ) -> None:
        """
        Validate topic name.

        Parameters
        ----------
        left_topic_name : str | None
            left topic name
        right_topic_name : str | None
            right topic name

        Raises
        ------
        InvalidArgumentError
            Occurs when combine is not available.

        """
        if self.__can_combine(left_topic_name, right_topic_name):
            return
        msg = 'Not matched topic name.'
        raise InvalidArgumentError(msg)

    def __validate_node_name(
        self, left_node_name: str | None, right_node_name: str | None
    ) -> None:
        """
        Validate node name.

        Parameters
        ----------
        left_node_name : str | None
            left node name
        right_node_name : str | None
            right node name

        Raises
        ------
        InvalidArgumentError
            Occurs when combine is not available.

        """
        if self.__can_combine(left_node_name, right_node_name):
            return
        msg = 'Not matched node name.'
        raise InvalidArgumentError(msg)

    @singledispatchmethod
    def _validate_child(
        self,
        left_last_child,    # 'singledispatchmethod' doesn't
        right_first_child,  # allow to attach annotation to parameters.
    ) -> None:
        """
        Validate to be able to combine.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : NodePathStructValue
            right first child

        """
        msg = 'Unsupported type.'
        raise UnsupportedTypeError(msg)

    @_validate_child.register
    def _validate_child_node_node(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: NodePathStructValue,
    ) -> None:
        """
        Validate to be able to combine Node-Node.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : NodePathStructValue
            right first child

        """
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
        """
        Validate to be able to combine Node-Comm.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : CommunicationStructValue
            right first child

        """
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
        """
        Validate to be able to combine Comm-Node.

        Parameters
        ----------
        left_last_child : CommunicationStructValue
            left last child
        right_first_child : NodePathStructValue
            right first child

        """
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
        """
        Validate to be able to combine Comm-Comm.

        Parameters
        ----------
        left_last_child : CommunicationStructValue
            left last child
        right_first_child : CommunicationStructValue
            right last child

        """
        self.__validate_topic_name(
            left_last_child.topic_name,
            right_first_child.topic_name)
        self.__validate_node_name(
            left_last_child.subscribe_node_name,
            right_first_child.subscribe_node_name)
        self.__validate_node_name(
            left_last_child.publish_node_name,
            right_first_child.publish_node_name)

    def _validate(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> None:
        """
        Validate arguments.

        Parameters
        ----------
        left_path : PathStructValue
            left path
        right_path : PathStructValue
            right path

        Raises
        ------
        InvalidArgumentError
            Occurs when combine is not available.

        """
        if len(left_path.child) == 0 or len(right_path.child) == 0:
            msg = 'No child in input path. '
            raise InvalidArgumentError(msg)

        left_last_child: NodePathStructValue | CommunicationStructValue = \
            left_path.child[-1]
        right_first_child: NodePathStructValue | CommunicationStructValue = \
            right_path.child[0]
        self._validate_child(left_last_child, right_first_child)

    @staticmethod
    def __is_valid(
        node_path: NodePathStructValue,
        subscribe_topic_name: str | None,
        subscription_construction_order: int | None,
        publish_topic_name: str | None,
        publisher_construction_order: int | None
    ) -> bool:
        """
        Check if the given NodePathStructValue matches.

        Parameters
        ----------
        node_path : NodePathStructValue
            candidate node path
        subscribe_topic_name : str | None
            expected subscribe topic name
        subscription_construction_order : int | None
            expected subscribe construction order
        publish_topic_name : str | None
            expected publish topic name
        publisher_construction_order : int | None
            expected publish construction order

        Returns
        -------
        bool
            True if match, False otherwise.

        """
        return node_path.subscribe_topic_name == subscribe_topic_name and \
            node_path.subscription_construction_order == subscription_construction_order and \
            node_path.publish_topic_name == publish_topic_name and \
            node_path.publisher_construction_order == publisher_construction_order

    @singledispatchmethod
    def _find_node_path_core(
        self,
        left_last_child: NodePathStructValue | CommunicationStructValue,
        right_first_child: NodePathStructValue | CommunicationStructValue,
        node_paths: NodePathStructValue
    ) -> NodePathStructValue:
        """
        Find node path.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : NodePathStructValue
            right last child
        node_paths : tuple[NodePathStructValue, ...]
            candidate node paths

        Returns
        -------
        NodePathStructValue
            node path

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError('')

    @_find_node_path_core.register
    def _find_node_path_node_node(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: NodePathStructValue,
        node_paths: tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        """
        Find node path in case of Node-Node.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : NodePathStructValue
            right last child
        node_paths : tuple[NodePathStructValue, ...]
            candidate node paths

        Returns
        -------
        NodePathStructValue
            node path

        Raises
        ------
        InvalidArgumentError
            No node path to combine.

        """
        for node_path in node_paths:
            if self.__is_valid(
                    node_path,
                    left_last_child.subscribe_topic_name,
                    left_last_child.subscription_construction_order,
                    right_first_child.publish_topic_name,
                    right_first_child.publisher_construction_order):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    @_find_node_path_core.register
    def _find_node_path_node_comm(
        self,
        left_last_child: NodePathStructValue,
        right_first_child: CommunicationStructValue,
        node_paths: tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        """
        Find node path in case of Node-Comm.

        Parameters
        ----------
        left_last_child : NodePathStructValue
            left last child
        right_first_child : CommunicationStructValue
            right last child
        node_paths : tuple[NodePathStructValue, ...]
            candidate node paths

        Returns
        -------
        NodePathStructValue
            node path

        Raises
        ------
        InvalidArgumentError
            No node path to combine.

        """
        for node_path in node_paths:
            if self.__is_valid(
                    node_path,
                    left_last_child.subscribe_topic_name,
                    left_last_child.subscription_construction_order,
                    right_first_child.topic_name,
                    right_first_child.publisher_construction_order):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    @_find_node_path_core.register
    def _find_node_path_comm_node(
        self,
        left_last_child: CommunicationStructValue,
        right_first_child: NodePathStructValue,
        node_paths: tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        """
        Find node path in case of Comm-Node.

        Parameters
        ----------
        left_last_child : CommunicationStructValue
            left last child
        right_first_child : NodePathStructValue
            right last child
        node_paths : tuple[NodePathStructValue, ...]
            candidate node paths

        Returns
        -------
        NodePathStructValue
            node path

        Raises
        ------
        InvalidArgumentError
            No node path to combine.

        """
        for node_path in node_paths:
            if self.__is_valid(
                    node_path,
                    left_last_child.topic_name,
                    left_last_child.subscription_construction_order,
                    right_first_child.publish_topic_name,
                    right_first_child.publisher_construction_order):
                return node_path
        msg = 'No node path to combine.'
        raise InvalidArgumentError(msg)

    def _find_node_path(
        self,
        left_last_child: NodePathStructValue | CommunicationStructValue,
        right_first_child: NodePathStructValue | CommunicationStructValue,
        node_paths: tuple[NodePathStructValue, ...],
    ) -> NodePathStructValue:
        """
        Find node path.

        Parameters
        ----------
        left_last_child : NodePathStructValue | CommunicationStructValue
            left last child
        right_first_child : NodePathStructValue | CommunicationStructValue
            right last child
        node_paths : tuple[NodePathStructValue, ...]
            candidate node paths

        Returns
        -------
        NodePathStructValue
            node path

        """
        return self._find_node_path_core(left_last_child, right_first_child, node_paths)

    @staticmethod
    def _get_node_name(
        left_last_child: NodePathStructValue | CommunicationStructValue,
        right_first_child: NodePathStructValue | CommunicationStructValue
    ) -> str:
        """
        Get node name.

        Parameters
        ----------
        left_last_child : NodePathStructValue | CommunicationStructValue
            left last child
        right_first_child : NodePathStructValue | CommunicationStructValue
            right first child

        Returns
        -------
        str
            Node name

        Raises
        ------
        UnsupportedTypeError
            Unsupported in case of Comm-Comm

        """
        if isinstance(left_last_child, NodePathStructValue):
            return left_last_child.node_name
        if isinstance(right_first_child, NodePathStructValue):
            return right_first_child.node_name
        msg = "'combine_path.get_node_name' does not support communication. "
        raise UnsupportedTypeError(msg)

    @staticmethod
    def __get_name(child: NodePathStructValue | CommunicationStructValue) -> str:
        """
        Get node or topic name.

        Parameters
        ----------
        child : NodePathStructValue | CommunicationStructValue
            child

        Returns
        -------
        str
            node or topic name

        """
        if isinstance(child, NodePathStructValue):
            return child.node_name
        return child.topic_name

    def __is_same(
        self,
        left_child: NodePathStructValue | CommunicationStructValue,
        right_child: NodePathStructValue | CommunicationStructValue,
    ) -> bool:
        """
        Validate left child and right child are same.

        Parameters
        ----------
        left_child : NodePathStructValue | CommunicationStructValue
            left child
        right_child : NodePathStructValue | CommunicationStructValue
            right child

        Returns
        -------
        bool
            same or difference

        """
        return self.__get_name(left_child) == self.__get_name(right_child) and \
            type(left_child) == type(right_child)

    def _create_path(
        self,
        left_path: tuple[NodePathStructValue | CommunicationStructValue, ...],
        middle_child: NodePathStructValue | CommunicationStructValue,
        right_path: tuple[NodePathStructValue | CommunicationStructValue, ...],
    ) -> PathStructValue:
        """
        Create path.

        Parameters
        ----------
        left_path : tuple[NodePathStructValue | CommunicationStructValue, ...]
            left path
        middle_child : NodePathStructValue | CommunicationStructValue
            middle child
        right_path : tuple[NodePathStructValue | CommunicationStructValue, ...]
            right path

        Returns
        -------
        PathStructValue
            Combined path

        """
        new_child: tuple[NodePathStructValue | CommunicationStructValue, ...] = ()
        candidate_path: tuple[NodePathStructValue | CommunicationStructValue, ...] = \
            left_path + right_path

        assert len(candidate_path) >= 2
        for child, child_ in zip(candidate_path[:-1], candidate_path[1:]):
            # Remove consecutive child
            if self.__is_same(child, child_):
                continue
            if self.__is_same(child, middle_child):
                new_child += (middle_child,)
            else:
                new_child += (child,)
        new_child += (right_path[-1],)
        return PathStructValue(None, new_child)

    @singledispatchmethod
    def _get_middle_child(
        self,
        left_last_child,    # 'singledispatchmethod' doesn't
        right_first_child,  # allow to attach annotation to parameters.
    ):
        # NOTE
        # This function is called when parameters are
        # Node-Node, Node-Comm, or Comm-Node.
        # In case Comm-Comm, '_get_middle_child_comm_comm' function is called.
        """
        Get a middle node path.

        Parameters
        ----------
        left_last_child : NodePathStructValue | CommunicationStructValue
            left last child
        right_first_child : NodePathStructValue | CommunicationStructValue
            right first child

        Returns
        -------
        NodePathStructValue
            Middle node path

        """
        node_name: str = self._get_node_name(left_last_child, right_first_child)
        node_paths: tuple[NodePathStructValue, ...] = self._get_node(node_name).paths
        target_node_path: NodePathStructValue = \
            self._find_node_path(left_last_child, right_first_child, node_paths)
        return target_node_path

    @_get_middle_child.register
    def _get_middle_child_comm_comm(
        self,
        left_last_child: CommunicationStructValue,
        right_first_child: CommunicationStructValue,
    ) -> CommunicationStructValue:
        """
        Get a middle comm.

        Parameters
        ----------
        left_last_child : CommunicationStructValue
            left last child
        right_first_child : CommunicationStructValue
            right first child

        Returns
        -------
        CommunicationStructValue
            Middle comm

        Raises
        ------
        InvalidArgumentError
            No matched topic (communication).

        """
        comm_path: CommunicationStructValue = \
            self._get_communication(
                left_last_child.publish_node_name,
                right_first_child.subscribe_node_name,
                left_last_child.topic_name)

        if comm_path is None:
            msg = 'No matched topic.'
            raise InvalidArgumentError(msg)

        return comm_path

    def _combine_continuous_paths(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> PathStructValue:
        """
        Combine continuous paths.

        Left last child and right first child are
        side by side or same.

        Parameters
        ----------
        left_path : PathStructValue
            left path
        right_path : PathStructValue
            right path

        Returns
        -------
        PathStructValue
            combined path

        """
        left_last_child: NodePathStructValue | CommunicationStructValue = \
            left_path.child[-1]
        right_first_child: NodePathStructValue | CommunicationStructValue = \
            right_path.child[0]
        middle_child: NodePathStructValue | CommunicationStructValue = \
            self._get_middle_child(left_last_child, right_first_child)

        return self._create_path(left_path.child, middle_child, right_path.child)

    def _combine_discrete_paths(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> PathStructValue:
        # TODO(miura): [comm_1] + [comm_2] = [comm_1, node_x, comm_2]
        # TODO(miura): [node_1] + [node_2] = [node_1, comm_x, node_2]
        """
        Combine discrete paths.

        Parameters
        ----------
        left_path: PathStructValue
            left path
        right_path: PathStructValue
            right path

        Returns
        -------
        PathStructValue
            combined path

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        msg = 'This is a not implemented COMBINE case. Please contact maintainer if necessary.'
        raise NotImplementedError(msg)

    def combine(
        self,
        left_path: PathStructValue,
        right_path: PathStructValue
    ) -> PathStructValue:
        """
        Combine paths.

        Parameters
        ----------
        left_path: PathStructValue
            left path
        right_path: PathStructValue
            right path

        Returns
        -------
        PathStructValue
            combined path

        """
        self._validate(left_path, right_path)

        left_last_child: NodePathStructValue | CommunicationStructValue = \
            left_path.child[-1]
        right_first_child: NodePathStructValue | CommunicationStructValue = \
            right_path.child[0]

        # Left, Right = (Node, Node)
        if isinstance(left_last_child, NodePathStructValue) and \
                isinstance(right_first_child, NodePathStructValue):
            if left_last_child.node_name == right_first_child.node_name:
                return self._combine_continuous_paths(left_path, right_path)
            return self._combine_discrete_paths(left_path, right_path)

        # Left, Right = (Node, Comm) or (Comm, Node)
        if isinstance(left_last_child, NodePathStructValue) or \
                isinstance(right_first_child, NodePathStructValue):
            return self._combine_continuous_paths(left_path, right_path)

        # Left, Right = (Comm, Comm)
        if left_last_child.topic_name == right_first_child.topic_name:
            return self._combine_continuous_paths(left_path, right_path)
        return self._combine_discrete_paths(left_path, right_path)
