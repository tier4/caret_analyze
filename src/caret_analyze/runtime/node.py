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

from .callback import CallbackBase
from .callback_group import CallbackGroup
from .node_path import NodePath
from .publisher import Publisher
from .subscription import Subscription
from .timer import Timer
from .variable_passing import VariablePassing
from ..common import Summarizable, Summary, Util
from ..exceptions import InvalidArgumentError, ItemNotFoundError
from ..value_objects import NodeStructValue


class Node(Summarizable):
    """A class that represents a node."""

    def __init__(
        self,
        node: NodeStructValue,
        publishers: list[Publisher],
        subscription: list[Subscription],
        timers: list[Timer],
        node_paths: list[NodePath],
        callback_groups: list[CallbackGroup] | None,
        variable_passings: list[VariablePassing] | None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node : NodeStructValue
            static info
        publishers : list[Publisher]
            publishers in the node.
        subscription : list[Subscription]
            subscriptions in the node.
        timers : list[Timer]
            timers in the node.
        node_paths : list[NodePath]
            node paths in the node.
        callback_groups : list[CallbackGroup] | None
            callback groups in the node.
        variable_passings : list[VariablePassing] | None
            variable passings in the node.

        """
        self._val = node
        self._publishers = publishers
        self._subscriptions = subscription
        self._timers = timers
        self._paths = node_paths
        self._callback_groups = callback_groups
        self._variable_passings = variable_passings

    @property
    def callback_groups(self) -> list[CallbackGroup] | None:
        """
        Get callback groups.

        Returns
        -------
        list[CallbackGroup] | None
            callback groups that the node contains.

        """
        if self._callback_groups is None:
            return None
        return sorted(self._callback_groups, key=lambda x: x.callback_group_name)

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name.

        """
        return self._val.node_name

    @property
    def callbacks(self) -> list[CallbackBase] | None:
        """
        Get callbacks.

        Returns
        -------
        list[CallbackBase] | None
            callbacks that the node contains.

        """
        if self.callback_groups is None:
            return None
        cbs = Util.flatten([cbg.callbacks for cbg in self.callback_groups])
        return sorted(cbs, key=lambda x: x.callback_name)

    @property
    def callback_names(self) -> list[str] | None:
        """
        Get callback names.

        Returns
        -------
        list[str] | None
            callback names that the node contains.

        """
        if self.callbacks is None:
            return None
        return sorted(c.callback_name for c in self.callbacks)

    @property
    def variable_passings(self) -> list[VariablePassing] | None:
        """
        Get variable passings.

        Returns
        -------
        list[VariablePassing] | None
            Variable passings that the node contains.

        """
        return self._variable_passings

    @property
    def publishers(self) -> list[Publisher]:
        """
        Get publishers.

        Returns
        -------
        list[Publisher]
            publishers used by the node.

        """
        return sorted(self._publishers, key=lambda x: x.topic_name)

    @property
    def timers(self) -> list[Timer]:
        """
        Get timers.

        Returns
        -------
        list[Timer]
            timers that the node contains.

        """
        return sorted(self._timers, key=lambda x: x.period_ns)

    @property
    def publish_topic_names(self) -> list[str]:
        """
        Get topic names the node publishes.

        Returns
        -------
        list[str]
            topic names that the node publishes to.

        """
        return sorted(_.topic_name for _ in self._publishers)

    @property
    def paths(self) -> list[NodePath]:
        """
        Get node paths.

        Node paths are defined by subscription and publisher pair.

        Returns
        -------
        list[NodePath]
            node paths that the node contains.

        """
        return self._paths

    @property
    def subscriptions(self) -> list[Subscription]:
        """
        Get subscriptions the node subscribes.

        Returns
        -------
        list[Subscription]
            subscriptions that the node subscribes to.

        """
        return sorted(self._subscriptions, key=lambda x: x.topic_name)

    @property
    def subscribe_topic_names(self) -> list[str]:
        """
        Get subscribe topic names.

        Returns
        -------
        list[str]
            topic names to which the node subscribes.

        """
        return sorted(_.topic_name for _ in self._subscriptions)

    @property
    def callback_group_names(self) -> list[str] | None:
        """
        Get callback group names.

        Returns
        -------
        list[str] | None
            callback group names that the node contains.

        """
        if self.callback_groups is None:
            return None
        return sorted(_.callback_group_name for _ in self.callback_groups)

    @property
    def value(self) -> NodeStructValue:
        """
        Get StructValue object.

        Returns
        -------
        NodeStructValue
            node value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._val

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._val.summary

    def get_callback_group(self, callback_group_name: str) -> CallbackGroup:
        """
        Get callback group.

        Parameters
        ----------
        callback_group_name : str
            callback group name to get.

        Returns
        -------
        CallbackGroup
            callback group that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(callback_group_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        if self._callback_groups is None:
            raise ItemNotFoundError('Callback group is None.')

        return Util.find_one(
            lambda x: x.callback_group_name == callback_group_name, self._callback_groups)

    def get_path(
        self,
        subscribe_topic_name: str | None,
        subscription_construction_order: int | None,
        publish_topic_name: str | None,
        publisher_construction_order: int | None
    ) -> NodePath:
        """
        Get node path.

        Parameters
        ----------
        subscribe_topic_name : str | None
            topic name to which the node subscribes.
        subscription_construction_order : int
            A construction order of subscription.
        publish_topic_name : str | None
            topic name to which the node publishes.
        publisher_construction_order : int
            A construction order of publisher.

        Returns
        -------
        NodePath
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
        if not isinstance(subscribe_topic_name, str) and subscribe_topic_name is not None:
            raise InvalidArgumentError('Argument type is invalid.')

        if not isinstance(publish_topic_name, str) and publish_topic_name is not None:
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(path: NodePath):
            return path.publish_topic_name == publish_topic_name and \
                path.subscribe_topic_name == subscribe_topic_name and \
                path.publisher_construction_order == publisher_construction_order and \
                path.subscription_construction_order == subscription_construction_order

        return Util.find_one(is_target, self.paths)

    def get_callback(self, callback_name: str) -> CallbackBase:
        """
        Get callback.

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

        if self.callbacks is None:
            raise ItemNotFoundError('Callback is None.')

        return Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)

    def get_callbacks(self, *callback_names: str) -> list[CallbackBase]:
        """
        Get callbacks.

        Parameters
        ----------
        *callback_names: str
            callback names to get.

        Returns
        -------
        list[CallbackBase]
            callbacks that match the condition.

        """
        callbacks = []
        for callback_name in callback_names:
            callbacks.append(self.get_callback(callback_name))

        return callbacks

    def get_subscription(
        self,
        topic_name: str,
        construction_order: int | None = None
    ) -> Subscription:
        """
        Get subscription.

        Parameters
        ----------
        topic_name : str
            topic name to get.
        construction_order : int | None
            construction order to get.

        Returns
        -------
        Subscription
            Subscription instance that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target(subscription: Subscription):
            match = subscription.topic_name == topic_name
            if construction_order is not None:
                match &= subscription.construction_order == construction_order
            return match

        return Util.find_one(is_target, self._subscriptions)

    def get_publisher(
            self,
            topic_name: str,
            construction_order: int | None = None
    ) -> Publisher:
        """
        Get publisher.

        Parameters
        ----------
        topic_name : str
            publisher topic name to get.
        construction_order : int | None
            construction order to get.

        Returns
        -------
        Publisher
            A publisher that matches the condition.

        Raises
        ------
        InvalidArgumentError
            Occurs when the given argument type is invalid.
        ItemNotFoundError
            Occurs when no items were found.
        MultipleItemFoundError
            Occurs when several items were found.

        """
        if not isinstance(topic_name, str):
            raise InvalidArgumentError('Argument type is invalid.')

        def is_target_publisher(publishers: Publisher):
            match = publishers.topic_name == topic_name
            if construction_order is not None:
                match &= publishers.construction_order == construction_order
            return match

        return Util.find_one(is_target_publisher, self._publishers)
