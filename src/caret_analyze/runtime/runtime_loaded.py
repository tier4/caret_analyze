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

from typing import List, Optional, Tuple, Union

from .callback import CallbackBase, SubscriptionCallback, TimerCallback
from .callback_group import CallbackGroup
from .communication import Communication
from .executor import Executor
from .node import Node
from .node_path import NodePath
from .path import Path
from .publisher import Publisher
from .subscription import Subscription
from .timer import Timer, TimerStructValue
from .variable_passing import VariablePassing
from ..architecture import Architecture
from ..common import Util
from ..exceptions import (Error, ItemNotFoundError, MultipleItemFoundError,
                          UnsupportedTypeError)
from ..infra.interface import RecordsProvider, RuntimeDataProvider
from ..value_objects import (CallbackGroupStructValue, CallbackStructValue,
                             CommunicationStructValue, ExecutorStructValue,
                             NodePathStructValue, NodeStructValue,
                             PathStructValue, PublisherStructValue,
                             ServiceCallbackStructValue,
                             SubscriptionCallbackStructValue,
                             SubscriptionStructValue, TimerCallbackStructValue,
                             VariablePassingStructValue)


logger = getLogger(__name__)


class RuntimeLoaded():
    def __init__(
        self,
        architecture: Architecture,
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        nodes_loaded = NodesLoaded(architecture.nodes, provider)
        self._nodes = nodes_loaded.data

        execs_loaded = ExecutorsLoaded(
            architecture.executors, nodes_loaded)
        self._executors = execs_loaded.data

        comms_loaded = CommunicationsLoaded(
            architecture.communications, provider, nodes_loaded)
        self._comms = comms_loaded.data

        paths_loaded = PathsLoaded(
            architecture.paths, nodes_loaded, comms_loaded)
        self._paths = paths_loaded.data

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    @property
    def executors(self) -> List[Executor]:
        return self._executors

    @property
    def communications(self) -> List[Communication]:
        return self._comms

    @property
    def paths(self) -> List[Path]:
        return self._paths


class ExecutorsLoaded:
    def __init__(
        self,
        executors_values: Tuple[ExecutorStructValue, ...],
        nodes_loaded: NodesLoaded
    ) -> None:
        self._data = []
        for exec_val in executors_values:
            try:
                self._data.append(self._to_runtime(exec_val, nodes_loaded))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        executor_value: ExecutorStructValue,
        nodes_loaded: NodesLoaded
    ) -> Executor:
        callback_groups = [
            nodes_loaded.find_callback_group(group_name)
            for group_name
            in executor_value.callback_group_names
        ]

        return Executor(
            executor_value,
            callback_groups,
        )

    @property
    def data(self) -> List[Executor]:
        return self._data


class NodesLoaded:
    def __init__(
        self,
        node_values: Tuple[NodeStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        self._nodes: List[Node] = []
        for node_value in node_values:
            try:
                self._nodes.append(self._to_runtime(node_value, provider))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        node_value: NodeStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Node:
        publishers_loaded = PublishersLoaded(
            node_value.publishers, provider)
        publishers = publishers_loaded.data

        subscriptions_loaded = SubscriptionsLoaded(
            node_value.subscriptions, provider)
        subscriptions = subscriptions_loaded.data

        timers_loaded = TimersLoaded(
            node_value.timers, provider
        )
        timers = timers_loaded.data

        callback_groups: List[CallbackGroup] = []
        if node_value.callback_groups is not None:
            callback_groups = CallbackGroupsLoaded(
                node_value.callback_groups,
                provider,
                publishers_loaded,
                subscriptions_loaded,
                timers_loaded
            ).data

        callbacks = Util.flatten([_.callbacks for _ in callback_groups])
        node_paths: List[NodePath]
        node_paths = NodePathsLoaded(
            node_value.paths, provider, publishers_loaded, subscriptions_loaded, callbacks
        ).data

        variable_passings: List[VariablePassing] = []
        if node_value.variable_passings is not None:
            variable_passings = VariablePassingsLoaded(
                node_value.variable_passings, provider).data

        return Node(
            node_value,
            publishers,
            subscriptions,
            timers,
            node_paths,
            callback_groups,
            variable_passings
        )

    @property
    def callback_groups(self) -> List[CallbackGroup]:
        callback_groups: List[CallbackGroup] = []
        for node in self._nodes:
            if node.callback_groups is not None:
                callback_groups += node.callback_groups
        return callback_groups

    @property
    def data(self) -> List[Node]:
        return self._nodes

    @property
    def callbacks(self) -> Optional[List[CallbackBase]]:
        cbs = []
        for node in self._nodes:
            if node.callbacks is not None:
                cbs += node.callbacks
        return cbs

    def find_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackGroup:
        try:
            callback_groups = self.callback_groups
            if callback_groups is None:
                raise ItemNotFoundError('')

            return Util.find_one(
                lambda x: x.callback_group_name == callback_group_name,
                callback_groups
            )
        except ItemNotFoundError:
            raise ItemNotFoundError(
                f'Failed to find node. callback_group_name = {callback_group_name}')

    def find_callback(
        self,
        callback_name: str
    ) -> CallbackBase:
        try:
            cbs = self.callbacks
            if cbs is None:
                raise ItemNotFoundError('')

            return Util.find_one(
                lambda x: x.callback_name == callback_name,
                cbs
            )
        except ItemNotFoundError:
            raise ItemNotFoundError(
                f'Failed to find node. callback_name = {callback_name}')

    def find_node(
        self,
        node_name: str
    ) -> Node:
        try:
            return Util.find_one(
                lambda x: x.node_name == node_name,
                self._nodes
            )
        except ItemNotFoundError:
            raise ItemNotFoundError(
                f'Failed to find node. node_name = {node_name}')

    def find_node_path(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        publisher_construction_order: Optional[int],
        subscription_construction_order: Optional[int],
    ) -> NodePath:
        def is_target(node_path: NodePath):
            return node_path.publish_topic_name == publish_topic_name and \
                node_path.subscribe_topic_name == subscribe_topic_name and \
                node_path.node_name == node_name and \
                node_path.publisher_construction_order == publisher_construction_order and \
                node_path.subscription_construction_order == subscription_construction_order

        try:
            node_paths = Util.flatten([n.paths for n in self._nodes])
            return Util.find_one(is_target, node_paths)
        except ItemNotFoundError:
            msg = 'Failed to find node path. '
            msg += f'node_name: {node_name}. '
            msg += f'publish_topic_name: {publish_topic_name}. '
            msg += f'subscribe_topic_name: {subscribe_topic_name}. '
            msg += f'publisher_construction_order: {publisher_construction_order}. '
            msg += f'subscription_construction_order: {subscription_construction_order}. '
            raise ItemNotFoundError(msg)


class PublishersLoaded:
    def __init__(
        self,
        publishers_info: Tuple[PublisherStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._pubs = []
        for pub_info in publishers_info:
            try:
                self._pubs.append(self._to_runtime(pub_info, provider))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        publisher_info: PublisherStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Publisher:
        return Publisher(publisher_info, provider)

    @property
    def data(self) -> List[Publisher]:
        return self._pubs

    def get_publishers(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
    ) -> List[Publisher]:
        is_target = PublishersLoaded.IsTarget(node_name, callback_name, topic_name)
        return Util.filter_items(is_target, self._pubs)

    def get_publisher(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
    ) -> Publisher:
        try:
            is_target = PublishersLoaded.IsTarget(node_name, callback_name, topic_name)
            return Util.find_one(is_target, self._pubs)
        except ItemNotFoundError:
            msg = 'Failed to find publisher. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    def get_publisher_from_construction_order(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
        construction_order: Optional[int] = None
    ) -> Publisher:
        try:
            is_target = PublishersLoaded.IsTargetWithConstructionOrder(
                node_name, callback_name, topic_name, construction_order)
            return Util.find_one(is_target, self._pubs)
        except ItemNotFoundError:
            msg = 'Failed to find publisher. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    class IsTarget:
        def __init__(
            self,
            node_name: Optional[str],
            callback_name: Optional[str],
            topic_name: Optional[str]
        ) -> None:
            self._node_name = node_name
            self._callback_name = callback_name
            self._topic_name = topic_name

        def __call__(self, pub: Publisher) -> bool:
            topic_match = True
            if self._topic_name is not None:
                topic_match = self._topic_name == pub.topic_name

            node_match = True
            if self._node_name is not None:
                node_match = self._node_name == pub.node_name

            callback_match = True
            if self._callback_name is not None:
                if pub.callback_names is None:
                    callback_match = False
                else:
                    callback_match = self._callback_name in pub.callback_names
            return topic_match and node_match and callback_match

    class IsTargetWithConstructionOrder:
        def __init__(
            self,
            node_name: Optional[str],
            callback_name: Optional[str],
            topic_name: Optional[str],
            construction_order: Optional[int] = None
        ) -> None:
            self._node_name = node_name
            self._callback_name = callback_name
            self._topic_name = topic_name
            self._construction_order = construction_order

        def __call__(self, pub: Publisher) -> bool:
            topic_match = True
            if self._topic_name is not None:
                topic_match = self._topic_name == pub.topic_name
            else:
                topic_match = pub.topic_name is None

            node_match = True
            if self._node_name is not None:
                node_match = self._node_name == pub.node_name
            else:
                node_match = pub.node_name is None

            callback_match = True
            if self._callback_name is not None:
                if pub.callback_names is None:
                    callback_match = False
                else:
                    callback_match = self._callback_name in pub.callback_names

            construction_order_match = True
            if self._construction_order is not None:
                construction_order_match = self._construction_order == pub.construction_order
            else:
                construction_order_match = pub.construction_order is None
            return topic_match and node_match and callback_match and construction_order_match


class SubscriptionsLoaded:
    def __init__(
        self,
        subscriptions_info: Tuple[SubscriptionStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._subs = []
        for sub_info in subscriptions_info:
            try:
                self._subs.append(self._to_runtime(sub_info, provider))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        subscription_info: SubscriptionStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Subscription:
        return Subscription(subscription_info, provider)

    @property
    def data(self) -> List[Subscription]:
        return self._subs

    def get_subscriptions(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
    ) -> List[Subscription]:
        is_target = SubscriptionsLoaded.IsTarget(node_name, callback_name, topic_name)
        return Util.filter_items(is_target, self._subs)

    def get_subscription(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
    ) -> Subscription:
        try:
            is_target = SubscriptionsLoaded.IsTarget(node_name, callback_name, topic_name)
            return Util.find_one(is_target, self._subs)
        except ItemNotFoundError:
            msg = 'Failed to find subscription. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple item found. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    def get_subscription_from_construction_order(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        topic_name: Optional[str],
        construction_order: Optional[int] = None
    ) -> Subscription:
        try:
            is_target = SubscriptionsLoaded.IsTargetWithConstructionOrder(node_name,
                                                                          callback_name,
                                                                          topic_name,
                                                                          construction_order)
            return Util.find_one(is_target, self._subs)
        except ItemNotFoundError:
            msg = 'Failed to find subscription. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple item found. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    class IsTarget:
        def __init__(
            self,
            node_name: Optional[str],
            callback_name: Optional[str],
            topic_name: Optional[str]
        ) -> None:
            self._node_name = node_name
            self._callback_name = callback_name
            self._topic_name = topic_name

        def __call__(self, sub: Subscription) -> bool:
            topic_match = True
            if self._topic_name is not None:
                topic_match = self._topic_name == sub.topic_name

            node_match = True
            if self._node_name is not None:
                node_match = self._node_name == sub.node_name

            callback_match = True
            if self._callback_name is not None:
                callback_match = self._callback_name == sub.callback_name
            return topic_match and node_match and callback_match

    class IsTargetWithConstructionOrder:
        def __init__(
            self,
            node_name: Optional[str],
            callback_name: Optional[str],
            topic_name: Optional[str],
            construction_order: Optional[int] = None
        ) -> None:
            self._node_name = node_name
            self._callback_name = callback_name
            self._topic_name = topic_name
            self._construction_order = construction_order

        def __call__(self, sub: Subscription) -> bool:
            topic_match = True
            if self._topic_name is not None:
                topic_match = self._topic_name == sub.topic_name
            else:
                topic_match = sub.topic_name is None

            node_match = True
            if self._node_name is not None:
                node_match = self._node_name == sub.node_name
            else:
                node_match = sub.node_name is None

            callback_match = True
            if self._callback_name is not None:  # callback is only None
                callback_match = self._callback_name == sub.callback_name

            construction_order_match = True
            if self._construction_order is not None:
                construction_order_match = self._construction_order == sub.construction_order
            else:
                construction_order_match = sub.construction_order is None
            return topic_match and node_match and callback_match and construction_order_match


class TimersLoaded:
    def __init__(
        self,
        timers_info: Tuple[TimerStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._timers = []
        for timer_info in timers_info:
            try:
                self._timers.append(self._to_runtime(timer_info, provider))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        timer_info: TimerStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Timer:
        return Timer(timer_info, provider)

    @property
    def data(self) -> List[Timer]:
        return self._timers

    def get_timers(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        period_ns: Optional[int],
    ) -> List[Timer]:
        is_target = TimersLoaded.IsTarget(node_name, callback_name, period_ns)
        return Util.filter_items(is_target, self._timers)

    def get_timer(
        self,
        node_name: Optional[str],
        callback_name: Optional[str],
        period_ns: Optional[int],
    ) -> Timer:
        try:
            is_target = TimersLoaded.IsTarget(node_name, callback_name, period_ns)
            return Util.find_one(is_target, self._timers)
        except ItemNotFoundError:
            msg = 'Failed to find timer. '
            msg += f'node_name: {node_name}, '
            msg += f'callback_name: {callback_name}, '
            msg += f' period_ns: {period_ns}, '
            raise ItemNotFoundError(msg)

    class IsTarget:
        def __init__(
            self,
            node_name: Optional[str],
            callback_name: Optional[str],
            period_ns: Optional[int],
        ) -> None:
            self._node_name = node_name
            self._callback_name = callback_name
            self._period_ns = period_ns

        def __call__(self, timer: Timer) -> bool:
            period_match = True
            if self._period_ns is not None:
                period_match = self._period_ns == timer.period_ns

            node_match = True
            if self._node_name is not None:
                node_match = self._node_name == timer.node_name

            callback_match = True
            if self._callback_name is not None:
                callback_match = self._callback_name == timer.callback_name
            return period_match and node_match and callback_match


class NodePathsLoaded:
    def __init__(
        self,
        node_path_values: Tuple[NodePathStructValue, ...],
        provider: RecordsProvider,
        publisher_loaded: PublishersLoaded,
        subscription_loaded: SubscriptionsLoaded,
        callbacks: List[CallbackBase],
    ) -> None:
        self._data = []
        for node_path_value in node_path_values:
            try:
                self._data.append(self._to_runtime(
                    node_path_value, provider, publisher_loaded, subscription_loaded, callbacks))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        node_path_value: NodePathStructValue,
        provider: RecordsProvider,
        publisher_loaded: PublishersLoaded,
        subscription_loaded: SubscriptionsLoaded,
        callbacks: List[CallbackBase]
    ) -> NodePath:
        publisher: Optional[Publisher] = None
        subscription: Optional[Subscription] = None

        try:
            publisher = publisher_loaded.get_publisher_from_construction_order(
                node_path_value.node_name,
                None,
                node_path_value.publish_topic_name,
                node_path_value.publisher_construction_order
            )
        except ItemNotFoundError:
            pass
        except MultipleItemFoundError:
            pass

        try:
            subscription = subscription_loaded.get_subscription_from_construction_order(
                node_path_value.node_name,
                None,
                node_path_value.subscribe_topic_name,
                node_path_value.subscription_construction_order
            )
        except ItemNotFoundError:
            pass
        except MultipleItemFoundError:
            pass

        return NodePath(node_path_value, provider, subscription, publisher, callbacks)

    @property
    def data(self) -> List[NodePath]:
        return self._data


class VariablePassingsLoaded:
    def __init__(
        self,
        variable_passings_info: Tuple[VariablePassingStructValue, ...],
        provider: RecordsProvider
    ) -> None:
        self._var_passes = []
        for var_pass in variable_passings_info:
            try:
                self._var_passes.append(self._to_runtime(var_pass, provider))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        variable_passing_info: VariablePassingStructValue,
        provider: RecordsProvider,
    ) -> VariablePassing:
        return VariablePassing(
            variable_passing_info,
            provider
        )

    @property
    def data(self) -> List[VariablePassing]:
        return self._var_passes


class PathsLoaded:
    def __init__(
        self,
        paths_info: Tuple[PathStructValue, ...],
        nodes_loaded: NodesLoaded,
        comms_loaded: CommunicationsLoaded,
    ) -> None:
        self._data = []
        for path_info in paths_info:
            try:
                self._data.append(self._to_runtime(path_info, nodes_loaded, comms_loaded))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        path_info: PathStructValue,
        nodes_loaded: NodesLoaded,
        comms_loaded: CommunicationsLoaded,
    ) -> Path:
        child: List[Union[NodePath, Communication]] = []
        callbacks: List[CallbackBase] = []
        for elem_info in path_info.child:
            child.append(
                PathsLoaded._get_loaded(elem_info, nodes_loaded, comms_loaded)
            )
            if isinstance(elem_info, NodePathStructValue):
                if elem_info.callbacks is None:
                    continue
                for cb_val in elem_info.callbacks:
                    callbacks.append(nodes_loaded.find_callback(cb_val.callback_name))

        return Path(path_info, child, callbacks)

    @staticmethod
    def _get_loaded(
        path_element: Union[NodePathStructValue, CommunicationStructValue],
        nodes_loaded: NodesLoaded,
        comms_loaded: CommunicationsLoaded,
    ) -> Union[NodePath, Communication]:
        if isinstance(path_element, NodePathStructValue):
            return nodes_loaded.find_node_path(
                path_element.node_name,
                path_element.subscribe_topic_name,
                path_element.publish_topic_name,
                path_element.publisher_construction_order,
                path_element.subscription_construction_order
            )

        if isinstance(path_element, CommunicationStructValue):
            return comms_loaded.find_communication(
                path_element.topic_name,
                path_element.publish_node_name,
                path_element.subscribe_node_name,
                path_element.publisher_construction_order,
                path_element.subscription_construction_order
            )

        msg = 'Given type is neither NodePathStructValue nor CommunicationStructValue.'
        raise UnsupportedTypeError(msg)

    @property
    def data(self) -> List[Path]:
        return self._data


class CommunicationsLoaded:
    def __init__(
        self,
        communication_values: Tuple[CommunicationStructValue, ...],
        provider: RecordsProvider,
        nodes_loaded: NodesLoaded,
    ) -> None:
        self._data: List[Communication] = []
        for comm_value in communication_values:
            try:
                comm = self._to_runtime(comm_value, provider, nodes_loaded)
                self._data.append(comm)
            except (ItemNotFoundError, MultipleItemFoundError):
                pass

    @property
    def data(self) -> List[Communication]:
        return self._data

    @staticmethod
    def _to_runtime(
        communication_value: CommunicationStructValue,
        provider: RecordsProvider,
        nodes_loaded: NodesLoaded,
    ) -> Communication:
        node_pub = nodes_loaded.find_node(communication_value.publish_node_name)
        node_sub = nodes_loaded.find_node(
            communication_value.subscribe_node_name)

        cb_pubs: Optional[List[CallbackBase]] = None
        if communication_value.publish_callback_names is not None:
            cb_pubs = [
                nodes_loaded.find_callback(cb_name)
                for cb_name
                in communication_value.publish_callback_names
            ]

        cb_sub: Optional[CallbackBase] = None
        if communication_value.subscribe_callback_name is not None:
            cb_name = communication_value.subscribe_callback_name
            cb_sub = nodes_loaded.find_callback(cb_name)

        topic_name = communication_value.topic_name
        subscription_construction_order = communication_value.subscription_construction_order
        publisher_construction_order = communication_value.publisher_construction_order
        subscription = node_sub.get_subscription_from_construction_order(
            topic_name, subscription_construction_order)
        publisher = node_pub.get_publisher(topic_name, publisher_construction_order)

        return Communication(
            node_pub, node_sub,
            publisher, subscription, communication_value,
            provider, cb_pubs, cb_sub)

    def find_communication(
        self,
        topic_name: str,
        publish_node_name: str,
        subscribe_node_name: str,
        publisher_construction_order: int,
        subscription_construction_order: int,
    ) -> Communication:
        def is_target(comm: Communication):
            return comm.publish_node_name == publish_node_name and \
                comm.subscribe_node_name == subscribe_node_name and \
                comm.topic_name == topic_name and \
                comm.publisher_construction_order == publisher_construction_order and \
                comm.subscription_construction_order == subscription_construction_order

        return Util.find_one(is_target, self._data)


class CallbacksLoaded:

    def __init__(
        self,
        callback_values: Tuple[CallbackStructValue, ...],
        provider: RecordsProvider,
        publishers_loaded: PublishersLoaded,
        subscriptions_loaded: SubscriptionsLoaded,
        timers_loaded: TimersLoaded
    ) -> None:

        # Processes related to services are implemented later.
        def _is_ignore_callback(callback: CallbackStructValue):
            ignore_callback_types = (ServiceCallbackStructValue, )
            return isinstance(callback, ignore_callback_types)

        self._callbacks = []

        for cb_info in callback_values:
            try:
                if _is_ignore_callback(cb_info):
                    continue

                self._callbacks.append(self._to_runtime(
                        cb_info, provider, publishers_loaded, subscriptions_loaded, timers_loaded))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        callback_value: CallbackStructValue,
        provider: RecordsProvider,
        publishers_loaded: PublishersLoaded,
        subscriptions_loaded: SubscriptionsLoaded,
        timers_loaded: TimersLoaded,
    ) -> CallbackBase:
        publishers: Optional[List[Publisher]] = None

        if callback_value.publish_topic_names is not None:
            publishers = publishers_loaded.get_publishers(
                None, callback_value.callback_name, None)

        if isinstance(callback_value, TimerCallbackStructValue):
            timer = timers_loaded.get_timer(
                None, callback_value.callback_name, None)
            return TimerCallback(
                callback_value,
                provider,
                publishers,
                timer
            )
        if isinstance(callback_value, SubscriptionCallbackStructValue):
            subscription = subscriptions_loaded.get_subscription(
                None, callback_value.callback_name, None)
            return SubscriptionCallback(
                callback_value,
                provider,
                subscription,
                publishers
            )

        raise UnsupportedTypeError('Unsupported callback type')

    @property
    def data(self) -> List[CallbackBase]:
        return self._callbacks


class CallbackGroupsLoaded:
    def __init__(
        self,
        callback_group_value: Tuple[CallbackGroupStructValue, ...],
        provider: RecordsProvider,
        publishers_loaded: PublishersLoaded,
        subscriptions_loaded: SubscriptionsLoaded,
        timers_loaded: TimersLoaded
    ) -> None:

        self._data = []
        for cbg_info in callback_group_value:
            try:
                self._data.append(
                    self._to_runtime(cbg_info, provider,
                                     publishers_loaded, subscriptions_loaded, timers_loaded))
            except Error as e:
                logger.warning(e)

    @staticmethod
    def _to_runtime(
        cbg_value: CallbackGroupStructValue,
        provider: RecordsProvider,
        publishers_loaded: PublishersLoaded,
        subscriptions_loaded: SubscriptionsLoaded,
        timers_loaded: TimersLoaded
    ) -> CallbackGroup:
        cbs_loaded = CallbacksLoaded(
            cbg_value.callbacks, provider, publishers_loaded, subscriptions_loaded, timers_loaded
        )

        return CallbackGroup(cbg_value, cbs_loaded.data)

    @property
    def data(self) -> List[CallbackGroup]:
        return self._data
