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

from functools import lru_cache

from logging import getLogger
from typing import List, Optional, Tuple, Union

from multimethod import multimethod as singledispatchmethod

from .callback import CallbackBase, SubscriptionCallback, TimerCallback
from .callback_group import CallbackGroup
from .communication import Communication, TransformCommunication
from .executor import Executor
from .node import Node
from .node_path import NodePath
from .path import Path
from .publisher import Publisher
from .subscription import Subscription
from .timer import Timer, TimerStructValue
from .transform import (
    TransformBroadcaster,
    TransformBuffer,
)

from .variable_passing import VariablePassing
from ..architecture import Architecture
from ..common import Util
from ..exceptions import (
    Error,
    InvalidArgumentError,
    ItemNotFoundError,
    UnsupportedTypeError,
)
from ..infra.interface import RecordsProvider, RuntimeDataProvider
from ..value_objects import (
    CallbackGroupStructValue,
    CallbackStructValue,
    CommunicationStructValue,
    ExecutorStructValue,
    NodePathStructValue,
    NodeStructValue,
    PathStructValue,
    PublisherStructValue,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    TimerCallbackStructValue,
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformCommunicationStructValue,
    VariablePassingStructValue,
)


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
        self._data = [
            self._to_runtime(exec_val, nodes_loaded)
            for exec_val
            in executors_values
        ]

    @staticmethod
    def _to_runtime(
        executor_value: ExecutorStructValue,
        nodes_loaded: NodesLoaded
    ) -> Executor:
        cbgs = []
        for group_name in executor_value.callback_group_names:
            try:
                cbgs.append(nodes_loaded.find_callback_group(group_name))
            except Error as e:
                logger.warning('Failed to find callback group: %s', e)

        return Executor(
            executor_value,
            cbgs,
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
        self._nodes: List[Node] = [
            self._to_runtime(node_value, provider)
            for node_value
            in node_values
        ]

    @staticmethod
    def _to_runtime(
        node_value: NodeStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Node:
        publishers_loaded = PublishersLoaded(node_value.publishers, provider)
        publishsers = publishers_loaded.data

        subscriptions_loaded = SubscriptionsLoaded(
            node_value.subscriptions, provider)
        subscriptions = subscriptions_loaded.data

        timers_loaded = TimersLoaded(node_value.timers, provider)
        timers = timers_loaded.data

        cbgs: List[CallbackGroup] = []
        if node_value.callback_groups is not None:
            cbgs = CallbackGroupsLoaded(
                node_value.callback_groups,
                provider,
                publishers_loaded,
                subscriptions_loaded,
                timers_loaded
            ).data

        tf_buffer_loaded = TfBufferLoaded(node_value.tf_buffer, provider)
        tf_buffer = tf_buffer_loaded.data

        tf_br_loaded = TfBroadcasterLoaded(node_value.tf_broadcaster, provider)
        tf_br = tf_br_loaded.data

        callbacks = Util.flatten([_.callbacks for _ in cbgs])
        node_paths: List[NodePath]
        node_paths = NodePathsLoaded(
            node_value.paths, provider, publishers_loaded, subscriptions_loaded,
            tf_buffer_loaded, tf_br_loaded, callbacks
        ).data

        variable_passings: List[VariablePassing] = []
        if node_value.variable_passings is not None:
            variable_passings = VariablePassingsLoaded(
                node_value.variable_passings, provider).data

        return Node(
            node_value,
            publishsers,
            subscriptions,
            timers,
            node_paths,
            cbgs,
            variable_passings,
            tf_buffer,
            tf_br
        )

    @property
    def callback_groups(self) -> List[CallbackGroup]:
        cbgs: List[CallbackGroup] = []
        for node in self._nodes:
            if node.callback_groups is not None:
                cbgs += node.callback_groups
        return cbgs

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

    @lru_cache
    def find_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackGroup:
        try:
            cbgs = self.callback_groups
            if cbgs is None:
                raise ItemNotFoundError('')

            return Util.find_one(
                lambda x: x.callback_group_name == callback_group_name,
                cbgs
            )
        except ItemNotFoundError:
            raise ItemNotFoundError(
                f'Failed to find node. callback_group_name = {callback_group_name}')

    @lru_cache
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

    @lru_cache
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

    @lru_cache
    def find_node_path(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        tf_broadcast_frame_id: Optional[str],
        tf_broadcast_child_frame_id: Optional[str],
        tf_buffer_lookup_frame_id: Optional[str],
        tf_buffer_lookup_child_frame_id: Optional[str],
        tf_buffer_listen_frame_id: Optional[str],
        tf_buffer_listen_child_frame_id: Optional[str],
    ) -> NodePath:
        def is_target(node_path: NodePath):
            return node_path.publish_topic_name == publish_topic_name and \
                node_path.subscribe_topic_name == subscribe_topic_name and \
                node_path.node_name == node_name and \
                node_path.tf_broadcast_frame_id == tf_broadcast_frame_id and \
                node_path.tf_broadcast_child_frame_id == tf_broadcast_child_frame_id and \
                node_path.tf_buffer_lookup_frame_id == tf_buffer_lookup_frame_id and \
                node_path.tf_buffer_lookup_child_frame_id == tf_buffer_lookup_child_frame_id and\
                node_path.tf_buffer_listen_frame_id == tf_buffer_listen_frame_id and \
                node_path.tf_buffer_listen_child_frame_id == tf_buffer_listen_child_frame_id

        try:
            node_paths = Util.flatten([n.paths for n in self._nodes])
            return Util.find_one(is_target, node_paths)
        except ItemNotFoundError:
            msg = 'Failed to find node path. '
            msg += f'node_name: {node_name}. '
            msg += f'publish_topic_name: {publish_topic_name}. '
            msg += f'subscribe_topic_name: {subscribe_topic_name}. '
            raise ItemNotFoundError(msg)


class PublishersLoaded:

    def __init__(
        self,
        publishers_info: Tuple[PublisherStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._pubs = [
            self._to_runtime(pub_info, provider)
            for pub_info
            in publishers_info
        ]

    @staticmethod
    def _to_runtime(
        publisher_info: PublisherStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Publisher:
        return Publisher(publisher_info, provider)

    @property
    def data(self) -> List[Publisher]:
        return self._pubs

    def get_publishers_by_cb_name(
        self,
        callback_name: str
    ) -> List[Publisher]:
        def is_target(publisher: Publisher):
            if publisher.callback_names is None:
                return None
            return callback_name in publisher.callback_names

        return Util.filter_items(is_target, self._pubs)

    @singledispatchmethod
    def get_publisher(self, arg) -> Publisher:
        raise InvalidArgumentError('')

    @get_publisher.register
    def _get_publisher_dict(
        self,
        node_name: Optional[str],
        topic_name: Optional[str],
    ) -> Publisher:
        try:
            def is_target(publisher: Publisher):
                return publisher.node_name == node_name and \
                    publisher.topic_name == topic_name
            return Util.find_one(is_target, self._pubs)
        except ItemNotFoundError:
            msg = 'Failed to find publisher. '
            msg += f'node_name: {node_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    @get_publisher.register
    def _get_publisher_struct(
        self,
        publisher: PublisherStructValue
    ) -> Publisher:
        return self._get_publisher_dict(
            publisher.node_name,
            publisher.topic_name
        )


class SubscriptionsLoaded:
    def __init__(
        self,
        subscriptions_info: Tuple[SubscriptionStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._subs = [
            self._to_runtime(sub_info, provider)
            for sub_info
            in subscriptions_info
        ]

    @staticmethod
    def _to_runtime(
        subscription_info: SubscriptionStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> Subscription:
        return Subscription(subscription_info, provider)

    @property
    def data(self) -> List[Subscription]:
        return self._subs

    @lru_cache
    def get_subscription_by_cb_name(
        self,
        callback_name: str
    ) -> Subscription:
        def is_target(subscription: Subscription):
            if subscription.callback_name is None:
                return None
            return callback_name == subscription.callback_name
        return Util.find_one(is_target, self._subs)

    @singledispatchmethod
    def get_subscription(self, arg) -> Subscription:
        raise InvalidArgumentError('')

    @get_subscription.register
    def _get_subscription_dict(
        self,
        node_name: Optional[str],
        topic_name: Optional[str],
    ) -> Subscription:
        def is_target(subscription: Subscription):
            return subscription.node_name == node_name and \
                subscription.topic_name == topic_name
        try:
            return Util.find_one(is_target, self._subs)
        except ItemNotFoundError:
            msg = 'Failed to find subscription. '
            msg += f'node_name: {node_name}, '
            msg += f'topic_name: {topic_name}, '
            raise ItemNotFoundError(msg)

    @get_subscription.register
    def _get_subscription_struct(self, subscription: SubscriptionStructValue) -> Subscription:
        return self._get_subscription_dict(
            subscription.node_name,
            subscription.topic_name)


class TimersLoaded:
    def __init__(
        self,
        timer_info: Tuple[TimerStructValue, ...],
        provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        self._timers = [
            self._to_runtime(timer_info, provider)
            for timer_info
            in timer_info
        ]

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
        tf_buffer_loaded: TfBufferLoaded,
        tf_broadcaster_loaded: TfBroadcasterLoaded,
        callbacks: List[CallbackBase],
    ) -> None:
        self._data = [
            self._to_runtime(
                node_path_value, provider, publisher_loaded, subscription_loaded,
                tf_buffer_loaded, tf_broadcaster_loaded, callbacks)
            for node_path_value
            in node_path_values
        ]

    @staticmethod
    def _to_runtime(
        node_path_value: NodePathStructValue,
        provider: RecordsProvider,
        publisher_loaded: PublishersLoaded,
        subscription_loaded: SubscriptionsLoaded,
        tf_buffer_loaded: TfBufferLoaded,
        tf_broadcaster_loaded: TfBroadcasterLoaded,
        callbacks: List[CallbackBase]
    ) -> NodePath:
        publisher: Optional[Publisher] = None
        subscription: Optional[Subscription] = None

        publisher = None
        if node_path_value.publisher is not None:
            publisher = publisher_loaded.get_publisher(
                node_path_value.publisher)

        subscription = None
        if node_path_value.subscription is not None:
            subscription = subscription_loaded.get_subscription(
                node_path_value.subscription)

        tf_broadcaster = None
        if node_path_value.tf_frame_broadcaster is not None and\
                tf_broadcaster_loaded.data is not None:
            tf_br = tf_broadcaster_loaded.data
            tf_broadcaster = tf_br.get(
                node_path_value.tf_frame_broadcaster.transform)

        tf_buffer = None
        if node_path_value.tf_frame_buffer is not None and\
                tf_buffer_loaded.data is not None:
            tf_buf = tf_buffer_loaded.data
            tf_buffer = tf_buf.get(
                node_path_value.tf_frame_buffer.listen_transform,
                node_path_value.tf_frame_buffer.lookup_transform
            )
        return NodePath(node_path_value, provider, subscription,
                        publisher, callbacks, tf_broadcaster, tf_buffer)

    @property
    def data(self) -> List[NodePath]:
        return self._data


class TfBufferLoaded:

    def __init__(
        self,
        tf_buffer: Optional[TransformBufferStructValue],
        provider: RecordsProvider,
    ) -> None:
        self._data: Optional[TransformBuffer] = None
        if tf_buffer is not None:
            self._data = self._to_runtime(tf_buffer, provider)

    @staticmethod
    def _to_runtime(
        tf_buffer: TransformBufferStructValue,
        provider: RecordsProvider,
    ) -> TransformBuffer:
        return TransformBuffer(
            tf_buffer, provider
        )

    # @singledispatchmethod
    # def get_frame_buffer(self, arg) -> TransformFrameBuffer:
    #     pass

    # @get_frame_buffer.register
    # def _get_frame_buffer_struct(
    #     self,
    #     tf_buffer_struct: TransformBufferStructValue,
    # ) -> TransformFrameBuffer:
    #     return

    # @get_frame_buffer.register
    # def _get_frame_buffer_dict(
    #     self,
    #     listen_frame_id: str,
    #     listen_child_frame_id: str,
    #     lookup_frame_id: str,
    #     lookup_child_frame_id: str
    # ) -> TransformFrameBuffer:
    #     return self._data.get(
    #         listen_frame_id,
    #         listen_child_frame_id,
    #         lookup_frame_id,
    #         lookup_child_frame_id)

    @property
    def data(self) -> Optional[TransformBuffer]:
        return self._data


class TfBroadcasterLoaded:
    def __init__(
        self,
        tf_broadcaster: Optional[TransformBroadcasterStructValue],
        provider: RecordsProvider,
    ) -> None:
        self._data: Optional[TransformBroadcaster] = None
        if tf_broadcaster is not None:
            self._data = self._to_runtime(tf_broadcaster, provider)

    @staticmethod
    def _to_runtime(
        tf_br: TransformBroadcasterStructValue,
        provider: RecordsProvider,
    ) -> TransformBroadcaster:
        return TransformBroadcaster(
            tf_br,
            provider
        )

    @property
    def data(self) -> Optional[TransformBroadcaster]:
        return self._data


class VariablePassingsLoaded:
    def __init__(
        self,
        variable_passings_info: Tuple[VariablePassingStructValue, ...],
        provider: RecordsProvider
    ) -> None:
        self._var_passes = [
            self._to_runtime(var_pass, provider)
            for var_pass
            in variable_passings_info
        ]

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
                self._data.append(
                    self._to_runtime(path_info, nodes_loaded, comms_loaded)
                )
            except Error as e:
                logger.warning('Failed to load path: %s', e)

    @staticmethod
    def _to_runtime(
        path_info: PathStructValue,
        nodes_loaded: NodesLoaded,
        comms_loaded: CommunicationsLoaded,
    ) -> Path:
        child: List[Union[NodePath, Communication,
                          TransformCommunication]] = []
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
        path_element: Union[
            NodePathStructValue, CommunicationStructValue, TransformCommunicationStructValue],
        nodes_loaded: NodesLoaded,
        comms: CommunicationsLoaded,
    ) -> Union[NodePath, Communication, TransformCommunication]:
        if isinstance(path_element, NodePathStructValue):
            return nodes_loaded.find_node_path(
                path_element.node_name,
                path_element.subscribe_topic_name,
                path_element.publish_topic_name,
                path_element.tf_broadcast_frame_id,
                path_element.tf_broadcast_child_frame_id,
                path_element.tf_buffer_lookup_frame_id,
                path_element.tf_buffer_lookup_child_frame_id,
                path_element.tf_buffer_listen_frame_id,
                path_element.tf_buffer_listen_child_frame_id,
            )

        if isinstance(path_element, CommunicationStructValue):
            return comms.get(path_element)

        if isinstance(path_element, TransformCommunicationStructValue):
            return comms.get(path_element)

        msg = 'Given type is neither NodePathStructInfo nor CommunicationStructInfo.'
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
                if isinstance(comm_value, CommunicationStructValue):
                    comm = self._to_runtime(comm_value, provider, nodes_loaded)
                elif isinstance(comm_value, TransformCommunicationStructValue):
                    comm = self._to_runtime_tf(comm_value, provider, nodes_loaded)
                else:
                    raise NotImplementedError('')
                self._data.append(comm)
            except Error as e:
                logger.warning(e)

    @property
    def data(self) -> List[Communication]:
        return self._data

    @staticmethod
    def _to_runtime_tf(
        communication_value: TransformCommunicationStructValue,
        provider: RecordsProvider,
        nodes_loaded: NodesLoaded,
    ) -> TransformCommunication:
        node_br = nodes_loaded.find_node(communication_value.broadcaster.node_name)
        node_buf = nodes_loaded.find_node(communication_value.buffer.lookup_node_name)

        assert node_br.tf_broadcaster is not None
        tf_broadcaster = node_br.tf_broadcaster.get(
            communication_value.broadcast_transform)

        assert node_buf.tf_buffer is not None
        tf_buffer = node_buf.tf_buffer.get(
            communication_value.listen_transform,
            communication_value.lookup_transform)

        return TransformCommunication(
            node_br, node_buf,
            tf_broadcaster, tf_buffer, communication_value,
            provider)

    @staticmethod
    def _to_runtime(
        communication_value: CommunicationStructValue,
        provider: RecordsProvider,
        nodes_loaded: NodesLoaded,
    ) -> Communication:
        node_pub = nodes_loaded.find_node(communication_value.publish_node_name)
        node_sub = nodes_loaded.find_node(
            communication_value.subscribe_node_name)

        topic_name = communication_value.topic_name
        subscription = node_sub.get_subscription(topic_name)
        publisher = node_pub.get_publisher(topic_name)

        return Communication(
            node_pub, node_sub,
            publisher, subscription, communication_value,
            provider)

    @singledispatchmethod
    def get(self, arg) -> Communication:
        raise InvalidArgumentError('')

    @get.register
    def _get_strct(
        self,
        comm: CommunicationStructValue
    ) -> Communication:
        return self._get_dict(
            comm.topic_name,
            comm.publish_node_name,
            comm.subscribe_node_name
        )

    @get.register
    def _get_dict(
        self,
        topic_name: str,
        publish_node_name: str,
        subscribe_node_name: str,
    ) -> Communication:
        def is_target(comm: Union[Communication, TransformCommunication]):
            if isinstance(comm, Communication):
                return (
                    comm.topic_name == topic_name
                    and comm.publish_node_name == publish_node_name
                    and comm.subscribe_node_name == subscribe_node_name
                )
            return False

        return Util.find_one(is_target, self._data)

    @get.register
    def _get_tf_strct(
        self,
        comm: TransformCommunicationStructValue
    ) -> TransformCommunication:
        return self._get_tf_dict(
            comm.broadcast_node_name,
            comm.lookup_node_name,
            comm.broadcast_frame_id,
            comm.broadcast_child_frame_id,
            comm.lookup_frame_id,
            comm.lookup_target_frame_id
        )

    @get.register
    def _get_tf_dict(
        self,
        broadcast_node_name: str,
        lookup_node_name: str,
        broadcast_frame_id: str,
        broadcast_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str,
    ) -> TransformCommunication:
        def is_target(comm: Union[Communication, TransformCommunication]):
            if isinstance(comm, TransformCommunication):
                return comm.broadcast_node_name == broadcast_node_name and \
                    comm.lookup_node_name == lookup_node_name and \
                    comm.tf_broadcaster.frame_id == broadcast_frame_id and \
                    comm.tf_broadcaster.child_frame_id == broadcast_child_frame_id and \
                    comm.tf_buffer.lookup_source_frame_id == lookup_frame_id and \
                    comm.tf_buffer.lookup_target_frame_id == lookup_child_frame_id
            return False

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
        self._callbacks = []
        for cb_info in callback_values:
            try:
                self._callbacks.append(
                    self._to_runtime(
                        cb_info, provider, publishers_loaded, subscriptions_loaded, timers_loaded)
                )
            except Error as e:
                logger.warning('Failed to load callback: %s', e)

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
            publishers = publishers_loaded.get_publishers_by_cb_name(
                callback_value.callback_name)

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
            subscription = subscriptions_loaded.get_subscription_by_cb_name(
                callback_value.callback_name)
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
                    self._to_runtime(
                        cbg_info, provider, publishers_loaded, subscriptions_loaded, timers_loaded)
                )
            except Error as e:
                logger.warning('Failed to load callback group: %s', e)

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
