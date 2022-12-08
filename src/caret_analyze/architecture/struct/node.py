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

from typing import List, Optional

from .callback import CallbackStruct
from .callback_group import CallbackGroupStruct
from .message_context import MessageContextStruct
from .node_path import NodePathStruct
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from .timer import TimerStruct
from .variable_passing import VariablePassingStruct
from ...common import Util
from ...exceptions import ItemNotFoundError
from ...value_objects import NodeStructValue


class NodeStruct():
    """Executor info for architecture."""

    def __init__(
        self,
        node_name: str,
        publishers: List[PublisherStruct],
        subscriptions_info: List[SubscriptionStruct],
        timers: List[TimerStruct],
        node_paths: List[NodePathStruct],
        callback_groups: Optional[List[CallbackGroupStruct]],
        variable_passings: Optional[List[VariablePassingStruct]],
    ) -> None:
        self._node_name = node_name
        self._publishers = publishers
        self._subscriptions = subscriptions_info
        self._timers = timers
        self._callback_groups = callback_groups
        self._node_paths = node_paths
        self._variable_passings_info = variable_passings

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def publishers(self) -> List[PublisherStruct]:
        return self._publishers

    @property
    def publish_topic_names(self) -> List[str]:
        return [p.topic_name for p in self._publishers]

    @property
    def subscribe_topic_names(self) -> List[str]:
        return [s.topic_name for s in self._subscriptions]

    @property
    def subscriptions(self) -> List[SubscriptionStruct]:
        return self._subscriptions

    @property
    def timers(self) -> List[TimerStruct]:
        return self._timers

    def get_path(
        self,
        subscribe_topic_name: str,
        publish_topic_name: str
    ) -> NodePathStruct:
        def is_target(path: NodePathStruct):
            return path.publish_topic_name == publish_topic_name and \
                path.subscribe_topic_name == subscribe_topic_name

        return Util.find_one(is_target, self.paths)

    @property
    def callbacks(self) -> Optional[List[CallbackStruct]]:
        if self._callback_groups is None:
            return None
        return list(Util.flatten(cbg.callbacks for cbg in self._callback_groups))

    @property
    def callback_names(self) -> Optional[List[str]]:
        if self.callbacks is None:
            return None
        return [_.callback_name for _ in self.callbacks]

    @property
    def callback_groups(self) -> Optional[List[CallbackGroupStruct]]:
        return self._callback_groups

    @property
    def callback_group_names(self) -> Optional[List[str]]:
        if self.callback_groups is None:
            return None
        return [_.callback_group_name for _ in self.callback_groups]

    @property
    def paths(self) -> List[NodePathStruct]:
        return self._node_paths

    @property
    def variable_passings(self) -> Optional[List[VariablePassingStruct]]:
        return self._variable_passings_info

    def get_subscription(
        self,
        subscribe_topic_name: str
    ) -> SubscriptionStruct:

        try:
            return Util.find_one(
                lambda x: x.topic_name == subscribe_topic_name,
                self._subscriptions)
        except ItemNotFoundError:
            msg = 'Failed to find subscription info. '
            msg += f'topic_name: {subscribe_topic_name}'
            raise ItemNotFoundError(msg)

    def get_publisher(
        self,
        publish_topic_name: str
    ) -> PublisherStruct:
        try:
            return Util.find_one(
                lambda x: x.topic_name == publish_topic_name,
                self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find publisher info. '
            msg += f'topic_name: {publish_topic_name}'
            raise ItemNotFoundError(msg)

    def get_timer(
        self,
        timer_period: str
    ) -> TimerStruct:
        try:
            return Util.find_one(
                lambda x: x.period == timer_period,
                self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find timer info. '
            msg += f'timer_period: {timer_period}'
            raise ItemNotFoundError(msg)

    def to_value(self) -> NodeStructValue:
        return NodeStructValue(
            self.node_name,
            tuple(v.to_value() for v in self.publishers),
            tuple(v.to_value() for v in self.subscriptions),
            tuple(v.to_value() for v in self.timers),
            tuple(v.to_value() for v in self.paths),
            None if self.callback_groups is None
            else tuple(v.to_value() for v in self.callback_groups),
            None if self.variable_passings is None
            else tuple(v.to_value() for v in self.variable_passings))

    def assign_message_context(self, context_type, sub_topic_name: str, pub_topic_name: str):
        try:
            path = self.get_path(sub_topic_name, pub_topic_name)
            path.message_context =\
                MessageContextStruct.create_instance(context_type, {},
                                                     self.node_name, path.subscription,
                                                     path.publisher, None)
        except ItemNotFoundError:
            subscription: SubscriptionStruct = \
                Util.find_one(lambda x: x.topic_name == sub_topic_name, self._subscriptions)
            publisher: PublisherStruct = \
                Util.find_one(lambda x: x.topic_name == pub_topic_name, self._publishers)
            message_context =\
                MessageContextStruct.create_instance(context_type, {},
                                                     self.node_name, subscription,
                                                     publisher, None)
            path = NodePathStruct(self.node_name, subscription, publisher,
                                  None, message_context)
            self._node_paths.append(path)

    def assign_publisher(self, pub_topic_name: str, callback_function_name: str):
        callback: CallbackStruct = \
            Util.find_one(lambda x: x.callback_name == callback_function_name, self.callbacks)

        callback.assign_publisher(pub_topic_name)
        publisher = PublisherStruct(self.node_name, pub_topic_name, [callback])

        self._publishers.append(publisher)

    def assign_message_passings(self, src_callback_name: str, des_callback_name: str):
        source_callback =\
            Util.find_one(lambda x: x.callback_name == src_callback_name, self.callbacks)
        destination_callback =\
            Util.find_one(lambda x: x.callback_name == des_callback_name, self.callbacks)

        passing = VariablePassingStruct(self.node_name, destination_callback, source_callback)

        if self._variable_passings_info is None:
            self._variable_passings_info = [passing]
        else:
            self._variable_passings_info.append(passing)

    def rename_node(self, src: str, dst: str) -> None:
        if self.node_name == src:
            self._node_name = dst

        for p in self._publishers:
            p.rename_node(src, dst)

        for s in self._subscriptions:
            s.rename_node(src, dst)

        for t in self._timers:
            t.rename_node(src, dst)

        if self._callback_groups is not None:
            for c in self._callback_groups:
                c.rename_node(src, dst)

        for n in self._node_paths:
            n.rename_node(src, dst)

        if self._variable_passings_info is not None:
            for v in self._variable_passings_info:
                v.rename_node(src, dst)

    def rename_topic(self, src: str, dst: str) -> None:
        for p in self._publishers:
            p.rename_topic(src, dst)

        for s in self._subscriptions:
            s.rename_topic(src, dst)

        for t in self._timers:
            t.rename_topic(src, dst)

        if self._callback_groups is not None:
            for c in self._callback_groups:
                c.rename_topic(src, dst)

        for n in self._node_paths:
            n.rename_topic(src, dst)

        if self._variable_passings_info is not None:
            for v in self._variable_passings_info:
                v.rename_topic(src, dst)
