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

from typing import Dict, Optional, Tuple

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
        publishers: Tuple[PublisherStruct, ...],
        subscriptions_info: Tuple[SubscriptionStruct, ...],
        timers: Tuple[TimerStruct, ...],
        node_paths: Tuple[NodePathStruct, ...],
        callback_groups: Optional[Tuple[CallbackGroupStruct, ...]],
        variable_passings: Optional[Tuple[VariablePassingStruct, ...]],
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
    def publishers(self) -> Tuple[PublisherStruct, ...]:
        return self._publishers

    @property
    def publish_topic_names(self) -> Tuple[str, ...]:
        return tuple(p.topic_name for p in self._publishers)

    @property
    def subscribe_topic_names(self) -> Tuple[str, ...]:
        return tuple(s.topic_name for s in self._subscriptions)

    @property
    def subscriptions(self) -> Tuple[SubscriptionStruct, ...]:
        return self._subscriptions

    @property
    def timers(self) -> Tuple[TimerStruct, ...]:
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
    def callbacks(self) -> Optional[Tuple[CallbackStruct, ...]]:
        if self._callback_groups is None:
            return None
        return tuple(Util.flatten(cbg.callbacks for cbg in self._callback_groups))

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self.callbacks is None:
            return None
        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def callback_groups(self) -> Optional[Tuple[CallbackGroupStruct, ...]]:
        return self._callback_groups

    @property
    def callback_group_names(self) -> Optional[Tuple[str, ...]]:
        if self.callback_groups is None:
            return None
        return tuple(_.callback_group_name for _ in self.callback_groups)

    @property
    def paths(self) -> Tuple[NodePathStruct, ...]:
        return self._node_paths

    @property
    def variable_passings(self) -> Optional[Tuple[VariablePassingStruct, ...]]:
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
        path = self.get_path(sub_topic_name, pub_topic_name)
        path.message_context =\
            MessageContextStruct.create_instance(context_type, Dict(),
                                                 self.node_name, path.subscription,
                                                 path.publisher, None)

    def assign_publisher(self, pub_topic_name: str,
                         callback_function: Optional[CallbackStruct]):
        publisher = PublisherStruct(self.node_name, pub_topic_name,
                                    () if callback_function is None else (callback_function,))
        publisher_list = list(self.publishers)
        publisher_list.append(publisher)
        self._publishers = tuple(publisher_list)

    def assign_message_passings(self, source_callback: CallbackStruct,
                                destination_callback: CallbackStruct):
        passing = VariablePassingStruct(self.node_name, destination_callback, source_callback)
        passing_list = [] if self.variable_passings is None else list(self.variable_passings)
        passing_list.append(passing)
        self._variable_passings_info = tuple(passing_list)
