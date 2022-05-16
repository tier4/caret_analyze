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


from typing import Optional, Tuple

from .callback import CallbackStructValue
from .callback_group import CallbackGroupStructValue
from .node_path import NodePathStructValue
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .timer import TimerStructValue
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue
from ..common import Summarizable, Summary, Util
from ..exceptions import ItemNotFoundError


class NodeValue(ValueObject):
    def __init__(
        self,
        node_name: str,
        node_id: Optional[str],
    ) -> None:
        self.__node_name = node_name
        self.__node_id = node_id

    @property
    def node_name(self) -> str:
        return self.__node_name

    @property
    def node_id(self) -> Optional[str]:
        return self.__node_id


class NodeValueWithId(NodeValue):
    def __init__(
        self,
        node_name: str,
        node_id: str,
    ) -> None:
        super().__init__(node_name, node_id)
        self._node_id = node_id

    @property
    def node_id(self) -> str:
        return self._node_id


class NodeStructValue(ValueObject, Summarizable):
    """Executor info for architecture."""

    def __init__(
        self,
        node_name: str,
        publishers: Tuple[PublisherStructValue, ...],
        subscriptions_info: Tuple[SubscriptionStructValue, ...],
        timers: Tuple[TimerStructValue, ...],
        node_paths: Tuple[NodePathStructValue, ...],
        callback_groups: Optional[Tuple[CallbackGroupStructValue, ...]],
        variable_passings: Optional[Tuple[VariablePassingStructValue, ...]],
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
    def publishers(self) -> Tuple[PublisherStructValue, ...]:
        return self._publishers

    @property
    def publish_topic_names(self) -> Tuple[str, ...]:
        return tuple(p.topic_name for p in self._publishers)

    @property
    def subscribe_topic_names(self) -> Tuple[str, ...]:
        return tuple(s.topic_name for s in self._subscriptions)

    @property
    def subscriptions(self) -> Tuple[SubscriptionStructValue, ...]:
        return self._subscriptions

    @property
    def timers(self) -> Tuple[TimerStructValue, ...]:
        return self._timers

    def get_path(
        self,
        subscribe_topic_name: str,
        publish_topic_name: str
    ) -> NodePathStructValue:
        def is_target(path: NodePathStructValue):
            return path.publish_topic_name == publish_topic_name and \
                path.subscribe_topic_name == subscribe_topic_name

        return Util.find_one(is_target, self.paths)

    @property
    def callbacks(self) -> Optional[Tuple[CallbackStructValue, ...]]:
        if self._callback_groups is None:
            return None
        return tuple(Util.flatten(cbg.callbacks for cbg in self._callback_groups))

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self.callbacks is None:
            return None
        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def callback_groups(self) -> Optional[Tuple[CallbackGroupStructValue, ...]]:
        return self._callback_groups

    @property
    def callback_group_names(self) -> Optional[Tuple[str, ...]]:
        if self.callback_groups is None:
            return None
        return tuple(_.callback_group_name for _ in self.callback_groups)

    @property
    def paths(self) -> Tuple[NodePathStructValue, ...]:
        return self._node_paths

    @property
    def variable_passings(self) -> Optional[Tuple[VariablePassingStructValue, ...]]:
        return self._variable_passings_info

    def get_subscription(
        self,
        subscribe_topic_name: str
    ) -> SubscriptionStructValue:

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
    ) -> PublisherStructValue:
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
    ) -> TimerStructValue:
        try:
            return Util.find_one(
                lambda x: x.period == timer_period,
                self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find timer info. '
            msg += f'timer_period: {timer_period}'
            raise ItemNotFoundError(msg)

    @property
    def summary(self) -> Summary:
        d: Summary = Summary()
        d['node'] = self.node_name
        d['callbacks'] = self.callback_names
        d['callback_groups'] = self.callback_group_names
        d['publishers'] = [_.summary for _ in self.publishers]
        d['subscriptions'] = [_.summary for _ in self.subscriptions]
        d['variable_passings'] = []
        if self.variable_passings is not None:
            d['variable_passings'] = [_.summary for _ in self.variable_passings]
        d['paths'] = [_.summary for _ in self.paths]

        return d
