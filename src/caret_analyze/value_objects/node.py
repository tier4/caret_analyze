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

from .callback import CallbackStructValue
from .callback_group import CallbackGroupStructValue
from .node_path import NodePathStructValue
from .publisher import PublisherStructValue
from .service import ServiceStructValue
from .subscription import SubscriptionStructValue
from .timer import TimerStructValue
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue
from ..common import Summarizable, Summary, Util
from ..exceptions import ItemNotFoundError


class NodeValue(ValueObject):
    """
    Value object class for representing a node.

    This class has minimal information and no structure,
    and used as the return value of ArchitectureReader.

    """

    def __init__(
        self,
        node_name: str,
        node_id: str | None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        node_id : str | None
            Identification of the node,
            a value that can be identified when retrieved from the Architecture reader.

        """
        self.__node_name = node_name
        self.__node_id = node_id

    @property
    def node_name(self) -> str:
        return self.__node_name

    @property
    def node_id(self) -> str | None:
        return self.__node_id


class NodeValueWithId(NodeValue):
    """
    Value object class for representing a node path.

    This class has minimal information and no structure,
    and used as the return value of ArchitectureReader.

    """

    def __init__(
        self,
        node_name: str,
        node_id: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        node_id : str | None
            Identification of the node,
            a value that can be identified when retrieved from the Architecture reader.

        """
        super().__init__(node_name, node_id)
        self._node_id = node_id

    @property
    def node_id(self) -> str:
        return self._node_id


class NodeStructValue(ValueObject, Summarizable):
    """
    StructValue object class for representing a node.

    This class is a structure that includes other related StructValue classes, such as callbacks,
    and used as the return value of Architecture object.
    """

    def __init__(
        self,
        node_name: str,
        publishers: tuple[PublisherStructValue, ...],
        subscriptions_info: tuple[SubscriptionStructValue, ...],
        services_info: tuple[ServiceStructValue, ...],
        timers: tuple[TimerStructValue, ...],
        node_paths: tuple[NodePathStructValue, ...],
        callback_groups: tuple[CallbackGroupStructValue, ...] | None,
        variable_passings: tuple[VariablePassingStructValue, ...] | None,
    ) -> None:
        self._node_name = node_name
        self._publishers = publishers
        self._subscriptions = subscriptions_info
        self._services = services_info
        self._timers = timers
        self._callback_groups = callback_groups
        self._node_paths = node_paths
        self._variable_passings_info = variable_passings

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def publishers(self) -> tuple[PublisherStructValue, ...]:
        return self._publishers

    @property
    def publish_topic_names(self) -> tuple[str, ...]:
        return tuple(p.topic_name for p in self._publishers)

    @property
    def subscribe_topic_names(self) -> tuple[str, ...]:
        return tuple(s.topic_name for s in self._subscriptions)

    @property
    def subscriptions(self) -> tuple[SubscriptionStructValue, ...]:
        return self._subscriptions

    @property
    def service_names(self) -> tuple[str, ...]:
        return tuple(s.service_name for s in self._services)

    @property
    def services(self) -> tuple[ServiceStructValue, ...]:
        return self._services

    @property
    def timers(self) -> tuple[TimerStructValue, ...]:
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
    def callbacks(self) -> tuple[CallbackStructValue, ...] | None:
        if self._callback_groups is None:
            return None
        return tuple(Util.flatten(cbg.callbacks for cbg in self._callback_groups))

    @property
    def callback_names(self) -> tuple[str, ...] | None:
        if self.callbacks is None:
            return None
        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def callback_groups(self) -> tuple[CallbackGroupStructValue, ...] | None:
        return self._callback_groups

    @property
    def callback_group_names(self) -> tuple[str, ...] | None:
        if self.callback_groups is None:
            return None
        return tuple(_.callback_group_name for _ in self.callback_groups)

    @property
    def paths(self) -> tuple[NodePathStructValue, ...]:
        return self._node_paths

    @property
    def variable_passings(self) -> tuple[VariablePassingStructValue, ...] | None:
        return self._variable_passings_info

    def get_subscription(
        self,
        subscribe_topic_name: str,
        construction_order: int | None
    ) -> SubscriptionStructValue:

        try:
            def is_target_subscribe(subscribe: SubscriptionStructValue):
                subscribe_topic_name_match = subscribe.topic_name == subscribe_topic_name
                construction_order_match = True
                if construction_order is not None:
                    construction_order_match = subscribe.construction_order == construction_order
                return subscribe_topic_name_match and construction_order_match

            return Util.find_one(is_target_subscribe, self._subscriptions)
        except ItemNotFoundError:
            msg = 'Failed to find subscription info. '
            msg += f'topic_name: {subscribe_topic_name}'
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def get_service(
        self,
        service_name: str,
        construction_order: int | None
    ) -> ServiceStructValue:

        try:
            def is_target_service(service: ServiceStructValue):
                service_name_match = service.service_name == service_name
                construction_order_match = True
                if construction_order is not None:
                    construction_order_match = service.construction_order == construction_order
                return service_name_match and construction_order_match

            return Util.find_one(is_target_service, self._services)
        except ItemNotFoundError:
            msg = 'Failed to find service info. '
            msg += f'service_name: {service_name}'
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def get_publisher(
        self,
        publish_topic_name: str,
        construction_order: int | None
    ) -> PublisherStructValue:
        try:
            def is_target_publisher(publisher: PublisherStructValue):
                publish_topic_name_match = publisher.topic_name == publish_topic_name
                construction_order_match = True
                if construction_order is not None:
                    construction_order_match = publisher.construction_order == construction_order
                return publish_topic_name_match and construction_order_match

            return Util.find_one(is_target_publisher, self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find publisher info. '
            msg += f'topic_name: {publish_topic_name}, '
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def get_timer(
        self,
        timer_period: str,
        construction_order: int | None
    ) -> TimerStructValue:
        try:
            def is_target_timer(timer: TimerStructValue):
                timer_node_name_match = timer.node_name == timer_period
                construction_order_match = True
                if construction_order is not None:
                    construction_order_match = timer.construction_order == construction_order
                return timer_node_name_match and construction_order_match

            return Util.find_one(is_target_timer, self._timers)
        except ItemNotFoundError:
            msg = 'Failed to find timer info. '
            msg += f'timer_period: {timer_period}'
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    @property
    def summary(self) -> Summary:
        d: Summary = Summary()
        d['node'] = self.node_name
        d['callbacks'] = self.callback_names
        d['callback_groups'] = self.callback_group_names
        d['publishers'] = [_.summary for _ in self.publishers]
        d['subscriptions'] = [_.summary for _ in self.subscriptions]
        d['services'] = [_.summary for _ in self.services]
        d['variable_passings'] = []
        if self.variable_passings is not None:
            d['variable_passings'] = [_.summary for _ in self.variable_passings]
        d['paths'] = [_.summary for _ in self.paths]

        return d

# NOTE: DiffNode may be changed when it is refactored.


class DiffNode:

    def __init__(
        self,
        left_node: NodeStructValue,
        right_node: NodeStructValue
    ):
        self._left_node = left_node
        self._right_node = right_node

    def diff_node_pubs(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        set_left_pubs = set(self._left_node.publish_topic_names)
        set_right_pubs = set(self._right_node.publish_topic_names)
        common_node_pubs = set_left_pubs & set_right_pubs
        left_only_pubs = tuple(set_left_pubs - common_node_pubs)
        right_only_pubs = tuple(set_right_pubs - common_node_pubs)
        return left_only_pubs, right_only_pubs

    def diff_node_subs(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        set_left_subs = set(self._left_node.subscribe_topic_names)
        set_right_subs = set(self._right_node.subscribe_topic_names)
        common_node_subs = set_left_subs & set_right_subs
        left_only_subs = tuple(set_left_subs - common_node_subs)
        right_only_subs = tuple(set_right_subs - common_node_subs)
        return left_only_subs, right_only_subs
