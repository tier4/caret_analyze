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

from .callback import CallbackStruct
from .callback_group import CallbackGroupStruct
from .node_path import NodePathStruct
from .publisher import PublisherStruct
from .service import ServiceStruct
from .subscription import SubscriptionStruct
from .timer import TimerStruct
from .variable_passing import VariablePassingStruct
from ...common import Util
from ...exceptions import ItemNotFoundError
from ...value_objects import (NodeStructValue, PublishTopicInfoValue)


class NodeStruct():
    """Executor info for architecture."""

    def __init__(
        self,
        node_name: str,
        callbacks: list[CallbackStruct],
        publishers: list[PublisherStruct],
        subscriptions_info: list[SubscriptionStruct],
        services: list[ServiceStruct],
        timers: list[TimerStruct],
        node_paths: list[NodePathStruct],
        callback_groups: list[CallbackGroupStruct] | None,
        variable_passings: list[VariablePassingStruct] | None,
    ) -> None:
        self._node_name = node_name
        self._callbacks = callbacks
        self._publishers = publishers
        self._subscriptions = subscriptions_info
        self._services = services
        self._timers = timers
        self._callback_groups = callback_groups
        self._node_paths = node_paths
        self._variable_passings_info = variable_passings

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def publishers(self) -> list[PublisherStruct]:
        return self._publishers

    @property
    def publish_topic_names(self) -> list[str]:
        return [p.topic_name for p in self._publishers]

    @property
    def subscribe_topic_names(self) -> list[str]:
        return [s.topic_name for s in self._subscriptions]

    @property
    def subscriptions(self) -> list[SubscriptionStruct]:
        return self._subscriptions

    @property
    def services(self) -> list[ServiceStruct]:
        return self._services

    @property
    def timers(self) -> list[TimerStruct]:
        return self._timers

    @property
    def callbacks(self) -> list[CallbackStruct] | None:
        return self._callbacks

    @property
    def callback_names(self) -> list[str] | None:
        if self.callbacks is None:
            return None
        return [_.callback_name for _ in self.callbacks]

    @property
    def callback_groups(self) -> list[CallbackGroupStruct] | None:
        return self._callback_groups

    @property
    def callback_group_names(self) -> list[str] | None:
        if self.callback_groups is None:
            return None
        return [_.callback_group_name for _ in self.callback_groups]

    @property
    def paths(self) -> list[NodePathStruct]:
        return self._node_paths

    @property
    def variable_passings(self) -> list[VariablePassingStruct] | None:
        return self._variable_passings_info

    def get_subscription(
        self,
        subscribe_topic_name: str,
        construction_order: int | None = None
    ) -> SubscriptionStruct:

        def is_target(subscription: SubscriptionStruct):
            match = subscription.topic_name == subscribe_topic_name
            if construction_order is not None:
                match &= subscription.construction_order == construction_order
            return match

        try:
            return Util.find_one(is_target, self._subscriptions)
        except ItemNotFoundError:
            msg = 'Failed to find subscription info. '
            msg += f'topic_name: {subscribe_topic_name} '
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def get_service(
        self,
        service_name: str,
        construction_order: int | None
    ) -> ServiceStruct:

        def is_target(service: ServiceStruct):
            match = service.service_name == service_name
            if construction_order is not None:
                match &= service.construction_order == construction_order
            return match

        try:
            return Util.find_one(is_target, self._services)
        except ItemNotFoundError:
            msg = 'Failed to find service info. '
            msg += f'service_name: {service_name} '
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def get_publisher(
        self,
        publish_topic_name: str,
        construction_order: int | None
    ) -> PublisherStruct:
        try:
            def is_target_publisher(publisher: PublisherStruct):
                return publisher.topic_name == publish_topic_name and \
                    publisher.construction_order == construction_order

            return Util.find_one(is_target_publisher, self._publishers)

        except ItemNotFoundError:
            msg = 'Failed to find publisher info. '
            msg += f'topic_name: {publish_topic_name}, '
            msg += f'construction_order: {construction_order}'
            raise ItemNotFoundError(msg)

    def to_value(self) -> NodeStructValue:
        return NodeStructValue(
            self.node_name,
            tuple(v.to_value() for v in self.callbacks),
            tuple(v.to_value() for v in self.publishers),
            tuple(v.to_value() for v in self.subscriptions),
            tuple(v.to_value() for v in self.services),
            tuple(v.to_value() for v in self.timers),
            tuple(v.to_value() for v in self.paths),
            None if self.callback_groups is None
            else tuple(v.to_value() for v in self.callback_groups),
            None if self.variable_passings is None
            else tuple(v.to_value() for v in self.variable_passings))

    def update_node_path(self, paths: list[NodePathStruct]) -> None:
        self._node_paths = paths

    # def update_message_context(self, node_name: str, context_type: str,
    #                            sub_topic_name: str, pub_topic_name: str):
    #     # To assign message context, update_node_path() is called in Architecture.
    #     # This is because module dependency.
    #     # TODO: Refactoring module dependency
    #     pass

    def insert_publisher_callback(self,
                                  publish_topic_name: str,
                                  callback_name: str,
                                  publisher_construction_order: int) -> None:
        callback: CallbackStruct = \
            Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)
        pub_info = PublishTopicInfoValue(publish_topic_name, publisher_construction_order)
        callback.insert_publisher(pub_info)

        def is_target(publish: PublisherStruct):
            match = publish.topic_name == publish_topic_name
            match &= publish.construction_order == publisher_construction_order
            return match

        try:
            publisher: PublisherStruct = \
                Util.find_one(is_target, self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find subscription info. '
            msg += f'topic_name: {publish_topic_name} '
            msg += f'construction_order: {publisher_construction_order}'
            raise ItemNotFoundError(msg)

        publisher.insert_callback(callback)

    def insert_variable_passing(self, callback_name_write: str, callback_name_read: str) -> None:
        callback_write: CallbackStruct =\
            Util.find_one(lambda x: x.callback_name == callback_name_write, self.callbacks)
        callback_read: CallbackStruct =\
            Util.find_one(lambda x: x.callback_name == callback_name_read, self.callbacks)

        passing = VariablePassingStruct(self.node_name, callback_write, callback_read)
        if self._variable_passings_info is None:
            self._variable_passings_info = [passing]
        elif (callback_name_read, callback_name_write) not in \
                [(passing.callback_name_read, passing.callback_name_write)
                    for passing in self._variable_passings_info]:
            self._variable_passings_info.append(passing)

    def remove_publisher_and_callback(self,
                                      publish_topic_name: str,
                                      callback_name: str,
                                      publisher_construction_order: int) -> None:
        callback: CallbackStruct = \
            Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)
        pub_info = PublishTopicInfoValue(publish_topic_name, publisher_construction_order)
        callback.remove_publisher(pub_info)

        def is_target(publish: PublisherStruct):
            match = publish.topic_name == publish_topic_name
            match &= publish.construction_order == publisher_construction_order
            return match

        try:
            publisher: PublisherStruct = \
                Util.find_one(is_target, self._publishers)
        except ItemNotFoundError:
            msg = 'Failed to find subscription info. '
            msg += f'topic_name: {publish_topic_name} '
            msg += f'construction_order: {publisher_construction_order}'
            raise ItemNotFoundError(msg)

        publisher.remove_callback(callback)

    def remove_variable_passing(self, callback_name_write: str, callback_name_read: str) -> None:
        if self._variable_passings_info:
            self._variable_passings_info =\
                [passing for passing in self._variable_passings_info
                 if (callback_name_write, callback_name_read) !=
                 (passing.callback_name_write, passing.callback_name_read)]

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

    def get_publisher_from_callback(self, callback_name: str) -> list[PublisherStruct]:
        l: list[PublisherStruct] = []
        for publisher in self.publishers:
            if publisher.callback_names and callback_name in publisher.callback_names:
                l.append(publisher)
        return l

    def get_subscription_from_callback(self, callback_name: str) -> SubscriptionStruct | None:
        for subscription in self.subscriptions:
            if subscription.callback_name == callback_name:
                return subscription
        return None
