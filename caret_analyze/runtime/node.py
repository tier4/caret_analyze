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

from typing import List, Optional

from caret_analyze.value_objects import NodeStructValue

from ..common import Util, CustomDict
from .callback import CallbackBase
from .callback_group import CallbackGroup
from .publisher import Publisher
from .node_path import NodePath
from .subscription import Subscription
from .variable_passing import VariablePassing
from ..exceptions import ItemNotFoundError


class Node:

    def __init__(
        self,
        node: NodeStructValue,
        publishers: List[Publisher],
        subscription: List[Subscription],
        node_paths: List[NodePath],
        callback_groups: Optional[List[CallbackGroup]],
        variable_passings: Optional[List[VariablePassing]],
    ) -> None:
        self._val = node
        self._publishers = publishers
        self._subscriptions = subscription
        self._paths = node_paths
        self._callback_groups = callback_groups
        self._variable_passings = variable_passings

    @property
    def callback_groups(self) -> Optional[List[CallbackGroup]]:
        return self._callback_groups

    @property
    def node_name(self) -> str:
        return self._val.node_name

    @property
    def callbacks(self) -> Optional[List[CallbackBase]]:
        if self.callback_groups is None:
            return None
        return Util.flatten([cbg.callbacks for cbg in self.callback_groups])

    @property
    def callback_names(self) -> Optional[List[str]]:
        if self.callbacks is None:
            return None
        return [c.callback_name for c in self.callbacks]

    @property
    def variable_passings(self) -> Optional[List[VariablePassing]]:
        return self._variable_passings

    @property
    def publishers(self) -> List[Publisher]:
        return self._publishers

    @property
    def publish_topics(self) -> List[str]:
        return [_.topic_name for _ in self._publishers]

    @property
    def paths(self) -> List[NodePath]:
        return self._paths

    @property
    def subscriptions(self) -> List[Subscription]:
        return self._subscriptions

    @property
    def subscribe_topics(self) -> List[str]:
        return [_.topic_name for _ in self._subscriptions]

    @property
    def callback_group_names(self) -> Optional[List[str]]:
        if self.callback_groups is None:
            return None
        return [_.callback_group_name for _ in self.callback_groups]

    @property
    def summary(self) -> CustomDict:
        return self._val.summary

    def get_callback_group(self, callback_group_name: str) -> CallbackGroup:
        if self._callback_groups is None:
            raise ItemNotFoundError('Callback group is None.')

        return Util.find_one(
            lambda x: x.callback_group_name == callback_group_name, self._callback_groups)

    def get_path(
        self,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
    ) -> NodePath:
        def is_target(path: NodePath):
            return path.publish_topic_name == publish_topic_name and \
                path.subscribe_topic_name == subscribe_topic_name

        return Util.find_one(is_target, self.paths)

    def get_callback(self, callback_name: str) -> CallbackBase:
        if self.callbacks is None:
            raise ItemNotFoundError('Callback is None.')

        return Util.find_one(lambda x: x.callback_name == callback_name, self.callbacks)

    def get_subscription(self, topic_name: str) -> Subscription:
        return Util.find_one(lambda x: x.topic_name == topic_name, self._subscriptions)

    def get_publisher(self, topic_name: str) -> Publisher:
        return Util.find_one(lambda x: x.topic_name == topic_name, self._publishers)
