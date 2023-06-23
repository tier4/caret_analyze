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

from .callback import SubscriptionCallbackStruct
from ...value_objects import SubscriptionStructValue


class SubscriptionStruct():
    """Subscription info."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_info: SubscriptionCallbackStruct | None,
        construction_order: int,
    ) -> None:
        self._node_name: str = node_name
        self._topic_name: str = topic_name
        self._callback_value = callback_info
        self._construction_order = construction_order

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callback_name(self) -> str | None:
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

    @property
    def callback(self) -> SubscriptionCallbackStruct | None:
        return self._callback_value

    @property
    def construction_order(self) -> int:
        return self._construction_order

    def to_value(self) -> SubscriptionStructValue:
        return SubscriptionStructValue(
            node_name=self.node_name,
            topic_name=self.topic_name,
            callback_info=None if self.callback is None else self.callback.to_value(),
            construction_order=self.construction_order
        )

    def rename_node(self, src: str, dst: str) -> None:
        if self.node_name == src:
            self._node_name = dst

        if self._callback_value is not None:
            self._callback_value.rename_node(src, dst)

    def rename_topic(self, src: str, dst: str) -> None:
        if self.topic_name == src:
            self._topic_name = dst

        if self._callback_value is not None:
            self._callback_value.rename_topic(src, dst)
