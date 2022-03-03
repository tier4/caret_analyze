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

from typing import Optional

from .callback import SubscriptionCallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class SubscriptionValue(ValueObject):
    """Subscription info."""

    def __init__(
        self,
        topic_name: str,
        node_name: str,
        node_id: Optional[str],
        callback_id: Optional[str],
    ) -> None:
        self._node_name = node_name
        self._node_id = node_id
        self._topic_name = topic_name
        self._callback_id = callback_id

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def node_id(self) -> Optional[str]:
        return self._node_id

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callback_id(self) -> Optional[str]:
        return self._callback_id


class SubscriptionStructValue(ValueObject, Summarizable):
    """Subscription info."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_info: Optional[SubscriptionCallbackStructValue],
    ) -> None:
        self._node_name: str = node_name
        self._topic_name: str = topic_name
        self._callback_value = callback_info

    def __eq__(self, other) -> bool:
        # It is not necessary because __eq__ is defined in ValueObject type,
        # but for speed, only necessary items are compared.

        if isinstance(other, SubscriptionStructValue):
            return self.node_name == other.node_name and self.topic_name == other.topic_name
        return False

    def __hash__(self):
        return super().__hash__()

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callback_name(self) -> Optional[str]:
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

    @property
    def summary(self) -> Summary:
        return Summary({
            'topic_name': self.topic_name,
            'callback': self.callback_name
        })

    @property
    def callback(self) -> Optional[SubscriptionCallbackStructValue]:
        return self._callback_value
