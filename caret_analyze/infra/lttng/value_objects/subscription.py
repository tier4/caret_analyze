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

from typing import Optional, Tuple

from ....value_objects import SubscriptionValue, IntraProcessBufferValue


class IntraProcessBufferValueLttng(IntraProcessBufferValue):

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        capacity: int,
        buffer: int,
    ) -> None:
        super().__init__(node_name, topic_name, capacity)
        self._buffer = buffer

    @property
    def buffer(self) -> int:
        return self._buffer


class SubscriptionValueLttng(SubscriptionValue):

    def __init__(
        self,
        topic_name: str,
        node_name: str,
        node_id: Optional[str],
        callback_id: Optional[str],
        subscription_handle: int,
        subscription_id: str,
        tilde_subscription: str,
    ) -> None:
        super().__init__(topic_name, node_name, node_id, callback_id)
        self._subscription_handle = subscription_handle
        self._subscription_id = subscription_id
        self._tilde_subscription = tilde_subscription

    @property
    def subscription_handle(self) -> int:
        return self._subscription_handle

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    @property
    def tilde_subscription(self) -> str:
        return self._tilde_subscription
