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

from ..value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
)
from ....value_objects import (
    BroadcastedTransformValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    TransformValue,
)


class TransformBroadcasterValueLttng(TransformBroadcasterValue):
    def __init__(
        self,
        pub: PublisherValueLttng,
        transforms: Tuple[BroadcastedTransformValue, ...],
        callback_ids: Tuple[str, ...],
        broadcaster_handler: int,
    ) -> None:
        super().__init__(pub, transforms, callback_ids)
        self.__pub = pub
        self._broadcaster_handler = broadcaster_handler

    @property
    def pid(self) -> int:
        return self.publisher.pid

    @property
    def publisher(self) -> PublisherValueLttng:
        return self.__pub

    @property
    def broadcaster_handler(self) -> int:
        return self._broadcaster_handler


class TransformBufferValueLttng(TransformBufferValue):
    def __init__(
        self,
        pid: int,
        lookup_node_name: str,
        lookup_node_id: str,
        listener_node_name: Optional[str],
        listener_node_id: Optional[str],
        lookup_transforms: Optional[Tuple[TransformValue, ...]],
        listen_transforms: Optional[Tuple[BroadcastedTransformValue, ...]],
        buffer_handler: int,
        listener_callback: SubscriptionCallbackValueLttng,
    ) -> None:
        super().__init__(
            lookup_node_name,
            lookup_node_id,
            listener_node_name,
            listener_node_id,
            lookup_transforms,
            listen_transforms)
        self._pid = pid
        self._buffer_handler = buffer_handler
        self._listener_callback = listener_callback

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def buffer_handler(self) -> int:
        return self._buffer_handler

    @property
    def listener_callback(self) -> SubscriptionCallbackValueLttng:
        return self._listener_callback

    @property
    def listener_subscription_handler(self) -> int:
        return self._listener_callback.subscription_handle
