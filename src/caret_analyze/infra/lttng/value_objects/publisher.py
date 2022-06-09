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

from ....value_objects import PublisherValue


class PublisherValueLttng(PublisherValue):
    def __init__(
        self,
        node_name: str,
        topic_name: str,
        node_id: str,
        callback_ids: Optional[Tuple[str, ...]],
        publisher_handle: int,
        tilde_publisher: Optional[int]
    ) -> None:
        super().__init__(
            node_name=node_name,
            topic_name=topic_name,
            node_id=node_id,
            callback_ids=callback_ids,
        )
        self._publisher_handle = publisher_handle
        self._tilde_publisher = tilde_publisher

    @property
    def publisher_handle(self) -> int:
        return self._publisher_handle

    @property
    def tilde_publisher(self) -> Optional[int]:
        return self._tilde_publisher
