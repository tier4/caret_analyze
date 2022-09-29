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

from .callback import CallbackStruct
from ...value_objects import PublisherStructValue


class PublisherStruct():
    """Structured publisher value."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_values: Optional[Tuple[CallbackStruct, ...]],
    ) -> None:
        self._node_name = node_name
        self._topic_name = topic_name
        self._callbacks = callback_values

    def __str__(self) -> str:
        msg = ''
        msg += f'node_name: {self.node_name}, '
        msg += f'topic_name: {self.topic_name}, '
        return msg

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callbacks(self) -> Optional[Tuple[CallbackStruct, ...]]:
        return self._callbacks

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self._callbacks is None:
            return None
        return tuple(c.callback_name for c in self._callbacks)

    def to_value(self) -> PublisherStructValue:
        return PublisherStructValue(
            self.node_name, self.topic_name,
            None if self.callbacks is None else tuple(v.to_value() for v in self.callbacks))
