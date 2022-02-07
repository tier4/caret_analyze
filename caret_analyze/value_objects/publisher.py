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
from .value_object import ValueObject
from ..common import Summarizable, Summary


class PublisherValue(ValueObject):
    """Publisher value."""

    def __init__(
        self,
        topic_name: str,
        node_name: str,
        node_id: str,
        callback_ids: Optional[Tuple[str, ...]],
    ) -> None:
        self._node_id = node_id
        self._node_name = node_name
        self._topic_name = topic_name
        self._callback_ids = callback_ids

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def topic_name(self) -> str:
        return self._topic_name

    @property
    def callback_ids(self) -> Optional[Tuple[str, ...]]:
        return self._callback_ids

    def __str__(self) -> str:
        msg = ''
        msg += f'node_name: {self.node_name}, '
        msg += f'topic_name: {self.topic_name}, '
        return msg


class PublisherStructValue(ValueObject, Summarizable):
    """Structured publisher value."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_values: Optional[Tuple[CallbackStructValue, ...]],
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
    def callbacks(self) -> Optional[Tuple[CallbackStructValue, ...]]:
        return self._callbacks

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self._callbacks is None:
            return None
        return tuple(c.callback_name for c in self._callbacks)

    @property
    def summary(self) -> Summary:
        return Summary({
            'topic_name': self.topic_name,
            'callbacks': self.callback_names
        })
