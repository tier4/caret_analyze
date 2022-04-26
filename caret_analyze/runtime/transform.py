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

from typing import List, Union
from multimethod import multimethod as singledispatchmethod

from .path_base import PathBase
from ..common import Summarizable
from ..infra import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface
from ..exceptions import InvalidArgumentError, ItemNotFoundError
from ..value_objects import (
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformFrameBufferStructValue,
    TransformFrameBroadcasterStructValue,
    TransformValue,
)


class TransformFrameBuffer(PathBase):

    def __init__(
        self,
        buffer: TransformFrameBufferStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        self._buff = buffer
        self._provider = provider

    @property
    def lookup_frame_id(self) -> str:
        return self._buff.lookup_frame_id

    @property
    def lookup_child_frame_id(self) -> str:
        return self._buff.lookup_child_frame_id

    @property
    def listen_frame_id(self) -> str:
        return self._buff.listen_frame_id

    @property
    def listen_child_frame_id(self) -> str:
        return self._buff.listen_child_frame_id

    # @property
    # def summary(self) -> Summary:
    #     return self._buff.summary

    def _to_records_core(self) -> RecordsInterface:
        return self._provider.tf_set_lookup_records(self._buff)


class TransformBuffer():

    def __init__(
        self,
        buffer: TransformBufferStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        self._buff = buffer
        self._provider = provider

    @property
    def lookup_transforms(self) -> List[TransformValue]:
        return list(self._buff.lookup_transforms)

    @property
    def listen_transforms(self) -> List[TransformValue]:
        return list(self._buff.listen_transforms)

    @singledispatchmethod
    def get(self, arg) -> TransformFrameBuffer:
        raise InvalidArgumentError('')

    @get.register
    def _get_tf_value(
        self,
        listen_transform: TransformValue,
        lookup_transform: TransformValue,
    ) -> TransformFrameBuffer:
        return self._get_tf_dist(
            listen_transform.frame_id,
            listen_transform.child_frame_id,
            lookup_transform.frame_id,
            lookup_transform.child_frame_id,
        )

    @get.register
    def _get_tf_dist(
        self,
        listen_frame_id: str,
        listen_child_frame_id: str,
        lookup_frame_id: str,
        lookup_child_frame_id: str,
    ) -> TransformFrameBuffer:
        frame_buff = self._buff.get_frame_buffer(
            listen_frame_id,
            listen_child_frame_id,
            lookup_frame_id,
            lookup_child_frame_id)
        return TransformFrameBuffer(frame_buff, self._provider)


class TransformFrameBroadcaster(PathBase):
    def __init__(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        self._broadcaster = broadcaster
        self._provider = provider

    @property
    def transform(self) -> TransformValue:
        return self._broadcaster.transform

    @property
    def frame_id(self) -> str:
        return self._broadcaster.frame_id

    @property
    def child_frame_id(self) -> str:
        return self._broadcaster.child_frame_id

    @property
    def topic_name(self) -> str:
        return self._broadcaster.topic_name

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.tf_broadcast_records(self._broadcaster)
        return records


class TransformBroadcaster():

    def __init__(
        self,
        broadcaster: TransformBroadcasterStructValue,
        provider: Union[RecordsProvider, RuntimeDataProvider]
    ) -> None:
        self._broadcaster = broadcaster
        self._provider = provider
        self._frame_brs = []

        for tf in broadcaster.transforms:
            frame_br_value = broadcaster.get(tf)
            frame_br = TransformFrameBroadcaster(frame_br_value, provider)
            self._frame_brs.append(frame_br)

    @property
    def transforms(self) -> List[TransformValue]:
        return list(self._broadcaster.transforms)

    def get(self, transform: TransformValue):
        for br in self._frame_brs:
            if br.transform == transform:
                return br
        raise ItemNotFoundError('TransformFrameBroadcaster')

