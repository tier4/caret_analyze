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

from typing import Dict, Tuple

from ...exceptions import InvalidArgumentError


class FrameMapper:

    def __init__(self) -> None:
        self._to_compact: Dict[Tuple[int, str], int] = {}
        self._to_frame_id: Dict[Tuple[int, int], str] = {}

    def assign(self, obj: int, frame_id: str, frame_id_compact: int) -> None:
        self._to_compact[(obj, frame_id)] = frame_id_compact
        self._to_frame_id[(obj, frame_id_compact)] = frame_id

    def to_compact(self, obj: int, frame_id: str) -> int:
        key = (obj, frame_id)
        if key in self._to_compact:
            return self._to_compact[key]
        raise InvalidArgumentError(f'unknown frame_id: {frame_id}')

    def to_frame_id(self, obj: int, frame_id_compact: int) -> str:
        return self._to_frame_id[(obj, frame_id_compact)]
