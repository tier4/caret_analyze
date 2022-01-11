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

from .value_object import ValueObject
from ..common import Summarizable, Summary


class Qos(ValueObject, Summarizable):
    """qos info."""

    def __init__(self, depth: int) -> None:
        self._depth = depth

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def summary(self) -> Summary:
        d: Summary = Summary()
        d['depth'] = self.depth

        return d
