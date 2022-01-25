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

from typing import Sequence

from ..exceptions import InvalidArgumentError


class ClockConverter():

    def __init__(
        self,
        a: float,
        b: float
    ) -> None:
        self._a = a
        self._b = b

    @staticmethod
    def create_from_series(
        times_from: Sequence[float],
        times_to: Sequence[float]
    ) -> ClockConverter:
        import numpy as np
        if len(times_from) < 2:
            raise InvalidArgumentError('Failed to construct ClockConverter. len(times_from) < 2')

        if len(times_to) < 2:
            raise InvalidArgumentError('Failed to construct ClockConverter. len(times_from) < 2')

        if len(times_from) != len(times_to):
            raise InvalidArgumentError(
                'Failed to construct ClockConverter. len(times_from) != len(times_to)')

        v = np.polyfit(times_from, times_to, deg=1)

        return ClockConverter(v[0], v[1])

    def convert(
        self,
        time: float
    ) -> float:
        converted = self._a * time + self._b
        return converted
