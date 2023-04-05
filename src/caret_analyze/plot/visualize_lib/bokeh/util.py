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

import numpy as np


class RectValues:
    def __init__(
        self,
        callback_start: float,
        callback_end: float,
        y_min: float,
        y_max: float
    ) -> None:
        self._y = [y_min, y_max]
        self._x = [callback_start, callback_end]

    @property
    def x(self) -> float:
        return float(np.mean(self._x))

    @property
    def y(self) -> float:
        return float(np.mean(self._y))

    @property
    def width(self) -> float:
        return abs(self._x[0] - self._x[1])

    @property
    def height(self) -> float:
        return abs(self._y[0] - self._y[1])
