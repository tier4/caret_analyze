# Copyright 2021 TIER IV, Inc.
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

from collections.abc import Sequence

import numpy as np

from ..exceptions import InvalidArgumentError


class ClockConverter():
    """
    Class for time conversion. Converts a time given in linear form (y=ax+b) to another time.

    TODO(hsgwa): Migrate into record.
    """

    def __init__(
        self,
        a: float,
        b: float
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        a : float
            Slope.
        b : float
            Offset.

        """
        self._a = a
        self._b = b

    @staticmethod
    def create_from_series(
        times_from: Sequence[float],
        times_to: Sequence[float]
    ) -> ClockConverter:
        """
        Construct an instance from time series data.

        Parameters
        ----------
        times_from : Sequence[float]
            Time before conversion.
        times_to : Sequence[float]
            Time after conversion.

        Returns
        -------
        ClockConverter
            converter instance.

        Raises
        ------
        InvalidArgumentError
            Occurs when calculation failed by the least-squares method.

        """
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
        """
        Convert input time.

        Parameters
        ----------
        time : float
            Time to convert.

        Returns
        -------
        float
            Time after conversion.
            Conversion are done with y=ax+b.

        """
        converted = self._a * time + self._b
        return converted
