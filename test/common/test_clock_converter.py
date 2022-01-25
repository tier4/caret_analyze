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

from caret_analyze.common import ClockConverter
from caret_analyze.exceptions import InvalidArgumentError

import pytest


class TestClockConverter:

    def test_convert(self):
        converter = ClockConverter(0, 1)
        assert converter.convert(0) == 1
        assert converter.convert(1) == 1

        converter = ClockConverter(1, 0)
        assert converter.convert(0) == 0
        assert converter.convert(1) == 1

        converter = ClockConverter(1, 1)
        assert converter.convert(0) == 1
        assert converter.convert(1) == 2

    def test_create_from_series(self):
        converter = ClockConverter.create_from_series([0, 1], [1, 1])
        e = 1.0e-10
        assert abs(converter._a - 0) < e
        assert abs(converter._b - 1) < e

        converter = ClockConverter.create_from_series([0, 1], [0, 1])
        assert abs(converter._a - 1) < e
        assert abs(converter._b - 0) < e

        converter = ClockConverter.create_from_series([0, 1], [1, 2])
        assert abs(converter._a - 1) < e
        assert abs(converter._b - 1) < e

    def test_create_from_series_assertion(self):
        with pytest.raises(InvalidArgumentError):
            ClockConverter.create_from_series([], [])

        with pytest.raises(InvalidArgumentError):
            ClockConverter.create_from_series([0], [0])

        with pytest.raises(InvalidArgumentError):
            ClockConverter.create_from_series([0], [])
