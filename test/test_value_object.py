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

from caret_analyze.value_objects.value_object import ValueObject

import pytest


class SampleClassA(ValueObject):

    def __init__(self, i: int, s: str, p: int) -> None:
        self._i = i
        self._s = s
        self._p = p

    @property
    def i(self):
        return self._i

    @property
    def s(self):
        return self._s

    def f(self):
        return None


class SampleClassB(ValueObject):

    def __init__(self, i: int, s: str, p: int) -> None:
        self._i = i
        self._s = s
        self._p = p

    @property
    def i(self):
        return self._i

    @property
    def s(self):
        return self._s

    def f(self):
        return None


class SampleClassC(ValueObject):

    def __init__(self, i: int, s: str, p: int) -> None:
        self._p = p
        self._v = SampleClassA(i, s, p)

    @property
    def p(self) -> int:
        return self._p

    @property
    def v(self) -> SampleClassA:
        return self._v


class TestValueObject:

    def test_immutable(self):
        c = SampleClassA(1, '1', 1)
        with pytest.raises(Exception):
            c.i = 1

    def test_eq(self):
        assert SampleClassA(1, '1', 1).i == 1
        assert SampleClassA(1, '1', 1) == SampleClassA(1, '1', 1)
        assert SampleClassA(1, '1', 2) == SampleClassA(1, '1', 2)
        assert SampleClassA(1, '1', 1) != SampleClassA(1, '2', 1)
        assert SampleClassA(1, '1', 1) != SampleClassA(2, '1', 1)
        assert SampleClassA(1, '1', 1) != SampleClassB(1, '1', 1)

    def test_hash(self):
        assert hash(SampleClassA(1, '1', 1)) == hash(SampleClassA(1, '1', 1))
        assert hash(SampleClassA(1, '1', 1)) == hash(SampleClassA(1, '1', 2))
        assert hash(SampleClassA(1, '1', 1)) != hash(SampleClassA(1, '2', 1))
        assert hash(SampleClassA(1, '1', 1)) != hash(SampleClassA(2, '1', 1))
        assert hash(SampleClassA(1, '1', 1)) != hash(SampleClassB(1, '1', 1))

    def test_str(self):
        from yaml import dump
        a = SampleClassA(1, '2', 3)
        assert str(a) == dump({'i': 1, 's': '2'})

        a = SampleClassC(1, '2', 3)
        assert str(a) == dump({'p': 3, 'v': {'i': 1, 's': '2'}})
