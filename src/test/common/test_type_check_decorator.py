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

from caret_analyze.common.type_check_decorator import type_check_decorator
from caret_analyze.exceptions import UnsupportedTypeError

import pytest


class DummyCustom:

    def __init__(self) -> None:
        pass


class TestTypeCheckDecorator:

    def test_type_check_decorator_built_in_type(self):
        @type_check_decorator
        def bool_arg(b: bool):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            bool_arg(10)
        assert "'b' must be 'bool'. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_custom_type(self):

        @type_check_decorator
        def custom_arg(c: DummyCustom):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            custom_arg(10)
        assert "'c' must be 'DummyCustom'. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_union(self):
        @type_check_decorator
        def union_arg(u: bool | set):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            union_arg(10)
        assert "'u' must be ['bool', 'set']. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_iterable(self):
        @type_check_decorator
        def iterable_arg(i: list[bool]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            iterable_arg([True, 10])
        assert "'i'[1] must be 'bool'. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_iterable_with_union(self):
        @type_check_decorator
        def iterable_arg(i: list[bool | str]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            iterable_arg([True, 10])
        assert "'i'[1] must be ['bool', 'str']. The given argument type is 'int'" in str(e.value)

    # TODO: test_type_check_decorator_union_with_iterable
        # @type_check_decorator
        # def iterable_arg(i: list[bool] | str):
        #     pass

    def test_type_check_decorator_dict(self):
        @type_check_decorator
        def dict_arg(d: dict[str, bool]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            dict_arg({'key1': True,
                      'key2': 10})
        assert "'d'[key2] must be 'bool'. The given argument type is 'int'" in str(e.value)

    # TODO: test_type_check_decorator_dict_key
        # with pytest.raises(UnsupportedTypeError) as e:
        #     dict_arg({'key1': True,
        #               1: 10})

    def test_type_check_decorator_dict_with_union(self):
        @type_check_decorator
        def dict_arg(d: dict[str, bool | str]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            dict_arg({'key1': True,
                      'key2': 10})
        assert "'d'[key2] must be ['bool', 'str']. The given argument type is 'int'"\
            in str(e.value)

    def test_type_check_decorator_kwargs(self):
        @type_check_decorator
        def kwarg(k: bool):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            kwarg(k=10)
        assert "'k' must be 'bool'. The given argument type is 'int'" in str(e.value)
