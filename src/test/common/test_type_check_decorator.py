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

from caret_analyze.common.type_check_decorator import type_check_decorator
from caret_analyze.exceptions import UnsupportedTypeError

import pytest


class DummyCustom1:

    def __init__(self) -> None:
        pass


class DummyCustom2:

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
        def custom_arg(c: DummyCustom1):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            custom_arg(10)
        assert "'c' must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            custom_arg(c=10)
        assert "'c' must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

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

    def test_type_check_decorator_not_string(self):
        @type_check_decorator
        def not_str_arg(a: bool, b: int):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            not_str_arg(False, 'string')
        assert "'b' must be 'int'. The given argument type is 'str'" in str(e.value)

    def test_type_check_decorator_default_param(self):
        @type_check_decorator
        def default_param(a: int, b: str = 'B'):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            default_param(1, 2)
        assert "'b' must be 'str'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            default_param('A')
        assert "'a' must be 'int'. The given argument type is 'str'" in str(e.value)

    def test_type_check_decorator_custom_iterable(self):
        @type_check_decorator
        def iterable_arg(i: list[DummyCustom1]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            iterable_arg([10, dummy1])
        assert "'i'[0] must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            iterable_arg([dummy1, 10])
        assert "'i'[1] must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            iterable_arg(i=[10, dummy1])
        assert "'i'[0] must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_custom_iterable_mix_arg(self):
        @type_check_decorator
        def iterable_arg(a: int, i: list[DummyCustom1]):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            iterable_arg(1, [10, dummy1])
        assert "'i'[0] must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            dummy2 = DummyCustom1()
            iterable_arg(1, [dummy1, dummy2, 30])
        assert "'i'[2] must be 'DummyCustom1'. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            dummy1 = DummyCustom1()
            dummy2 = DummyCustom1()
            iterable_arg('A', [dummy1, dummy2])
        assert "'a' must be 'int'. The given argument type is 'str'" in str(e.value)

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

    def test_type_check_decorator_mix(self):
        @type_check_decorator
        def mix_arg(a: int,  b: bool, c: str = 'default'):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg(1, 2)
        assert "'b' must be 'bool'. The given argument type is 'int'"\
               in str(e.value)
        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg('dummy', False)
        assert "'a' must be 'int'. The given argument type is 'str'"\
               in str(e.value)
        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg(b=True, a='dummy', c='test')
        assert "'a' must be 'int'. The given argument type is 'str'"\
               in str(e.value)
        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg(b=2, a=1, c='test')
        assert "'b' must be 'bool'. The given argument type is 'int'"\
               in str(e.value)

    def test_type_check_decorator_variable_length_mix(self):
        @type_check_decorator
        def mix_arg(a: int, *b: bool, c: str = 'default'):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg('dummy', False, True)
        assert "'a' must be 'int'. The given argument type is 'str'"\
               in str(e.value)
        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg(1, 10, True)
        assert "'b'[0] must be 'bool'. The given argument type is 'int'"\
               in str(e.value)
        with pytest.raises(UnsupportedTypeError) as e:
            mix_arg(1, True, 20, c='test')
        assert "'b'[1] must be 'bool'. The given argument type is 'int'"\
               in str(e.value)

    def test_type_check_decorator_variable_length_union(self):
        @type_check_decorator
        def union_arg(*u: bool | set):
            pass

        with pytest.raises(UnsupportedTypeError) as e:
            union_arg(10, True)
        assert "'u'[0] must be ['bool', 'set']. The given argument type is 'int'" in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            union_arg(True, 20)
        assert "'u'[1] must be ['bool', 'set']. The given argument type is 'int'" in str(e.value)

    def test_type_check_decorator_variable_length_arg(self):
        @type_check_decorator
        def var_len_args(*i: DummyCustom1 | DummyCustom2):
            pass

        dummy_1 = DummyCustom1()
        dummy_2 = DummyCustom2()
        with pytest.raises(UnsupportedTypeError) as e:
            var_len_args(1, dummy_1, dummy_2)
        assert "'i'[0] must be ['DummyCustom1', 'DummyCustom2']. The given argument type is 'int'"\
               in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            var_len_args([dummy_1, 1, dummy_2])
        assert "'i'[1] must be ['DummyCustom1', 'DummyCustom2']. The given argument type is 'int'"\
               in str(e.value)

    def test_type_check_decorator_mix_arg(self):
        @type_check_decorator
        def mix_args(a: DummyCustom1, *i: DummyCustom2):
            pass

        dummy_1 = DummyCustom1()
        dummy_2 = DummyCustom2()
        with pytest.raises(UnsupportedTypeError) as e:
            mix_args(dummy_1, [1, dummy_2])
        assert "'i'[0] must be 'DummyCustom2'. The given argument type is 'int'"\
               in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            mix_args(dummy_1, [dummy_2, 1])
        assert "'i'[1] must be 'DummyCustom2'. The given argument type is 'int'"\
               in str(e.value)

    def test_method_case(self):
        class ValidateTestClass:

            @type_check_decorator
            def bool_arg(self, a: bool, b: bool):
                pass

        v = ValidateTestClass()

        # Ensure that it can be executed at the appropriate input
        v.bool_arg(a=True, b=False)

        with pytest.raises(UnsupportedTypeError) as e:
            v.bool_arg(True, 'test')
        assert "'b' must be 'bool'. The given argument type is 'str'"\
               in str(e.value)

    def test_method_case_variable_length_arg(self):
        class ValidateTestClass:

            @type_check_decorator
            def var_len_args(self, *i: DummyCustom1):
                pass

        dummy_1 = DummyCustom1()
        v = ValidateTestClass()

        with pytest.raises(UnsupportedTypeError) as e:
            v.var_len_args(4, dummy_1)
        assert "'i'[0] must be 'DummyCustom1'. The given argument type is 'int'"\
               in str(e.value)

        with pytest.raises(UnsupportedTypeError) as e:
            v.var_len_args(dummy_1, 4)
        assert "'i'[1] must be 'DummyCustom1'. The given argument type is 'int'"\
               in str(e.value)
