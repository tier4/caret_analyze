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

from collections.abc import Collection, Sequence
from functools import wraps
import inspect
from inspect import get_annotations, getfullargspec
from logging import getLogger
from re import findall
from typing import Any

from ..exceptions import UnsupportedTypeError

try:
    from pydantic import ValidationError
    from pydantic import validate_call

    def _get_given_arg(
        annotations: dict[str, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        given_arg_loc: tuple,
        varargs_name: None | str
    ) -> Any:
        """
        Get an argument which validation error occurs.

        Parameters
        ----------
        annotations: dict[str, Any]
            Dict of annotations of target function.
        args: tuple[Any, ...]
            Arguments of target function.
        kwargs: dict[str, Any]
            Keyword arguments of target function.
        given_arg_loc: tuple
            (i) Not Dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        varargs_name: None | str
            The name of the variable length argument if the function has one, otherwise None.

        Returns
        -------
        Any
            The argument which validation error occurs.

        """
        arg_name = given_arg_loc[0]
        given_arg: Any = None

        # Check kwargs
        if arg_name in kwargs:
            given_arg = kwargs.get(arg_name)

        if given_arg is None:
            # Check args
            given_arg_idx = list(annotations.keys()).index(arg_name)

            # for variable length arguments
            if arg_name == varargs_name:
                given_arg = args[given_arg_idx:]
            else:
                given_arg = args[given_arg_idx]

        return given_arg

    def _get_expected_types(given_arg_loc: tuple, annotations: dict[str, Any]) -> str:
        """
        Get expected types.

        Parameters
        ----------
        given_arg_loc: tuple
            (i) Not Dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        annotations: dict[str, Any]
            Dict of annotations of target function.

        Returns
        -------
        str
            (i) Union case:
                ['<EXPECT_TYPE1>', '<EXPECT_TYPE2>', ...]

            (ii) otherwise:
                '<EXPECT_TYPE>'

        """
        invalid_arg_name: str = given_arg_loc[0]
        expected_type: str = str(annotations[invalid_arg_name])

        # for list and dict
        if 'list[' in expected_type:
            expected_type = str(findall(r'.*\[(.*)\]', expected_type)[0])
        if 'dict[' in expected_type:
            expected_type = str(findall(r'.*\[.*, (.*)\]', expected_type)[0])

        # for union annotations
        expected_types: list[str] = expected_type.split(' | ')

        if len(expected_types) > 1:  # Union case
            expected_types_str = str(expected_types)
        else:
            expected_types_str = f"'{expected_types[0]}'"

        return expected_types_str

    def _get_given_arg_loc_str(given_arg_loc: tuple, given_arg: Any) -> str:
        """
        Get given argument location string.

        Parameters
        ----------
        given_arg_loc: tuple
            (i) Not Dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        given_arg: Any
            The argument which validation error occurs.

        Returns
        -------
        str
            (i) Iterable type except for dict case
                '<ARGUMENT_NAME>'[INDEX]

            (ii) Dict case
                '<ARGUMENT_NAME>'[KEY]

        """
        # Iterable or dict type case
        if isinstance(given_arg, str):
            loc_str = f"'{given_arg_loc[0]}'"
        elif isinstance(given_arg, Sequence) or isinstance(given_arg, dict):
            loc_str = f"'{given_arg_loc[0]}'[{given_arg_loc[1]}]"
        else:
            loc_str = f"'{given_arg_loc[0]}'"

        return loc_str

    def _get_given_arg_type(given_arg: Any, given_arg_loc: tuple) -> str:
        """
        Get given argument type.

        Parameters
        ----------
        given_arg: Any
            The argument which validation error occurs.
        given_arg_loc: tuple
            (i) Not Dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')

        Returns
        -------
        str
            (i) Not iterable type case
                Class name input for argument <ARGUMENT_NAME>

            (ii) Iterable type except for dict case
                Class name input for argument <ARGUMENT_NAME>[<INDEX>]

            (iii) Dict case
                Class name input for argument <ARGUMENT_NAME>[<KEY>]

        """
        if isinstance(given_arg, str):
            given_arg_type_str = f"'{given_arg.__class__.__name__}'"
        elif isinstance(given_arg, Sequence) or isinstance(given_arg, dict):
            given_arg_type_str = f"'{given_arg[given_arg_loc[1]].__class__.__name__}'"
        else:
            given_arg_type_str = f"'{given_arg.__class__.__name__}'"

        return given_arg_type_str

    def _parse_collection_or_unpack(
        target_arg: tuple[Collection[Any]] | tuple[Any, ...]
    ) -> tuple[Any]:
        """
        Parse target argument.

        To address both cases where the target argument is passed in collection type
        or unpacked, this function converts them to the same list format.

        Parameters
        ----------
        target_arg : tuple[Collection[Any]] | tuple[Any, ...]
            Target objects.

        Returns
        -------
        tuple[Any]

        """
        parsed_target_objects: tuple[Any]
        if isinstance(target_arg[0], Collection):
            assert len(target_arg) == 1
            parsed_target_objects = tuple(target_arg[0])
        else:  # Unpacked case
            parsed_target_objects = tuple(target_arg)  # type: ignore
        return parsed_target_objects

    def _change_loc_tuple(
        args_dict: dict[str, Any],
        loc_tuple: tuple[Any, ...],
        list_key: list
    ) -> tuple[Any, ...]:
        count = 0
        ng_pos1 = loc_tuple[0]
        ng_pos2: int | str = 0
        if len(loc_tuple) != 1:
            ng_pos2 = loc_tuple[1]
        for key_value in list_key:
            if type(args_dict[key_value]) is tuple or type(args_dict[key_value]) is list:
                for count2 in range(len(args_dict[key_value])):
                    if count2 == ng_pos2:
                        loc_tuple = (key_value, ng_pos2)
                        count += 1
                        break
                    count += 1
            else:
                if count == ng_pos1:
                    loc_tuple = (key_value, ng_pos2)
                    break
                count += 1
        return loc_tuple

    def _change_loc_tuple_varargs_name(
        args_dict: dict[str, Any],
        loc_tuple: tuple[Any, ...],
        list_key: list
    ) -> tuple[Any, ...]:
        count = 0
        ng_pos = loc_tuple[0]
        for key_value in list_key:
            if type(args_dict[key_value]) is tuple or type(args_dict[key_value]) is list:
                for count2 in args_dict[key_value]:
                    if count == ng_pos:
                        loc_tuple = (key_value, count2)
                        count += 1
                        break
                    count += 1
            else:
                if count == ng_pos:
                    loc_tuple = (key_value, count)
                    break
                count += 1
        return loc_tuple

    def decorator(func):
        validate_arguments_wrapper = \
            validate_call(config={'arbitrary_types_allowed': True})(func)

        @wraps(func)
        def _custom_wrapper(*args, **kwargs):
            try:
                # Checks whether the arguments of a given func have variable length arguments
                arg_spec = getfullargspec(func)
                varargs_name = arg_spec.varargs
                arg_len = len(arg_spec.args)

                if varargs_name is not None:
                    args = args[:arg_len] + _parse_collection_or_unpack(args[arg_len:])
                sig = inspect.signature(func)
                bound_args = sig.bind(*args, **kwargs)
                args_dict = bound_args.arguments
                return validate_arguments_wrapper(*args, **kwargs)

            except ValidationError as e:

                loc_tuple = e.errors()[0]['loc']
                annotations = get_annotations(func)

                list_key = list(args_dict.keys())
                if varargs_name is None:
                    loc_tuple = _change_loc_tuple(args_dict, loc_tuple, list_key)
                else:
                    loc_tuple = _change_loc_tuple_varargs_name(args_dict, loc_tuple, list_key)

                is_method = 'self' in args_dict.keys()
                if is_method:
                    args = args[1:]
                given_arg = _get_given_arg(annotations, args, kwargs, loc_tuple, varargs_name)
                expected_types = _get_expected_types(loc_tuple, annotations)
                thrown_in_varargs = loc_tuple[0] == varargs_name
                if thrown_in_varargs:
                    for i, arg in enumerate(given_arg):
                        if arg.__class__.__name__ not in expected_types:
                            loc_tuple = (loc_tuple[0], i)

                given_arg_loc_str = _get_given_arg_loc_str(loc_tuple, given_arg)
                given_arg_type = _get_given_arg_type(given_arg, loc_tuple)

                msg = f'Type of argument {given_arg_loc_str} must be {expected_types}. '
                msg += f'The given argument type is {given_arg_type}.'
                raise UnsupportedTypeError(msg) from None
        return _custom_wrapper

    type_check_decorator = decorator

except ImportError:
    logger = getLogger(__name__)

    logger.warning('pydantic is not installed or is not the latest version.')
    logger.warning('Please install or upgrade pydantic as CARET may not work properly.\n')

    def empty_decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return _wrapper

    type_check_decorator = empty_decorator
