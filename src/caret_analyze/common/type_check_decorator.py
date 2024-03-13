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

from collections.abc import Collection
from functools import wraps

from inspect import getfullargspec, Signature, signature
from re import findall
from typing import Any

from ..exceptions import UnsupportedTypeError

try:
    from pydantic import ValidationError
    from pydantic.deprecated.decorator import validate_arguments

    def _get_given_arg(
        signature: Signature,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        given_arg_loc: tuple,
        varargs: None | str
    ) -> Any:
        """
        Get an argument which validation error occurs.

        Parameters
        ----------
        signature: Signature
            Signature of target function.
        args: tuple[Any, ...]
            Arguments of target function.
        kwargs: dict[str, Any]
            Keyword arguments of target function.
        given_arg_loc: tuple
            (i) Not iterable type case
                ('<ARGUMENT_NAME>,')

            (ii) Iterable type except for dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        varargs: None | str
            The name of the variable length argument if the function has one, otherwise None.

        Returns
        -------
        str
            The argument which validation error occurs.

        """
        arg_name = given_arg_loc[0]
        given_arg: Any = None

        # Check kwargs
        for k, v in kwargs.items():
            if k == arg_name:
                given_arg = v
                break
        if given_arg is None:
            # Check args
            given_arg_idx = list(signature.parameters.keys()).index(arg_name)

            # for variable length arguments
            if arg_name == varargs:
                given_arg = args[given_arg_idx:]
            else:
                given_arg = args[given_arg_idx]

        return given_arg

    def _get_expected_types(e: ValidationError, signature: Signature) -> str:
        """
        Get expected types.

        Parameters
        ----------
        e: ValidationError
            ValidationError instance has one or more ErrorDict instances.
            Example of ErrorDict structure is as follows.
            (i) Build-in type case:
                {'type': 'type_error.<EXPECT_TYPE>', ...}

            (ii) Custom class type case:
                {'type': 'type_error.arbitrary_type',
                'ctx': {'expected_arbitrary_type': '<EXPECT_TYPE>'}, ...}
        signature: Signature
            Signature of target function.

        Returns
        -------
        str
            (i) Union case:
                ['<EXPECT_TYPE1>', '<EXPECT_TYPE2>', ...]

            (ii) otherwise:
                '<EXPECT_TYPE>'

        """
        error = e.errors()[0]
        invalid_arg_name: str = str(error['loc'][0])
        expected_type: str = str(signature.parameters[invalid_arg_name].annotation)

        if e.title == 'IterableArg':
            expected_type = str(findall(r'.*\[(.*)\]', expected_type)[0])
        if e.title == 'DictArg':
            expected_type = str(findall(r'.*\[.*, (.*)\]', expected_type)[0])

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
            (i) Not iterable type case
                ('<ARGUMENT_NAME>,')

            (ii) Iterable type except for dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        given_arg: Any
            The argument which validation error occurs.

        Returns
        -------
        str
            (i) Not iterable type case
                '<ARGUMENT_NAME>'

            (ii) Iterable type except for dict case
                '<ARGUMENT_NAME>'[INDEX]

            (ii) Dict case
                '<ARGUMENT_NAME>'[KEY]

        """
        # Iterable or dict type case
        if isinstance(given_arg, Collection) or isinstance(given_arg, dict):
            loc_str = f"'{given_arg_loc[0]}'[{given_arg_loc[1]}]"
        else:
            loc_str = f"'{given_arg_loc[0]}'"

        return loc_str

    def _get_given_arg_type(
        given_arg: Any,
        given_arg_loc: tuple,
        error_type: str,
        varargs: None | str
    ) -> str:
        """
        Get given argument type.

        Parameters
        ----------
        given_arg: Any
            The argument which validation error occurs.
        given_arg_loc: tuple
            (i) Not iterable type case
                ('<ARGUMENT_NAME>,')

            (ii) Iterable type except for dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')
        error_type: str
            (i) Dict case
                'DictArg'

            (ii) Iterable type except for dict case
                'IterableArg'

            (iii) Not iterable type case
                other
        varargs: None | str
            The name of the variable length argument if the function has one, otherwise None.

        Returns
        -------
        str
            (i) Not iterable type case
                Class name input for argument <ARGUMENT_NAME>

            (ii) Iterable type except for dict case
                Class name input for argument <ARGUMENT_NAME>[<INDEX>]

            (ii) Dict case
                Class name input for argument <ARGUMENT_NAME>[<KEY>]

        """
        if error_type == 'DictArg':
            given_arg_type_str = f"'{given_arg[given_arg_loc[1]].__class__.__name__}'"
        elif error_type == 'IterableArg':
            given_arg_type_str = f"'{given_arg[int(given_arg_loc[1])].__class__.__name__}'"
        elif varargs is None:
            given_arg_type_str = f"'{given_arg.__class__.__name__}'"
        else:
            # For functions with variable length arguments,
            given_arg_type_str = f"'{given_arg[given_arg_loc[1]].__class__.__name__}'"

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

    def decorator(func):
        validate_arguments_wrapper = \
            validate_arguments(config={'arbitrary_types_allowed': True})(func)

        @wraps(func)
        def _custom_wrapper(*args, **kwargs):
            try:
                # Checks whether the arguments of a given func have variable length arguments
                arg_spec = getfullargspec(func)
                arg_len = len(arg_spec.args)
                if arg_spec.varargs is not None:
                    args = args[:arg_len] + _parse_collection_or_unpack(args[arg_len:])
                return validate_arguments_wrapper(*args, **kwargs)
            except ValidationError as e:
                loc_tuple = e.errors()[0]['loc']
                given_arg = _get_given_arg(signature(func), args, kwargs,
                                           loc_tuple, arg_spec.varargs)
                expected_types = _get_expected_types(e, signature(func))
                error_type = e.title
                given_arg_loc_str = _get_given_arg_loc_str(loc_tuple, given_arg)
                given_arg_type = _get_given_arg_type(given_arg, loc_tuple,
                                                     error_type, arg_spec.varargs)

                msg = f'Type of argument {given_arg_loc_str} must be {expected_types}. '
                msg += f'The given argument type is {given_arg_type}.'
                raise UnsupportedTypeError(msg) from None
        return _custom_wrapper

    type_check_decorator = decorator

except ImportError:
    def empty_decorator(func):
        @wraps(func)
        def _wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return _wrapper

    type_check_decorator = empty_decorator
