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

from functools import wraps
from inspect import Signature, signature
from typing import Any, Dict, List, Tuple

from ..exceptions import UnsupportedTypeError


try:
    from pydantic import validate_arguments, ValidationError

    def _get_expected_types(e: ValidationError) -> str:
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

        Returns
        -------
        str
            (i) Union case:
                ['<EXPECT_TYPE1>', '<EXPECT_TYPE2>', ...]

            (ii) otherwise:
                '<EXPECT_TYPE>'

        """
        expected_types: List[str] = []
        for error in e.errors():
            if error['type'] == 'type_error.arbitrary_type':  # Custom class type case
                expected_types.append(error['ctx']['expected_arbitrary_type'])
            else:
                expected_types.append(error['type'].replace('type_error.', ''))

        if len(expected_types) > 1:  # Union case
            expected_types_str = str(expected_types)
        else:
            expected_types_str = f"'{expected_types[0]}'"

        return expected_types_str

    def _get_given_arg_loc(given_arg_loc: tuple) -> str:
        """
        Get given argument location.

        Parameters
        ----------
        given_arg_loc: tuple
            (i) Not iterable type case
                ('<ARGUMENT_NAME>,')

            (ii) Iterable type except for dict case
                ('<ARGUMENT_NAME>', '<INDEX>')

            (ii) Dict case
                ('<ARGUMENT_NAME>', '<KEY>')

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
        if len(given_arg_loc) == 2:  # Iterable type case
            loc_str = f"'{given_arg_loc[0]}'[{given_arg_loc[1]}]"
        else:
            loc_str = f"'{given_arg_loc[0]}'"

        return loc_str

    def _get_given_arg_type(
        signature: Signature,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        given_arg_loc: tuple
    ) -> str:
        """
        Get given argument type.

        Parameters
        ----------
        signature: Signature
            Signature of target function.
        args: Tuple[Any, ...]
            Arguments of target function.
        kwargs: Dict[str, Any]
            Keyword arguments of target function.
        given_arg_loc: tuple
            (i) Not iterable type case
                ('<ARGUMENT_NAME>,')

            (ii) Iterable type except for dict case
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

            (ii) Dict case
                Class name input for argument <ARGUMENT_NAME>[<KEY>]

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
            given_arg = args[given_arg_idx]

        if len(given_arg_loc) == 2:  # Iterable type case
            if isinstance(given_arg, dict):
                given_arg_type_str = f"'{given_arg[given_arg_loc[1]].__class__.__name__}'"
            else:
                given_arg_type_str = f"'{given_arg[int(given_arg_loc[1])].__class__.__name__}'"
        else:
            given_arg_type_str = f"'{given_arg.__class__.__name__}'"

        return given_arg_type_str

    def decorator(func):
        validate_arguments_wrapper = \
            validate_arguments(config={'arbitrary_types_allowed': True})(func)

        @wraps(func)
        def _custom_wrapper(*args, **kwargs):
            try:
                return validate_arguments_wrapper(*args, **kwargs)
            except ValidationError as e:
                expected_types = _get_expected_types(e)
                loc_tuple = e.errors()[0]['loc']
                given_arg_loc = _get_given_arg_loc(loc_tuple)
                given_arg_type = _get_given_arg_type(signature(func), args, kwargs, loc_tuple)

                msg = f'Type of argument {given_arg_loc} must be {expected_types}. '
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
