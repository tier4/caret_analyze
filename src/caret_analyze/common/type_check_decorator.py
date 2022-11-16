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

from typing import List

from ..exceptions import InvalidArgumentError


try:
    from pydantic import validate_arguments, ValidationError

    def decorator(func):
        validate_arguments_wrapper = \
            validate_arguments(config={'arbitrary_types_allowed': True})(func)

        def _custom_wrapper(*args, **kargs):
            try:
                return validate_arguments_wrapper(*args, **kargs)
            except ValidationError as e:
                expected_types: List[str] = []
                for error in e.errors():
                    if error['type'] == 'type_error.arbitrary_type':
                        expected_types.append(error['ctx']['expected_arbitrary_type'])
                    else:
                        expected_types.append(error['type'].replace('type_error.', ''))
                if len(expected_types) == 1:
                    expected_types_str = f"'{expected_types[0]}'"
                else:
                    expected_types_str = str(expected_types)

                if len(error['loc']) == 2:  # (argument_name, index)
                    loc_str = f'`{error["loc"][0]}`[{error["loc"][1]}]'
                else:
                    loc_str = f'`{error["loc"][0]}`'

                msg = f'Type of argument {loc_str} must be {expected_types_str}\n'
                raise InvalidArgumentError(msg) from None
        return _custom_wrapper
    type_check_decorator = decorator

except ImportError:
    def empty_decorator(func):
        def _wrapper(*args, **kargs):
            return func(*args, **kargs)
        return _wrapper
    type_check_decorator = empty_decorator
