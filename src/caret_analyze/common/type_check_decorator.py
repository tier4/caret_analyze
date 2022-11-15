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

try:
    from pydantic import validate_arguments
    type_check_decorator = validate_arguments(config={'arbitrary_types_allowed': True})
except ImportError:
    def empty_decorator(func):
        def _wrapper(*args, **kargs):
            return func(*args, **kargs)
        return _wrapper
    type_check_decorator = empty_decorator
