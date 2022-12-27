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

"""
Common package.

This package defines common, generic functions and classes used by other packages.
Common functions should be placed under the Util class.

Note: Consider using ValueObject to avoid excessive size of the Util class.
"""

from .clock_converter import ClockConverter
from .logger_config import init_logger
from .progress import Progress
from .singleton import Singleton
from .summary import Summarizable, Summary
from .type_check_decorator import type_check_decorator
from .unique_list import UniqueList
from .util import Util

__all__ = [
    'ClockConverter',
    'Progress',
    'Singleton',
    'Summarizable',
    'Summary',
    'type_check_decorator',
    'Util',
    'UniqueList',
    'init_logger'
]
