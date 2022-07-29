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
Functions in the experimental phase.

Functions for which much of the implementation is done,
but the use cases, interfaces, etc. are not yet organized.
See docstring of each class for usage.

ResponseTime:
Convert message flow to response time and calculate histogram.

"""

from ..record import ResponseTime

__all__ = [
    'ResponseTime'
]
