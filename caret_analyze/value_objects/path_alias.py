
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

from .value_object import ValueObject


class PathAlias(ValueObject):
    """Path name alias."""

    def __init__(
        self,
        alias: str,
        *callback_unique_names: str
    ) -> None:
        self._alias = alias
        self._callback_unique_names = callback_unique_names

    @property
    def path_name(self) -> str:
        return self._alias

    @property
    def callback_unique_names(self) -> List[str]:
        """
        Get callback unique names.

        Returns
        -------
        List[str]
            callback unique names

        """
        return list(self._callback_unique_names)
