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

from .value_object import ValueObject


class CallbackGroupType(ValueObject):
    """callback group type class."""

    Exclusive: CallbackGroupType
    Reentrant: CallbackGroupType

    def __init__(self, name: str) -> None:
        """
        Construct CallbackGroupType.

        Parameters
        ----------
        name : str
            type name ['exclusive', 'reentrant']

        """
        if name not in ['exclusive', 'reentrant']:
            raise ValueError(f'Unsupported callback group type: {name}')

        self._name = name

    @property
    def name(self) -> str:
        """
        Return callback group type name.

        Returns
        -------
        str
            type name.

        """
        return self._name


CallbackGroupType.Exclusive = CallbackGroupType('exclusive')
CallbackGroupType.Reentrant = CallbackGroupType('reentrant')
