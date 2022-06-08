
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

from typing import Optional, Tuple

from .callback import CallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class CallbackGroupType(ValueObject):
    """callback group type class."""

    MUTUALLY_EXCLUSIVE: CallbackGroupType
    REENTRANT: CallbackGroupType

    def __init__(self, name: str) -> None:
        """
        Construct CallbackGroupType.

        Parameters
        ----------
        name : str
            type name ['mutually_exclusive', 'reentrant']

        """
        if name not in ['mutually_exclusive', 'reentrant']:
            raise ValueError(f'Unsupported callback group type: {name}')

        self._name = name

    def __str__(self) -> str:
        return self.type_name

    @property
    def type_name(self) -> str:
        """
        Return callback group type name.

        Returns
        -------
        str
            type name.

        """
        return self._name


CallbackGroupType.MUTUALLY_EXCLUSIVE = CallbackGroupType('mutually_exclusive')
CallbackGroupType.REENTRANT = CallbackGroupType('reentrant')


class CallbackGroupValue(ValueObject):
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type_name: str,
        node_name: str,
        node_id: str,
        callback_ids: Tuple[str, ...],
        callback_group_id: str,
        *,
        callback_group_name: Optional[str] = None
    ) -> None:
        """
        Construct callback value object.

        Parameters
        ----------
        callback_group_type_name : str
        node_names : Tuple[str]

        """
        self._callback_group_type = CallbackGroupType(callback_group_type_name)
        self._callback_group_id = callback_group_id
        self._node_name = node_name
        self._node_id = node_id
        self._callback_ids = callback_ids
        self._callback_group_name = callback_group_name

    @property
    def callback_group_type(self) -> CallbackGroupType:
        """
        Get callback_group_type.

        Returns
        -------
        CallbackGroupType

        """
        return self._callback_group_type

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def callback_group_name(self) -> Optional[str]:
        return self._callback_group_name

    @property
    def callback_group_id(self) -> str:
        return self._callback_group_id

    @property
    def callback_ids(self) -> Tuple[str, ...]:
        """
        Get callback ids.

        Returns
        -------
        Tuple[str, ...]
            callback ids

        """
        return self._callback_ids


class CallbackGroupStructValue(ValueObject, Summarizable):
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type: CallbackGroupType,
        node_name: str,
        callback_values: Tuple[CallbackStructValue, ...],
        callback_group_name: str
    ) -> None:
        """
        Construct callback group value object.

        Parameters
        ----------
        callback_group_type_name : str
        node_names : List[str]

        """
        self._callback_group_type = callback_group_type
        self._node_name = node_name
        self._callback_values = callback_values
        self._callback_group_name = callback_group_name

    @property
    def callback_group_type(self) -> CallbackGroupType:
        """
        Get callback_group_type.

        Returns
        -------
        CallbackGroupType

        """
        return self._callback_group_type

    @property
    def callback_group_type_name(self) -> str:
        """
        Get callback_group_type name.

        Returns
        -------
        CallbackGroupType name

        """
        return self._callback_group_type.type_name

    @property
    def callback_group_name(self) -> str:
        return self._callback_group_name

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name

        """
        return self._node_name

    @property
    def callbacks(self) -> Tuple[CallbackStructValue, ...]:
        return self._callback_values

    @property
    def callback_names(self) -> Tuple[str, ...]:
        return tuple(i.callback_name for i in self._callback_values)

    @property
    def summary(self) -> Summary:
        return Summary({
            'name': self.callback_group_name,
            'type': self.callback_group_type_name,
            'node': self.node_name,
            'callbacks': [_.summary for _ in self.callbacks]
        })
