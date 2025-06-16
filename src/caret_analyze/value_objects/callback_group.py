
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

from .callback import CallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class CallbackGroupType(ValueObject):
    """
    Callback group type class.

    The types of callback groups are as follows:

    - MUTUALLY_EXCLUSIVE
    - REENTRANT
    - UNDEFINED

    """

    MUTUALLY_EXCLUSIVE: CallbackGroupType
    REENTRANT: CallbackGroupType
    # TODO: rename UNDEFIND
    # The CallbackGroup type marked as UNDEFINED here indicates that it is not connected to an Executor.
    UNDEFINED: CallbackGroupType

    def __init__(self, name: str) -> None:
        """
        Construct CallbackGroupType.

        Parameters
        ----------
        name : str
            type name ['mutually_exclusive', 'reentrant', 'UNDEFINED']

        Raises
        ------
        ValueError
            Argument name is not "mutually_exclusive", "reentrant", or "UNDEFINED".

        """
        if name not in ['mutually_exclusive', 'reentrant', 'UNDEFINED']:
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
CallbackGroupType.UNDEFINED = CallbackGroupType('UNDEFINED')


class CallbackGroupValue(ValueObject):
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type_name: str,
        node_name: str,
        node_id: str,
        callback_ids: tuple[str, ...],
        callback_group_id: str,
        *,
        callback_group_name: str | None = None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        callback_group_type_name : str
            callback group type name: ['mutually_exclusive' / 'reentrant' / 'UNDEFINED']
        node_name : str
            node name.
        node_id : str
            Identification of the node,
            a value that can be identified when retrieved from the Architecture reader.
        callback_ids : tuple[str, ...]
            Identification of the callback,
            a value that can be identified when retrieved from the Architecture reader.
        callback_group_id : str
            Identification of the callback group,
            a value that can be identified when retrieved from the Architecture reader.
        callback_group_name : str | None, optional
            callback group name, by default None. If None, it is generated automatically.

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
            callback group type

        """
        return self._callback_group_type

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name which includes the callback group.

        """
        return self._node_name

    @property
    def node_id(self) -> str:
        """
        Get node id.

        Returns
        -------
        str
            Node id.

        """
        return self._node_id

    @property
    def callback_group_name(self) -> str | None:
        """
        Get callback group name.

        Returns
        -------
        str | None
            Callback group name.

        """
        return self._callback_group_name

    @property
    def callback_group_id(self) -> str:
        """
        Get callback group id.

        Returns
        -------
        str
            Callback group id.

        """
        return self._callback_group_id

    @property
    def callback_ids(self) -> tuple[str, ...]:
        """
        Get callback ids.

        Returns
        -------
        tuple[str, ...]
            callback ids added to the callback group.

        """
        return self._callback_ids


class CallbackGroupStructValue(ValueObject, Summarizable):
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type: CallbackGroupType,
        node_name: str,
        callback_values: tuple[CallbackStructValue, ...],
        callback_group_name: str
    ) -> None:
        """
        Construct callback group value object.

        Parameters
        ----------
        callback_group_type : CallbackGroupType
            callback group type
        node_name : str
            node name
        callback_values: tuple[CallbackStructValue, ...]
            callback values
        callback_group_name: str
            callback group name

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
            callback group type

        """
        return self._callback_group_type

    @property
    def callback_group_type_name(self) -> str:
        """
        Get callback_group_type name.

        Returns
        -------
        str
            callback group type name

        """
        return self._callback_group_type.type_name

    @property
    def callback_group_name(self) -> str:
        """
        Get callback group name.

        Returns
        -------
        str
            Callback group name.

        """
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
    def callbacks(self) -> tuple[CallbackStructValue, ...]:
        """
        Get callbacks.

        Returns
        -------
        tuple[CallbackStructValue, ...]
            Callbacks which are added to the callback group.

        """
        return self._callback_values

    @property
    def callback_names(self) -> tuple[str, ...]:
        """
        Get callback names.

        Returns
        -------
        tuple[str, ...]
            Callback names which are added to the callback group.

        """
        return tuple(i.callback_name for i in self._callback_values)

    @property
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            Summary about value objects and runtime data objects.

        """
        return Summary({
            'name': self.callback_group_name,
            'type': self.callback_group_type_name,
            'node': self.node_name,
            'callbacks': [_.summary for _ in self.callbacks]
        })
