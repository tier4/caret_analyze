
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

from caret_analyze.value_objects.callback_group import CallbackGroupType

from .callback import CallbackStruct
from ...common import Summarizable, Summary

from ...value_objects import CallbackGroupType, CallbackGroupStructValue

class CallbackGroupStruct(Summarizable):
    """Callback group value object."""

    def __init__(
        self,
        callback_group_type: CallbackGroupType,
        node_name: str,
        callback_values: Tuple[CallbackStruct, ...],
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
    def callbacks(self) -> Tuple[CallbackStruct, ...]:
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

    def to_value(self) -> CallbackGroupStructValue:
        return CallbackGroupStructValue(self.callback_group_type, self.node_name, tuple(v.to_value() for v in self.callbacks), self.callback_group_name)
