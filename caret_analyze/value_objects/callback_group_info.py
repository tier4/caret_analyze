
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

from .callback_group_type import CallbackGroupType
from .callback_info import CallbackName
from .value_object import ValueObject


class CallbackGroupInfo(ValueObject):
    """Callback group info."""

    def __init__(
        self,
        callback_group_type: CallbackGroupType,
        node_name: str,
        *callback_names: str
    ) -> None:
        """
        Construct callback info.

        Parameters
        ----------
        callback_group_type : CallbackGroupType
        node_name : str

        """
        self._callback_group_type = callback_group_type
        self._node_name = node_name
        self._callback_names: List[CallbackName] = []
        for callabck_name in callback_names:
            self._callback_names.append(CallbackName(node_name, callabck_name))

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
        """
        Get node name.

        Returns
        -------
        str
            node name

        """
        return self._node_name

    @property
    def callback_names(self) -> List[str]:
        """
        Get callback names.

        Returns
        -------
        List[str]
            callback names

        """
        return [name.name for name in self._callback_names]

    @property
    def callback_unique_names(self) -> List[str]:
        """
        Get callback unique names.

        Returns
        -------
        List[str]
            callback unique names

        """
        return [name.unique_name for name in self._callback_names]
