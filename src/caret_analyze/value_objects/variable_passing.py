
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


from .callback import CallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class VariablePassingValue(ValueObject):
    """variable passing info."""

    def __init__(
        self,
        node_name: str,
        callback_id_write: str,
        callback_id_read: str,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        callback_id_write : str
            Unique id of write callback,
            a value that can be identified when retrieved from the Architecture reader.
        callback_id_read : str
            Unique id of read callback,
            a value that can be identified when retrieved from the Architecture reader.

        """
        self._node_name = node_name
        self._callback_id_write = callback_id_write
        self._callback_id_read = callback_id_read

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node_name

    @property
    def callback_id_write(self) -> str:
        """
        Get write-side callback id.

        Returns
        -------
        str
            Write-side callback id.

        """
        return self._callback_id_write

    @property
    def callback_id_read(self) -> str:
        """
        Get read-side callback id.

        Returns
        -------
        str
            Read-side callback id.

        """
        return self._callback_id_read


class VariablePassingStructValue(ValueObject, Summarizable):
    """variable passing info."""

    def __init__(
        self,
        node_name: str,
        callback_write: CallbackStructValue,
        callback_read: CallbackStructValue,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        callback_write : CallbackStructValue
            Write callback.
        callback_read : CallbackStructValue
            Read callback.

        """
        self._node_name = node_name
        self._cb_write = callback_write
        self._cb_read = callback_read

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name.

        """
        return self._node_name

    @property
    def callback_name_write(self) -> str:
        """
        Get write-side callback name.

        Returns
        -------
        str
            Write-side callback name.

        """
        return self._cb_write.callback_name

    @property
    def callback_name_read(self):
        """
        Get read-side callback name.

        Returns
        -------
        str
            Read-side callback name.

        """
        return self._cb_read.callback_name

    @property
    def callback_write(self) -> CallbackStructValue:
        """
        Get write-side callback.

        Returns
        -------
        str
            Write-side callback.

        """
        return self._cb_write

    @property
    def callback_read(self) -> CallbackStructValue:
        """
        Get read-side callback.

        Returns
        -------
        str
            Read-side callback.

        """
        return self._cb_read

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
            'node': self.node_name,
            'write': self.callback_name_write,
            'read': self.callback_name_read,
        })
