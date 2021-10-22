
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


from .callback_info import CallbackName
from .value_object import ValueObject


class VariablePassingInfo(ValueObject):
    """variable passing info."""

    def __init__(
        self,
        node_name: str,
        callback_name_write: str,
        callback_name_read: str,
    ) -> None:
        """
        Construct variable passing info.

        Parameters
        ----------
        node_name : str
        callback_name_write : str
            writer-side callback name
        callback_name_read : str
            reader-side callback name

        """
        self._node_name = node_name
        self._write = CallbackName(node_name, callback_name_write)
        self._read = CallbackName(node_name, callback_name_read)

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
    def write_callback_unique_name(self) -> str:
        """
        Get write-side callback unique name.

        Returns
        -------
        [str]
            write-side callback unique name.

        """
        return self._write.unique_name

    @property
    def write_callback_name(self) -> str:
        """
        Get write-side callback name.

        Returns
        -------
        [str]
            write-side callback name.

        """
        return self._write.name

    @property
    def read_callback_unique_name(self):
        """
        Get read-side callback unique name.

        Returns
        -------
        [str]
            read-side callbackunique name.

        """
        return self._read.unique_name

    @property
    def read_callback_name(self):
        """
        Get read-side callback name.

        Returns
        -------
        [str]
            read-side callback name.

        """
        return self._read.name
