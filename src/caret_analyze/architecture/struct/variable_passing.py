
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


from .callback import CallbackStruct
from ...value_objects import VariablePassingStructValue


class VariablePassingStruct():
    """variable passing info."""

    def __init__(
        self,
        node_name: str,
        callback_write: CallbackStruct,
        callback_read: CallbackStruct,
    ) -> None:
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
            node name

        """
        return self._node_name

    @property
    def callback_name_write(self) -> str:
        """
        Get write-side callback name.

        Returns
        -------
        [str]
            write-side callback name.

        """
        return self._cb_write.callback_name

    @property
    def callback_name_read(self):
        """
        Get read-side callback name.

        Returns
        -------
        [str]
            read-side callback name.

        """
        return self._cb_read.callback_name

    @property
    def callback_write(self) -> CallbackStruct:
        return self._cb_write

    @property
    def callback_read(self) -> CallbackStruct:
        return self._cb_read

    def to_value(self) -> VariablePassingStructValue:
        return VariablePassingStructValue(self.node_name,
                                          self.callback_write.to_value(),
                                          self.callback_read.to_value())

    def rename_node(self, src: str, dst: str) -> None:
        if self.node_name == src:
            self._node_name = dst

        self._cb_read.rename_node(src, dst)
        self._cb_write.rename_node(src, dst)

    def rename_topic(self, src: str, dst: str) -> None:
        self._cb_read.rename_topic(src, dst)
        self._cb_write.rename_topic(src, dst)
