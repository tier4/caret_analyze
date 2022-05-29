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

from typing import Optional

from ....value_objects import NodeValue


class NodeValueLttng(NodeValue):
    def __init__(
        self,
        pid: int,
        node_name: str,
        node_handle: int,
        node_id: str,
        lib_caret_version: Optional[str],
    ):
        super().__init__(node_name, node_id)
        self._pid = pid
        self._node_id = node_id
        self._node_handle = node_handle
        self._lib_caret_version = lib_caret_version

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def node_handle(self) -> int:
        return self._node_handle

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def lib_caret_version(self) -> Optional[str]:
        return self._lib_caret_version
