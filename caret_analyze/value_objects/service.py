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

from typing import Optional

from .value_object import ValueObject


class ServiceValue(ValueObject):
    """Service info."""

    def __init__(
        self,
        service_name: str,
        node_name: str,
        node_id: Optional[str],
        callback_id: Optional[str],
    ) -> None:
        self._node_name = node_name
        self._node_id = node_id
        self._service_name = service_name
        self._callback_id = callback_id

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def node_id(self) -> Optional[str]:
        return self._node_id

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def callback_id(self) -> Optional[str]:
        return self._callback_id
