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

from .callback import ServiceCallbackStruct
from ...value_objects import ServiceStructValue


class ServiceStruct():
    """Service info."""

    def __init__(
        self,
        node_name: str,
        service_name: str,
        callback_info: ServiceCallbackStruct | None,
        construction_order: int,
    ) -> None:
        self._node_name: str = node_name
        self._service_name: str = service_name
        self._callback_value = callback_info
        self._construction_order = construction_order

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def service_name(self) -> str:
        return self._service_name

    @property
    def callback_name(self) -> str | None:
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

    @property
    def callback(self) -> ServiceCallbackStruct | None:
        return self._callback_value

    @property
    def construction_order(self) -> int:
        return self._construction_order

    def to_value(self) -> ServiceStructValue:
        """
        Get service struct value.

        Returns
        -------
        ServiceStructValue
            Service struct value instance.

        """
        return ServiceStructValue(
            node_name=self.node_name,
            service_name=self.service_name,
            callback_info=None if self.callback is None else self.callback.to_value(),
            construction_order=self.construction_order
        )
