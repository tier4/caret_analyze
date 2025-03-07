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

from .callback import ServiceCallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class ServiceValue(ValueObject):
    """Service info."""

    def __init__(
        self,
        service_name: str,
        node_name: str,
        node_id: str | None,
        callback_id: str | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        service_name : str
            Service name.
        node_name : str
            Node name.
        node_id : str | None
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        callback_id : str | None
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        construction_order : int
            Order of instance creation within the identical node.

        """
        self._node_name = node_name
        self._node_id = node_id
        self._service_name = service_name
        self._callback_id = callback_id
        self._construction_order = construction_order

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
    def node_id(self) -> str | None:
        """
        Get node id.

        Returns
        -------
        str | None
            Node id.

        """
        return self._node_id

    @property
    def service_name(self) -> str:
        """
        Get service name.

        Returns
        -------
        str
            Service name.

        """
        return self._service_name

    @property
    def callback_id(self) -> str | None:
        """
        Get callback id.

        Returns
        -------
        str | None
            Callback unique id.

        """
        return self._callback_id

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order


class ServiceStructValue(ValueObject, Summarizable):
    """Service info."""

    def __init__(
        self,
        node_name: str,
        service_name: str,
        callback_info: ServiceCallbackStructValue | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        service_name : str
            Service name.
        callback_info : ServiceCallbackStructValue | None
            Static info of callback.
        construction_order : int
            Order of instance creation within the identical node.

        """
        self._node_name: str = node_name
        self._service_name: str = service_name
        self._callback_value = callback_info
        self._construction_order = construction_order

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
    def service_name(self) -> str:
        """
        Get service name.

        Returns
        -------
        str
            Service name.

        """
        return self._service_name

    @property
    def callback_name(self) -> str | None:
        """
        Get callback name.

        Returns
        -------
        str | None
            Callback name.

        """
        if self._callback_value is None:
            return None

        return self._callback_value.callback_name

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
            'service_name': self.service_name,
            'callback': self.callback_name
        })

    @property
    def callback(self) -> ServiceCallbackStructValue | None:
        """
        Get callback.

        Returns
        -------
        ServiceCallbackStructValue | None
            Callback.

        """
        return self._callback_value

    @property
    def construction_order(self) -> int:
        """
        Get construction order.

        Returns
        -------
        int
            Construction order.

        """
        return self._construction_order
