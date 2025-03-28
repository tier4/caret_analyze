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

from .callback import SubscriptionCallbackStructValue
from .value_object import ValueObject
from ..common import Summarizable, Summary


class SubscriptionValue(ValueObject):
    """Subscription info."""

    def __init__(
        self,
        topic_name: str,
        node_name: str,
        node_id: str | None,
        callback_id: str | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        topic_name : str
            Topic name.
        node_name : str
            Node name.
        node_id : str | None
            Node unique id,
            a value that can be identified when retrieved from the Architecture reader.
        callback_id : str | None
            Callback unique id,
            a value that can be identified when retrieved from the Architecture reader.
        construction_order: int
            Order of instance creation within the identical node.

        """
        self._node_name = node_name
        self._node_id = node_id
        self._topic_name = topic_name
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
    def topic_name(self) -> str:
        """
        Get a topic name.

        Returns
        -------
        str
            Topic name of this communication.

        """
        return self._topic_name

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


class SubscriptionStructValue(ValueObject, Summarizable):
    """Subscription info."""

    def __init__(
        self,
        node_name: str,
        topic_name: str,
        callback_info: SubscriptionCallbackStructValue | None,
        construction_order: int
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        topic_name : str
            Topic name.
        callback_info : SubscriptionCallbackStructValue | None
            Static info of callback.
        construction_order: int
            Order of instance creation within the identical node.

        """
        self._node_name: str = node_name
        self._topic_name: str = topic_name
        self._callback_value = callback_info
        self._construction_order = construction_order

    def __eq__(self, other) -> bool:
        """
        Check whether self object equals to given instance.

        Parameters
        ----------
        other : Any
            Comparison target.

        Returns
        -------
        bool
            Compares the values of the published properties and
            returns True only if they all match. False otherwise.

        """
        # It is not necessary because __eq__ is defined in ValueObject type,
        # but for speed, only necessary items are compared.

        if isinstance(other, SubscriptionStructValue):
            return self.node_name == other.node_name and \
                self.topic_name == other.topic_name and \
                self.construction_order == other.construction_order
        return False

    def __hash__(self):
        """
        Calculate hash value.

        Returns
        -------
        int
            A hash value calculated from all of the publicly available
            property values by recursively referencing them.

        References
        ----------
            https://www.baeldung.com/java-hashcode

        """
        return super().__hash__()

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
    def topic_name(self) -> str:
        """
        Get a topic name.

        Returns
        -------
        str
            Topic name of this subscription.

        """
        return self._topic_name

    @property
    def callback_name(self) -> str | None:
        """
        Get callback name.

        Returns
        -------
        str | None
            Callback name

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
            'topic_name': self.topic_name,
            'callback': self.callback_name
        })

    @property
    def callback(self) -> SubscriptionCallbackStructValue | None:
        """
        Get callback.

        Returns
        -------
        SubscriptionCallbackStructValue | None
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
