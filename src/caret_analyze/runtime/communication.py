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

from .callback import CallbackBase
from .node import Node
from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription
from ..common import Summarizable, Summary
from ..infra import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface
from ..value_objects import CommunicationStructValue, CallbackGroupType



class Communication(PathBase, Summarizable):
    """Class that represents topic communication."""

    def __init__(
        self,
        node_publish: Node,
        node_subscription: Node,
        publisher: Publisher,
        subscription: Subscription,
        communication_value: CommunicationStructValue,
        records_provider: RecordsProvider | RuntimeDataProvider | None,
        callbacks_publish: list[CallbackBase] | None,
        callback_subscription: CallbackBase | None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_publish : Node
            publish node.
        node_subscription : Node
            subscribe node
        publisher : Publisher
            publisher
        subscription : Subscription
            subscription
        communication_value : CommunicationStructValue
            static info.
        records_provider : RecordsProvider | RuntimeDataProvider | None
            provider to be evaluated.
        callbacks_publish : list[CallbackBase] | None
            callbacks publish
        callback_subscription : CallbackBase | None
            callback subscription

        """
        super().__init__()
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._val = communication_value
        self._records_provider = records_provider
        self._callbacks_publish = callbacks_publish
        self._callback_subscription = callback_subscription
        self._is_intra_process: bool | None = None
        self._rmw_implementation: str | None = None
        if isinstance(records_provider, RuntimeDataProvider):
            self._is_intra_process = \
                records_provider.is_intra_process_communication(communication_value)
            self._rmw_implementation = \
                records_provider.get_rmw_implementation()
        self._publisher = publisher
        self._subscription = subscription

    @property
    def rmw_implementation(self) -> str | None:
        """
        Get rmw implementation.

        Returns
        -------
        str | None
            rmw implementation.

        """
        return self._rmw_implementation

    @property
    def summary(self) -> Summary:
        """
        Get summary [override].

        Returns
        -------
        Summary
            summary info.

        """
        return self._val.summary

    @property
    def is_intra_proc_comm(self) -> bool | None:
        """
        Get whether this communication is intra-process-communication.

        Returns
        -------
        bool | None
            True when intra-process-communication. otherwise False.

        """
        return self._is_intra_process

    @property
    def callback_publish(self) -> list[CallbackBase] | None:
        """
        Get publisher callback.

        Returns
        -------
        list[CallbackBase] | None
            callback which publishes this communication.

        """
        return self._callbacks_publish

    @property
    def callback_subscription(self) -> CallbackBase | None:
        """
        Get subscribe callback.

        Returns
        -------
        CallbackBase | None
            callback to which subscribe this communication.

        """
        return self._callback_subscription

    @property
    def publisher(self) -> Publisher:
        """
        Get publisher.

        Returns
        -------
        Publisher
            publisher to publish this communication.

        """
        return self._publisher

    @property
    def subscription(self) -> Subscription:
        """
        Get subscription.

        Returns
        -------
        Subscription
            subscription to subscribe to this communication.

        """
        return self._subscription

    @property
    def subscribe_node_name(self) -> str:
        """
        Get subscribe node name.

        Returns
        -------
        str
            node name which subscribes to this communication.

        """
        return self._val.subscribe_node_name

    @property
    def publish_node_name(self) -> str:
        """
        Get publish node name.

        Returns
        -------
        str
            node name which publishes this communication.

        """
        return self._val.publish_node_name

    @property
    def subscribe_node(self) -> Node:
        """
        Get subscribe node.

        Returns
        -------
        Node
            node to which subscribes this communication.

        """
        return self._node_sub

    @property
    def publish_node(self) -> Node:
        """
        Get publish node.

        Returns
        -------
        Node
            A node that publishes this communication.

        """
        return self._node_pub

    @property
    def topic_name(self) -> str:
        """
        Get a topic name.

        Returns
        -------
        str
            topic name of this communication.

        """
        return self._val.topic_name

    @property
    def column_names(self) -> list[str]:
        """
        Get column names.

        Returns
        -------
        list[str]
            Column names of this communication.

        """
        records = self.to_records()
        return records.columns

    @property
    def value(self) -> CommunicationStructValue:
        """
        Get StructValue object.

        Returns
        -------
        CommunicationStructValue
            communication value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._val

    def verify(self) -> bool:
        """
        Verify whether latency can be generated.

        Returns
        -------
        bool
            True if valid. Otherwise False.

        """
        is_valid = True
        if self._records_provider is not None:
            is_valid &= self._records_provider.verify_communication(self._val)
        return is_valid

    def _to_records_core(self) -> RecordsInterface:
        """
        Calculate records.

        Returns
        -------
        RecordsInterface
            communication latency (publish-subscribe).

        """
        assert self._records_provider is not None
        records = self._records_provider.communication_records(self._val)

        return records

    def use_take_manually(self) -> bool:
        # TODO: Refactor whether the communication uses 'take' should be determinable
        # from the Subscription alone, rather than from Communication.
        callback_groups = self.subscribe_node.callback_groups
        if callback_groups is None:
            return False

        cbg_type = None
        for cbg in callback_groups:
            for cb in cbg.callbacks:
                if cb.subscription == self.subscription:
                    cbg_type = cbg.callback_group_type
                    break
        is_automatically_add_to_executor = cbg_type == CallbackGroupType.UNDEFINED
        return is_automatically_add_to_executor

    @property
    def subscription_construction_order(self) -> int | None:
        """
        Get subscription construction order.

        Returns
        -------
        int | None
            A construction order of subscription.

        """
        if self.subscription:
            return self.subscription.construction_order
        return None

    @property
    def publisher_construction_order(self) -> int | None:
        """
        Get publisher construction order.

        Returns
        -------
        int | None
            A construction order of publisher.

        """
        if self.publisher:
            return self.publisher.construction_order
        return None
