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

from typing import List, Optional, Union

from .callback import CallbackBase
from .node import Node
from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription
from ..common import Summarizable, Summary
from ..infra import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface
from ..value_objects import CommunicationStructValue


class Communication(PathBase, Summarizable):
    """Class that represents topic communication."""

    def __init__(
        self,
        node_publish: Node,
        node_subscription: Node,
        publisher: Publisher,
        subscription: Subscription,
        communication_value: CommunicationStructValue,
        records_provider: Union[RecordsProvider, RuntimeDataProvider, None],
        callbacks_publish: Optional[List[CallbackBase]],
        callback_subscription: Optional[CallbackBase],
    ) -> None:
        """
        Constructor.

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
        records_provider : Union[RecordsProvider, RuntimeDataProvider, None]
            provider to be evaluated.
        callbacks_publish : Optional[List[CallbackBase]]
        callback_subscription : Optional[CallbackBase]

        """
        super().__init__()
        self._node_pub = node_publish
        self._node_sub = node_subscription
        self._val = communication_value
        self._records_provider = records_provider
        self._callbacks_publish = callbacks_publish
        self._callback_subscription = callback_subscription
        self._is_intra_process: Optional[bool] = None
        self._rmw_implementation: Optional[str] = None
        if isinstance(records_provider, RuntimeDataProvider):
            self._is_intra_process = \
                records_provider.is_intra_process_communication(communication_value)
            self._rmw_implementation = \
                records_provider.get_rmw_implementation()
        self._publisher = publisher
        self._subscription = subscription

    @property
    def rmw_implementation(self) -> Optional[str]:
        """
        Get rmw implementation.

        Returns
        -------
        Optional[str]
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
    def is_intra_proc_comm(self) -> Optional[bool]:
        """
        Get whether this communication is intra-process-communication.

        Returns
        -------
        Optional[bool]
            True when intra-process-communication. otherwise False.

        """
        return self._is_intra_process

    @property
    def callback_publish(self) -> Optional[List[CallbackBase]]:
        """
        Get publisher callback.

        Returns
        -------
        Optional[List[CallbackBase]]
            callback which publishes this communication.

        """
        return self._callbacks_publish

    @property
    def callback_subscription(self) -> Optional[CallbackBase]:
        """
        Get subscribe callback.

        Returns
        -------
        Optional[CallbackBase]
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
    def column_names(self) -> List[str]:
        records = self.to_records()
        return records.columns

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
