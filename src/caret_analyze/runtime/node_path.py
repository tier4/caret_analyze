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

from typing import List, Optional

from .callback import CallbackBase
from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription
from ..common import Summarizable, Summary
from ..infra import RecordsProvider
from ..record import RecordsFactory, RecordsInterface
from ..struct import MessageContext
from ..struct import NodePathStruct


class NodePath(PathBase, Summarizable):
    """
    A class that represents a path inside a node.

    Node path is defined as subscription-publisher pair.
    subscribe-publish policies are defined as "message context"
    """

    def __init__(
        self,
        node_path_value: NodePathStruct,
        records_provider: RecordsProvider,
        subscription: Optional[Subscription],
        publisher: Optional[Publisher],
        callbacks: Optional[List[CallbackBase]]
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        node_path_value : NodePathStruct
            static info.
        records_provider : RecordsProvider
            provider to be evaluated.
        subscription : Optional[Subscription]
            node path subscription
        publisher : Optional[Publisher]
            node path publisher
        callbacks : Optional[List[CallbackBase]]
            Callbacks in node path. Needed only if message context is CallbackChain.

        """
        super().__init__()
        self._val = node_path_value
        self._provider = records_provider
        self._pub = publisher
        self._sub = subscription
        self._callbacks = callbacks

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            Node name which contains this node path.

        """
        return self._val.node_name

    @property
    def callbacks(self) -> Optional[List[CallbackBase]]:
        """
        Get callbacks.

        Returns
        -------
        Optional[List[CallbackBase]]
            Callbacks in node path.
            None except for message context is callback chain.

        """
        if self._callbacks is None:
            return None
        return sorted(self._callbacks, key=lambda x: x.callback_name)

    @property
    def message_context(self) -> Optional[MessageContext]:
        """
        Get message context.

        Returns
        -------
        Optional[MessageContext]
            message context for this node path.

        """
        return self._val.message_context

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

    def _to_records_core(self) -> RecordsInterface:
        """
        Calculate records [override].

        Returns
        -------
        RecordsInterface
            node latency latency (subscribe-publish).

        """
        if self.message_context is None:
            return RecordsFactory.create_instance()

        records = self._provider.node_records(self._val)
        return records

    @property
    def publisher(self) -> Optional[Publisher]:
        """
        Get publisher.

        Returns
        -------
        Optional[Publisher]
            node path publisher.

        """
        return self._pub

    @property
    def publish_topic_name(self) -> Optional[str]:
        """
        Get a topic name to publish.

        Returns
        -------
        Optional[str]
            topic name to publish.

        """
        return self._val.publish_topic_name

    @property
    def subscription(self) -> Optional[Subscription]:
        """
        Get a subscription.

        Returns
        -------
        Optional[Subscription]
            subscription to subscribe to.

        """
        return self._sub

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        """
        Get a topic name to subscribe to.

        Returns
        -------
        Optional[str]
            topic name to subscribe to.

        """
        return self._val.subscribe_topic_name
