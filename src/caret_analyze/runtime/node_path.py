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

from logging import getLogger
from typing import List, Optional

from .callback import CallbackBase
from .path_base import PathBase
from .publisher import Publisher
from .subscription import Subscription
from ..common import Summarizable, Summary
from ..exceptions import Error
from ..infra import RecordsProvider
from ..record import RecordsFactory, RecordsInterface
from ..value_objects import MessageContext, NodePathStructValue

logger = getLogger(__name__)


class NodePath(PathBase, Summarizable):
    """
    A class that represents a path inside a node.

    Node path is defined as subscription-publisher pair.
    subscribe-publish policies are defined as "message context"
    """

    def __init__(
        self,
        node_path_value: NodePathStructValue,
        records_provider: RecordsProvider,
        subscription: Optional[Subscription],
        publisher: Optional[Publisher],
        callbacks: Optional[List[CallbackBase]]
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_path_value : NodePathStructValue
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
        self._path_beginning_records_cache: Optional[RecordsInterface] = None
        self._path_end_records_cache: Optional[RecordsInterface] = None

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
    def value(self) -> NodePathStructValue:
        """
        Get StructValue object.

        Returns
        -------
        NodePathStructValue
            node path value.

        Notes
        -----
        This property is for CARET debugging purposes.

        """
        return self._val

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

    def clear_cache(self) -> None:
        self._path_beginning_records_cache = None
        self._path_end_records_cache = None
        return super().clear_cache()

    def to_path_beginning_records(self) -> RecordsInterface:
        """
        Calculate records from last callback to publish.

        Returns
        -------
        RecordsInterface
            Execution time of each operation.

        """
        return self._path_beginning_records.clone()

    def to_path_end_records(self) -> RecordsInterface:
        """
        Calculate records from last callback_start to callback_end.

        Returns
        -------
        RecordsInterface
            Execution time of each operation.

        """
        return self._path_end_records.clone()

    @property
    def _path_beginning_records(self) -> RecordsInterface:
        if self._path_beginning_records_cache is None:
            try:
                self._path_beginning_records_cache = self._to_path_beginning_records_core()
            except Error as e:
                logger.warning(e)
                self._path_beginning_records_cache = RecordsFactory.create_instance()

        assert self._path_beginning_records_cache is not None
        return self._path_beginning_records_cache

    @property
    def _path_end_records(self) -> RecordsInterface:
        if self._path_end_records_cache is None:
            try:
                self._path_end_records_cache = self._to_path_end_records_core()
            except Error as e:
                logger.warning(e)
                self._path_end_records_cache = RecordsFactory.create_instance()

        assert self._path_end_records_cache is not None
        return self._path_end_records_cache

    def _to_path_beginning_records_core(self) -> RecordsInterface:
        """
        Calculate records from last callback_start to publish.

        Returns
        -------
        RecordsInterface
            node partial latency (callback_start-publish).

        """
        if self._val.publisher is None:
            return RecordsFactory.create_instance()

        records = self._provider.path_beginning_records(self._val.publisher)
        return records

    def _to_path_end_records_core(self) -> RecordsInterface:
        """
        Calculate records from last callback_start to publish.

        Returns
        -------
        RecordsInterface
            node partial latency (callback_start-publish).

        """
        if self._val.subscription_callback is None:
            return RecordsFactory.create_instance()

        records = self._provider.path_end_records(self._val.subscription_callback)
        return records
