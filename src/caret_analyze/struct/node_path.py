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

from logging import getLogger
from typing import Optional, Tuple, Union

from .callback import CallbackStruct, SubscriptionCallbackStruct
from ..value_objects.message_context import MessageContextType
from .message_context import MessageContext
from .publisher import PublisherStruct
from .subscription import SubscriptionStruct
from .variable_passing import VariablePassingStruct
from ..common import Summarizable, Summary, Util

logger = getLogger(__name__)


class NodePathStruct(Summarizable):
    def __init__(
        self,
        node_name: str,
        subscription: Optional[SubscriptionStruct],
        publisher: Optional[PublisherStruct],
        child: Optional[Tuple[Union[CallbackStruct, VariablePassingStruct], ...]],
        message_context: Optional[MessageContext],
    ) -> None:
        self._node_name = node_name
        self._child = child
        self._subscription = subscription
        self._publisher = publisher
        self._context = message_context

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def callbacks(self) -> Optional[Tuple[CallbackStruct, ...]]:
        if self._child is None:
            return None

        cb_values = Util.filter_items(
            lambda x: isinstance(x, CallbackStruct),
            self._child
        )
        return tuple(cb_values)

    @property
    def summary(self) -> Summary:
        context = None
        if self.message_context is not None:
            context = self.message_context.summary
        return Summary({
            'node': self.node_name,
            'message_context': context,
            'subscribe_topic_name': self.subscribe_topic_name,
            'publish_topic_name': self.publish_topic_name,
        })

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self.callbacks is None:
            return None

        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def variable_passings(self) -> Optional[Tuple[VariablePassingStruct, ...]]:
        if self._child is None:
            return None

        cbs_info = Util.filter_items(
            lambda x: isinstance(x, VariablePassingStruct),
            self._child
        )
        return tuple(cbs_info)

    @property
    def message_context(self) -> Optional[MessageContext]:
        return self._context

    @property
    def message_context_type(self) -> Optional[MessageContextType]:
        if self._context is None:
            return None

        return self._context.context_type

    @property
    def child(
        self,
    ) -> Optional[Tuple[Union[CallbackStruct, VariablePassingStruct], ...]]:
        if self._child is None:
            return None

        return tuple(self._child)

    @property
    def publisher(self) -> Optional[PublisherStruct]:
        return self._publisher

    @property
    def subscription(self) -> Optional[SubscriptionStruct]:
        return self._subscription

    @property
    def subscription_callback(self) -> Optional[SubscriptionCallbackStruct]:
        if self._subscription is not None:
            return self._subscription.callback
        return None

    @property
    def publish_topic_name(self) -> Optional[str]:
        if self._publisher is None:
            return None
        return self._publisher.topic_name

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        if self._subscription is None:
            return None
        return self._subscription.topic_name
