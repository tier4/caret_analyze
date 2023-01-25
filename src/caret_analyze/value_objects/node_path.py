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

from .callback import CallbackStructValue, SubscriptionCallbackStructValue
from .message_context import MessageContext, MessageContextType
from .publisher import PublisherStructValue
from .subscription import SubscriptionStructValue
from .value_object import ValueObject
from .variable_passing import VariablePassingStructValue
from ..common import Summarizable, Summary, Util

logger = getLogger(__name__)


class NodePathValue(ValueObject):
    """
    Value object class for representing a node path.

    This class has minimal information and no structure,
    and used as the return value of ArchitectureReader.
    In CARET, the node path is defined as from subscribe to publish.
    """

    def __init__(
        self,
        node_name: str,
        subscribe_topic_name: Optional[str],
        publish_topic_name: Optional[str],
        publisher_construction_order: Optional[int],
        subscription_construction_order: Optional[int],
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name.
        subscribe_topic_name : Optional[str]
            Topic name which the node-path subscribes.
        publish_topic_name : Optional[str]
            Topic name which the node-path publishes.

        """
        self._node_name = node_name
        self._publish_topic_name = publish_topic_name
        self._subscribe_topic_name = subscribe_topic_name
        self._publisher_construction_order = publisher_construction_order
        self._subscription_construction_order = subscription_construction_order

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def publish_topic_name(self) -> Optional[str]:
        return self._publish_topic_name

    @property
    def subscribe_topic_name(self) -> Optional[str]:
        return self._subscribe_topic_name

    @property
    def publisher_construction_order(self) -> Optional[int]:
        return self._publisher_construction_order

    @property
    def subscription_construction_order(self) -> Optional[int]:
        return self._subscription_construction_order


class NodePathStructValue(ValueObject, Summarizable):
    """
    StructValue object class for representing a node path.

    This class is a structure that includes other related StructValue classes, such as callbacks,
    and used as the return value of Architecture object.
    In CARET, the node path is defined as from subscribe to publish.
    """

    def __init__(
        self,
        node_name: str,
        subscription: Optional[SubscriptionStructValue],
        publisher: Optional[PublisherStructValue],
        child: Optional[Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]],
        message_context: Optional[MessageContext],
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        node_name : str
            Node name
        subscription : Optional[SubscriptionStructValue]
            Subscription which the node path subscribes.
        publisher : Optional[PublisherStructValue]
            Publisher which the node path publishes.
        child : Optional[Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]]
            Child elements of a node path.
            Required only when message_context is callback_chain.
        message_context : Optional[MessageContext]
            Message Context. Used to define node latency.

        """
        self._node_name = node_name
        self._child = child
        self._subscription = subscription
        self._publisher = publisher
        self._context = message_context

    @property
    def node_name(self) -> str:
        return self._node_name

    @property
    def callbacks(self) -> Optional[Tuple[CallbackStructValue, ...]]:
        if self._child is None:
            return None

        cb_values = Util.filter_items(
            lambda x: isinstance(x, CallbackStructValue),
            self._child
        )
        return tuple(cb_values)

    @property
    def summary(self) -> Summary:
        context = None
        if self.message_context is not None:
            context = self.message_context.summary
        d = {
            'node': self.node_name,
            'message_context': context,
            'subscribe_topic_name': self.subscribe_topic_name,
            'publish_topic_name': self.publish_topic_name,
        }
        if self.publisher_construction_order or 0 > 0:
            d['publisher_construction_order'] = self.publisher_construction_order

        if self.subscription_construction_order or 0 > 0:
            d['subscription_construction_order'] = self.subscription_construction_order

        return Summary(d)

    @property
    def callback_names(self) -> Optional[Tuple[str, ...]]:
        if self.callbacks is None:
            return None

        return tuple(_.callback_name for _ in self.callbacks)

    @property
    def variable_passings(self) -> Optional[Tuple[VariablePassingStructValue, ...]]:
        if self._child is None:
            return None

        cbs_info = Util.filter_items(
            lambda x: isinstance(x, VariablePassingStructValue),
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
    ) -> Optional[Tuple[Union[CallbackStructValue, VariablePassingStructValue], ...]]:
        if self._child is None:
            return None

        return tuple(self._child)

    @property
    def publisher(self) -> Optional[PublisherStructValue]:
        return self._publisher

    @property
    def subscription(self) -> Optional[SubscriptionStructValue]:
        return self._subscription

    @property
    def subscription_callback(self) -> Optional[SubscriptionCallbackStructValue]:
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

    @property
    def publisher_construction_order(self) -> Optional[int]:
        if self.publisher:
            return self.publisher.construction_order
        return None

    @property
    def subscription_construction_order(self) -> Optional[int]:
        if self.subscription:
            return self.subscription.construction_order
        return None

