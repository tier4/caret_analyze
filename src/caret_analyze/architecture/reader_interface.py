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

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence

from ..value_objects import (CallbackGroupValue, ExecutorValue, NodeValue,
                             NodeValueWithId, PathValue, PublisherValue,
                             ServiceCallbackValue, ServiceValue,
                             SubscriptionCallbackValue, SubscriptionValue,
                             TimerCallbackValue, TimerValue, VariablePassingValue)

UNDEFINED_STR = 'UNDEFINED'
IGNORE_TOPICS = ['/parameter_events', '/rosout', '/clock']


class ArchitectureReader(metaclass=ABCMeta):
    """Architecture reader base class."""

    @abstractmethod
    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[tuple[str | None, str | None]]:
        """
        Get node names and callback symbols from callback group id.

        Returns
        -------
        Sequence[tuple[str | None, str | None]]
            node names and callback symbols.
            tuple structure: (node_name, callback_symbol)

        """
        pass

    @abstractmethod
    def get_nodes(
        self
    ) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            node values.

        """
        pass

    @abstractmethod
    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        """
        Get timer callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[TimerCallbackValue]
            timer callback values

        """
        pass

    @abstractmethod
    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        """
        Get subscription callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[SubscriptionCallbackValue]
            subscription callback values

        """
        pass

    @abstractmethod
    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        """
        Get service callback values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[ServiceCallbackValue]
            service callback values

        """
        pass

    @abstractmethod
    def get_publishers(
        self,
        node_info: NodeValue
    ) -> Sequence[PublisherValue]:
        """
        Get publishers info.

        Parameters
        ----------
        node_info : NodeValue
            target node

        Returns
        -------
        Sequence[PublisherValue]
            publisher values

        """
        pass

    @abstractmethod
    def get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        """
        Get timers info.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[TimerValue]
            timers values

        """
        pass

    @abstractmethod
    def get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        """
        Get subscription values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[SubscriptionValue]
            subscription values

        """
        pass

    @abstractmethod
    def get_services(
        self,
        node: NodeValue
    ) -> Sequence[ServiceValue]:
        """
        Get service values.

        Parameters
        ----------
        node : NodeInfo
            target node

        Returns
        -------
        Sequence[ServiceValue]
            service values

        """
        pass

    @abstractmethod
    def get_paths(self) -> Sequence[PathValue]:
        """
        Get path value.

        Returns
        -------
        Sequence[PathValue]
            path values

        """
        pass

    @abstractmethod
    def get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[dict]:
        """
        Get message contexts.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[dict]
            message contexts

        """
        pass

    @abstractmethod
    def get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        """
        Get variable passing values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[VariablePassingvalue]
            variable passing values

        """
        pass

    @abstractmethod
    def get_executors(self) -> Sequence[ExecutorValue]:
        """
        Get executor values.

        Returns
        -------
        Sequence[ExecutorValue]
            executor values

        """
        pass

    @abstractmethod
    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback group values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        Sequence[CallbackGroupValue]
            callback group values

        """
        pass
