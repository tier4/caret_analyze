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

from collections.abc import Sequence


from .lttng import Lttng, LttngEventFilter
from ...architecture.reader_interface import ArchitectureReader
from ...value_objects import (
    CallbackGroupValue,
    ExecutorValue,
    NodeValue,
    NodeValueWithId,
    PathValue,
    PublisherValue,
    ServiceCallbackValue,
    ServiceValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerCallbackValue,
    TimerValue,
    VariablePassingValue,
)


class ArchitectureReaderLttng(ArchitectureReader):
    def __init__(
        self,
        trace_dir: str | list[str]
    ) -> None:
        self._lttng = Lttng(
            trace_dir, event_filters=[LttngEventFilter.init_pass_filter()],
            validate=False)

    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[tuple[str | None, str | None]]:
        """
        Get node names and callback symbols.

        Parameters
        ----------
        callback_group_id : str
            callback group id.

        Returns
        -------
        Sequence[tuple[str | None, str | None]]
            Node names and callback symbols.

        """
        return self._lttng.get_node_names_and_cb_symbols(callback_group_id)

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            Node value with id.

        """
        return self._lttng.get_nodes()

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
        return self._lttng.get_timer_callbacks(node)

    def get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        """
        Get variable passings.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[VariablePassingValue]
            Always empty sequence.

        """
        return []

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[dict]:
        """
        Get message contexts.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[dict]
            Always empty sequence.

        """
        return []

    def get_executors(
        self
    ) -> Sequence[ExecutorValue]:
        """
        Get executors.

        Returns
        -------
        Sequence[ExecutorValue]
            Executor value.

        """
        return self._lttng.get_executors()

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        """
        Get subscription callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[SubscriptionCallbackValue]
            Subscription callback value.

        """
        return self._lttng.get_subscription_callbacks(node)

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        """
        Get service callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[ServiceCallbackValue]
            Service callback value.

        """
        return self._lttng.get_service_callbacks(node)

    def get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValue]:
        """
        Get publishers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[PublisherValue]
            Publisher value.

        """
        return self._lttng.get_publishers(node)

    def get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        """
        Get timers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[TimerValue]
            Timer value.

        """
        return self._lttng.get_timers(node)

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        """
        Get callback groups.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[CallbackGroupValue]
            Callback group value.

        """
        return self._lttng.get_callback_groups(node)

    def get_paths(
        self
    ) -> Sequence[PathValue]:
        """
        Get paths.

        Returns
        -------
        Sequence[PathValue]
            Path value.

        """
        return []

    def get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        """
        Get subscriptions.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[SubscriptionValue]
            Subscription value.

        """
        return self._lttng.get_subscriptions(node)

    def get_services(
        self,
        node: NodeValue
    ) -> Sequence[ServiceValue]:
        """
        Get services.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[ServiceValue]
            Service value.

        """
        return self._lttng.get_services(node)
