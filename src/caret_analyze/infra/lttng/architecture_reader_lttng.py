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

from collections import defaultdict
from collections.abc import Sequence


from .lttng import LttngEventFilter
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
        trace_dir: str
    ) -> None:
        from .lttng import Lttng
        self._lttng = Lttng(
            trace_dir, event_filters=[LttngEventFilter.init_pass_filter()],
            validate=False)

    def get_node_names_and_cb_symbols(
        self,
        callback_group_id: str
    ) -> Sequence[tuple[str | None, str | None]]:
        return self._lttng.get_node_names_and_cb_symbols(callback_group_id)

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        return self._lttng.get_nodes()

    def get_timer_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[TimerCallbackValue]:
        return self._lttng.get_timer_callbacks(node)

    def get_variable_passings(
        self,
        node: NodeValue
    ) -> Sequence[VariablePassingValue]:
        return []

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> Sequence[dict]:
        return []

    def get_executors(
        self
    ) -> Sequence[ExecutorValue]:
        return self._lttng.get_executors()

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionCallbackValue]:
        return self._lttng.get_subscription_callbacks(node)

    def get_service_callbacks(
        self,
        node: NodeValue
    ) -> Sequence[ServiceCallbackValue]:
        return self._lttng.get_service_callbacks(node)

    def get_publishers(
        self,
        node: NodeValue
    ) -> Sequence[PublisherValue]:
        return self._lttng.get_publishers(node)

    def get_timers(
        self,
        node: NodeValue
    ) -> Sequence[TimerValue]:
        return self._lttng.get_timers(node)

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> Sequence[CallbackGroupValue]:
        return self._lttng.get_callback_groups(node)

    def get_paths(
        self
    ) -> Sequence[PathValue]:
        return []

    def get_subscriptions(
        self,
        node: NodeValue
    ) -> Sequence[SubscriptionValue]:
        info: list[SubscriptionValue] = []
        construction_order_counter: dict[tuple[str, str], int] = defaultdict(int)

        for sub_cb in self.get_subscription_callbacks(node):
            topic_name = sub_cb.subscribe_topic_name
            node_name = sub_cb.node_name
            construction_order = construction_order_counter[node_name, topic_name]
            construction_order_counter[node_name, topic_name] += 1

            info.append(SubscriptionValue(
                topic_name=topic_name,
                node_name=node_name,
                node_id=sub_cb.node_id,
                callback_id=sub_cb.callback_id,
                construction_order=construction_order
            ))
        return info

    def get_services(
        self,
        node: NodeValue
    ) -> Sequence[ServiceValue]:
        info: list[ServiceValue] = []
        construction_order_counter: dict[tuple[str, str], int] = defaultdict(int)
        for srv_cb in self.get_service_callbacks(node):
            service_name = srv_cb.service_name
            node_name = srv_cb.node_name
            construction_order = construction_order_counter[node_name, service_name]
            construction_order_counter[node_name, service_name] += 1

            info.append(ServiceValue(
                service_name=service_name,
                node_name=srv_cb.node_name,
                node_id=srv_cb.node_id,
                callback_id=srv_cb.callback_id,
                construction_order=construction_order
            ))
        return info
