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
from logging import getLogger
from typing import Any

import yaml

from ...architecture.reader_interface import ArchitectureReader, UNDEFINED_STR
from ...common import Util
from ...exceptions import InvalidYamlFormatError
from ...value_objects import (CallbackGroupValue, CallbackType, ExecutorValue,
                              NodePathValue, NodeValue, NodeValueWithId,
                              PathValue, PublisherValue, PublishTopicInfoValue,
                              ServiceCallbackValue, ServiceValue,
                              SubscriptionCallbackValue, SubscriptionValue,
                              TimerCallbackValue, TimerValue, VariablePassingValue)

logger = getLogger(__name__)


class ArchitectureReaderYaml(ArchitectureReader):

    def __init__(self, file_path: str):
        with open(file_path, 'r') as f:
            yaml_str = f.read()
        self._arch = yaml.safe_load(yaml_str)

        if self._arch is None:
            raise InvalidYamlFormatError('Failed to parse yaml.')

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
            Always empty sequence.

        """
        logger.warning('get_node_names_and_cb_symbols method is not implemented '
                       'in ArchitectureReaderYaml class.')
        return []

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        """
        Get nodes.

        Returns
        -------
        Sequence[NodeValueWithId]
            All node objects defined in the architecture file.

        """
        nodes_dict = self._get_value(self._arch, 'nodes')
        nodes = []
        for node_dict in nodes_dict:
            node_name = self._get_value(node_dict, 'node_name')
            #  In yaml reader, node_name is used as node_id.
            nodes.append(NodeValueWithId(node_name, node_name))
        return nodes

    def _get_value(self, obj: dict, key_name: str):
        try:
            v = obj[key_name]
            if v is None:
                raise InvalidYamlFormatError(f"'{key_name}' value is None. obj: {obj}")
            return v
        except (KeyError, TypeError) as e:
            msg = f'Failed to parse yaml file. {key_name} not found. obj: {obj}'
            raise InvalidYamlFormatError(msg) from e

    def _get_value_with_default(
        self,
        obj: dict,
        key_name: str,
        default: Any
    ):
        return obj.get(key_name, default)

    def _get_publish_topic_infos(
        self,
        node_name: str,
        callback_name: str
    ) -> list[PublishTopicInfoValue]:
        node_dict = self._get_node_dict(node_name)
        if 'publishes' not in node_dict.keys():
            return []

        publishes = self._get_value(node_dict, 'publishes')
        topic_names = []
        for pub in publishes:
            if callback_name in self._get_value(pub, 'callback_names'):
                if 'construction_order' in pub.keys():
                    construction_order = self._get_value(pub, 'construction_order')
                else:
                    construction_order = 0
                pub_info = PublishTopicInfoValue(self._get_value(pub, 'topic_name'),
                                                 construction_order)
                topic_names.append(pub_info)

        return topic_names

    def get_timer_callbacks(
        self,
        node: NodeValue,
    ) -> list[TimerCallbackValue]:
        """
        Get timer callback values.

        Parameters
        ----------
        node : NodeValue
            target node

        Returns
        -------
        list[TimerCallbackValue]
            Timer callback object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)

        if 'callbacks' not in node_dict:
            return []

        cbs_dict = self._get_value(node_dict, 'callbacks')
        cbs_dict = Util.filter_items(
            lambda x: self._get_value(x, 'callback_type') == str(CallbackType.TIMER),
            cbs_dict
        )

        callbacks = []
        for _, info in enumerate(cbs_dict):
            callback_name = self._get_value(info, 'callback_name')
            publish_topic_names = self._get_publish_topic_infos(node.node_name, callback_name)
            callback_id = callback_name
            node_name = self._get_value(node_dict, 'node_name')
            construction_order = self._get_value_with_default(info, 'construction_order', 0)

            callbacks.append(
                TimerCallbackValue(
                    callback_id=callback_id,
                    node_name=node_name,
                    node_id=node_name,
                    symbol=self._get_value(info, 'symbol'),
                    period_ns=self._get_value(info, 'period_ns'),
                    publish_topics=tuple(publish_topic_names),
                    construction_order=construction_order,
                    callback_name=callback_name)
            )
        return callbacks

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> list[dict]:
        """
        Get message contexts.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[dict]
            Message context object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)
        if 'message_contexts' not in node_dict.keys():
            return []

        context_dicts = self._get_value(node_dict, 'message_contexts')
        for context in context_dicts:
            # Verify the existence of the required key.
            self._get_value(context, 'context_type')
            self._get_value(context, 'publisher_topic_name')
            self._get_value(context, 'subscription_topic_name')

        return context_dicts

    def get_callback_groups(
        self,
        node: NodeValue
    ) -> list[CallbackGroupValue]:
        """
        Get callback groups.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[CallbackGroupValue]
            Callback group object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)
        if 'callback_groups' not in node_dict.keys():
            return []

        callback_groups: list[CallbackGroupValue] = []
        cbg_dicts = self._get_value(node_dict, 'callback_groups')
        for cbg_dict in cbg_dicts:
            cbg_name = self._get_value(cbg_dict, 'callback_group_name')
            cbg = CallbackGroupValue(
                callback_group_type_name=self._get_value(cbg_dict, 'callback_group_type'),
                node_name=node.node_name,
                node_id=node.node_name,
                callback_ids=tuple(self._get_value(cbg_dict, 'callback_names')),
                callback_group_id=cbg_name,
                callback_group_name=cbg_name
            )
            callback_groups.append(cbg)

        return callback_groups

    def get_paths(self) -> list[PathValue]:
        """
        Get paths.

        Returns
        -------
        list[PathValue]
            All path information objects defined in the architecture file.

        """
        aliases_info = self._get_value(self._arch, 'named_paths')

        paths_info = []
        for alias in aliases_info:
            path_name = self._get_value(alias, 'path_name')

            node_chain: list[NodePathValue] = []
            for node_path_value in self._get_value(alias, 'node_chain'):
                node_name = self._get_value(node_path_value, 'node_name')
                pub_topic = self._get_value_with_default(
                    node_path_value, 'publish_topic_name', None)
                sub_topic = self._get_value_with_default(
                    node_path_value, 'subscribe_topic_name', None)
                publisher_construction_order = self._get_value_with_default(
                    node_path_value, 'publisher_construction_order', 0)
                subscription_construction_order = self._get_value_with_default(
                    node_path_value, 'subscription_construction_order', 0)

                if pub_topic == UNDEFINED_STR or pub_topic is None:
                    pub_topic = None
                    publisher_construction_order = None
                if sub_topic == UNDEFINED_STR or sub_topic is None:
                    sub_topic = None
                    subscription_construction_order = None

                node_path_value = NodePathValue(
                    node_name, sub_topic, pub_topic,
                    publisher_construction_order, subscription_construction_order)
                node_chain.append(node_path_value)

            paths_info.append(
                PathValue(path_name, tuple(node_chain))
            )
        return paths_info

    def get_variable_passings(
        self,
        node: NodeValue,
    ) -> list[VariablePassingValue]:
        """
        Get variable passings.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[VariablePassingValue]
            Variable passing object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)

        if 'variable_passings' not in node_dict.keys():
            return []

        var_passes = []
        for val in self._get_value(node_dict, 'variable_passings'):
            var_passes.append(
                VariablePassingValue(
                    self._get_value(node_dict, 'node_name'),
                    self._get_value(val, 'callback_name_write'),
                    self._get_value(val, 'callback_name_read'),
                )
            )
        return var_passes

    def get_executors(self) -> list[ExecutorValue]:
        """
        Get executors.

        Returns
        -------
        list[ExecutorValue]
            All executor objects defined in the architecture file.

        """
        executors = []
        for e in self._get_value(self._arch, 'executors'):
            cbg_names = self._get_value(e, 'callback_group_names')
            executor_name = self._get_value(e, 'executor_name')
            executor = ExecutorValue(
                e['executor_type'],
                callback_group_ids=tuple(cbg_names),
                executor_name=executor_name)
            executors.append(executor)
        return executors

    def get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> list[SubscriptionCallbackValue]:
        """
        Get subscription callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[SubscriptionCallbackValue]
            Subscription callback object of specified node defined in architecture file.

        """
        def is_target(x: dict):
            return self._get_value(x, 'callback_type') == CallbackType.SUBSCRIPTION.type_name
        node_dict = self._get_node_dict(node.node_name)

        if 'callbacks' not in node_dict:
            return []

        callback_values = self._get_value(node_dict, 'callbacks')
        callback_values = filter(is_target, callback_values)

        callbacks = []
        for val in callback_values:
            callback_name = self._get_value(val, 'callback_name')
            publish_topic_infos = self._get_publish_topic_infos(node.node_name, callback_name)
            callback_id = callback_name
            node_name = self._get_value(node_dict, 'node_name')
            construction_order = self._get_value_with_default(val, 'construction_order', 0)
            callbacks.append(
                SubscriptionCallbackValue(
                    callback_id=callback_id,
                    node_id=node_name,
                    node_name=node_name,
                    symbol=self._get_value(val, 'symbol'),
                    subscribe_topic_name=self._get_value(val, 'topic_name'),
                    publish_topics=tuple(publish_topic_infos),
                    callback_name=callback_name,
                    construction_order=construction_order
                )
            )

        return callbacks

    def get_service_callbacks(self, node: NodeValue) -> Sequence[ServiceCallbackValue]:
        """
        Get service callbacks.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[ServiceCallbackValue]
            Always empty sequence.

        """
        return []

    def get_publishers(
        self,
        node: NodeValue
    ) -> list[PublisherValue]:
        """
        Get publishers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[PublisherValue]
            Publisher object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)
        publishers = []

        if 'publishes' not in node_dict.keys():
            return []

        for pub in self._get_value(node_dict, 'publishes'):
            publishers.append(
                PublisherValue(
                    topic_name=self._get_value(pub, 'topic_name'),
                    node_name=node.node_name,
                    node_id=node.node_name,
                    callback_ids=tuple(self._get_value(pub, 'callback_names')),
                    construction_order=self._get_value_with_default(pub, 'construction_order', 0)
                )
            )
        return publishers

    def get_timers(
        self,
        node: NodeValue
    ) -> list[TimerValue]:
        """
        Get timers.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[TimerValue]
            Timer object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)
        timers = []

        if 'callbacks' not in node_dict.keys():
            return []
        cbs_dict = self._get_value(node_dict, 'callbacks')

        for cb_dict in cbs_dict:
            if self._get_value(cb_dict, 'callback_type') != 'timer_callback':
                continue
            # construction_order of timer uses callback
            timers.append(
                TimerValue(
                    period=int(self._get_value(cb_dict, 'period_ns')),
                    node_name=node.node_name,
                    node_id=node.node_id,
                    callback_id=self._get_value(cb_dict, 'callback_name'),
                    construction_order=(
                        self._get_value_with_default(cb_dict, 'construction_order', 0))
                )
            )
        return timers

    def get_subscriptions(
        self,
        node: NodeValue
    ) -> list[SubscriptionValue]:
        """
        Get subscriptions.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        list[SubscriptionValue]
            Subscription object of specified node defined in architecture file.

        """
        node_dict = self._get_node_dict(node.node_name)
        subscriptions = []

        if 'subscribes' not in node_dict.keys():
            return []

        for sub in self._get_value(node_dict, 'subscribes'):
            subscriptions.append(
                SubscriptionValue(
                    node_name=node.node_name,
                    node_id=node.node_name,
                    topic_name=self._get_value(sub, 'topic_name'),
                    callback_id=self._get_value(sub, 'callback_name'),
                    construction_order=self._get_value_with_default(sub, 'construction_order', 0)
                )
            )
        return subscriptions

    def get_services(self, node: NodeValue) -> Sequence[ServiceValue]:
        """
        Get services.

        Parameters
        ----------
        node : NodeValue
            Target node value.

        Returns
        -------
        Sequence[ServiceValue]
            Always empty sequence.

        """
        return []

    def _get_node_dict(
        self,
        node_name: str
    ) -> dict:
        node_values = self._get_value(self._arch, 'nodes')
        nodes = list(filter(lambda x: self._get_value(
            x, 'node_name') == node_name, node_values))

        if len(nodes) == 0:
            message = f'Failed to find node by node_name. target node name = {node_name}'
            raise InvalidYamlFormatError(message)

        if len(nodes) >= 2:
            message = (
                'Failed to specify node by node_name. same node_name are exist.'
                f'target node name = {node_name}')
            raise InvalidYamlFormatError(message)

        return nodes[0]
