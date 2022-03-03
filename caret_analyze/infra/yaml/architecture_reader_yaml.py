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

from typing import Any, Dict, List, Sequence

import yaml

from ...architecture.reader_interface import ArchitectureReader, UNDEFINED_STR
from ...common import Util
from ...exceptions import InvalidYamlFormatError
from ...value_objects import (CallbackGroupValue, CallbackType, ExecutorValue,
                              NodePathValue, NodeValue, NodeValueWithId,
                              PathValue, PublisherValue,
                              SubscriptionCallbackValue, SubscriptionValue,
                              TimerCallbackValue, TimerValue, VariablePassingValue)


class ArchitectureReaderYaml(ArchitectureReader):

    def __init__(self, file_path: str):
        with open(file_path, 'r') as f:
            yaml_str = f.read()
        self._arch = yaml.safe_load(yaml_str)

        if self._arch is None:
            raise InvalidYamlFormatError('Failed to parse yaml.')

    def get_nodes(self) -> Sequence[NodeValueWithId]:
        nodes_dict = self._get_value(self._arch, 'nodes')
        nodes = []
        for node_dict in nodes_dict:
            node_name = self._get_value(node_dict, 'node_name')
            #  In yaml reader, node_name is used as node_id.
            nodes.append(NodeValueWithId(node_name, node_name))
        return nodes

    def _get_value(self, obj: Dict, key_name: str):
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
        obj: Dict,
        key_name: str,
        default: Any
    ):
        return obj.get(key_name, default)

    def _get_publish_topic_names(
        self,
        node_name: str,
        callback_name: str
    ) -> List[str]:
        node_dict = self._get_node_dict(node_name)
        if 'publishes' not in node_dict.keys():
            return []

        publishes = self._get_value(node_dict, 'publishes')
        topic_names = [
            self._get_value(p, 'topic_name')
            for p
            in publishes
            if callback_name in self._get_value(p, 'callback_names')]
        return topic_names

    def get_timer_callbacks(
        self,
        node: NodeValue,
    ) -> List[TimerCallbackValue]:
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
            publish_topic_names = self._get_publish_topic_names(node.node_name, callback_name)
            callback_id = callback_name
            node_name = self._get_value(node_dict, 'node_name')
            callbacks.append(
                TimerCallbackValue(
                    callback_id=callback_id,
                    node_name=node_name,
                    node_id=node_name,
                    symbol=self._get_value(info, 'symbol'),
                    period_ns=self._get_value(info, 'period_ns'),
                    publish_topic_names=tuple(publish_topic_names),
                    callback_name=callback_name
                )
            )
        return callbacks

    def get_message_contexts(
        self,
        node: NodeValue
    ) -> List[Dict]:
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
    ) -> List[CallbackGroupValue]:
        node_dict = self._get_node_dict(node.node_name)
        if 'callback_groups' not in node_dict.keys():
            return []

        cbgs: List[CallbackGroupValue] = []
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
            cbgs.append(cbg)

        return cbgs

    def get_paths(self) -> List[PathValue]:
        aliases_info = self._get_value(self._arch, 'named_paths')

        paths_info = []
        for alias in aliases_info:
            path_name = self._get_value(alias, 'path_name')

            node_chain: List[NodePathValue] = []
            for node_path_value in self._get_value(alias, 'node_chain'):
                node_name = self._get_value(node_path_value, 'node_name')
                pub_topic = self._get_value_with_default(
                    node_path_value, 'publish_topic_name', None)
                sub_topic = self._get_value_with_default(
                    node_path_value, 'subscribe_topic_name', None)

                if pub_topic == UNDEFINED_STR:
                    pub_topic = None
                if sub_topic == UNDEFINED_STR:
                    sub_topic = None

                node_path_value = NodePathValue(node_name, sub_topic, pub_topic)
                node_chain.append(node_path_value)

            paths_info.append(
                PathValue(path_name, tuple(node_chain))
            )
        return paths_info

    def get_variable_passings(
        self,
        node: NodeValue,
    ) -> List[VariablePassingValue]:
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

    def get_executors(self) -> List[ExecutorValue]:
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
    ) -> List[SubscriptionCallbackValue]:
        def is_target(x: Dict):
            return self._get_value(x, 'callback_type') == CallbackType.SUBSCRIPTION.type_name
        node_dict = self._get_node_dict(node.node_name)

        if 'callbacks' not in node_dict:
            return []

        callback_values = self._get_value(node_dict, 'callbacks')
        callback_values = filter(is_target, callback_values)

        callbacks = []
        for val in callback_values:
            callback_name = self._get_value(val, 'callback_name')
            publish_topic_names = self._get_publish_topic_names(node.node_name, callback_name)
            callback_id = callback_name
            node_name = self._get_value(node_dict, 'node_name')
            callbacks.append(
                SubscriptionCallbackValue(
                    callback_id=callback_id,
                    node_id=node_name,
                    node_name=node_name,
                    symbol=self._get_value(val, 'symbol'),
                    subscribe_topic_name=self._get_value(val, 'topic_name'),
                    publish_topic_names=tuple(publish_topic_names),
                    callback_name=callback_name
                )
            )

        return callbacks

    def get_publishers(
        self,
        node: NodeValue
    ) -> List[PublisherValue]:
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
                )
            )
        return publishers

    def get_timers(
        self,
        node: NodeValue
    ) -> List[TimerValue]:
        node_dict = self._get_node_dict(node.node_name)
        timers = []

        if 'callbacks' not in node_dict.keys():
            return []
        cbs_dict = self._get_value(node_dict, 'callbacks')

        for cb_dict in cbs_dict:
            if self._get_value(cb_dict, 'callback_type') != 'timer_callback':
                continue
            timers.append(
                TimerValue(
                    period=int(self._get_value(cb_dict, 'period_ns')),
                    node_name=node.node_name,
                    node_id=node.node_id,
                    callback_id=self._get_value(cb_dict, 'callback_name'),
                )
            )
        return timers

    def get_subscriptions(
        self,
        node: NodeValue
    ) -> List[SubscriptionValue]:
        node_dict = self._get_node_dict(node.node_name)
        subscriptions = []

        if 'subscribes' not in node_dict.keys():
            return []

        for pub in self._get_value(node_dict, 'subscribes'):
            subscriptions.append(
                SubscriptionValue(
                    node_name=node.node_name,
                    node_id=node.node_name,
                    topic_name=self._get_value(pub, 'topic_name'),
                    callback_id=self._get_value(pub, 'callback_name'),
                )
            )
        return subscriptions

    def _get_node_dict(
        self,
        node_name: str
    ) -> Dict:
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
