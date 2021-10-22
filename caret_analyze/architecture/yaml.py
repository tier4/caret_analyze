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

from typing import Any, Dict, List

import yaml

from .interface import ArchitectureReader
from ..exceptions import Error, WrongYamlFormatError
from ..value_objects.callback_info import SubscriptionCallbackInfo, TimerCallbackInfo
from ..value_objects.callback_type import CallbackType
from ..value_objects.path_alias import PathAlias
from ..value_objects.publisher import Publisher
from ..value_objects.variable_passing_info import VariablePassingInfo

YamlObject = Dict[str, Any]


class ArchitectureYaml(ArchitectureReader):

    def __init__(self, file_path: str):
        with open(file_path, 'r') as f:
            yaml_str = f.read()
            self._arch = yaml.safe_load(yaml_str)

    def get_node_names(self) -> List[str]:
        nodes = self._arch['nodes']
        return [node['node_name'] for node in nodes]

    def get_timer_callbacks(
        self,
        node_name: str
    ) -> List[TimerCallbackInfo]:
        try:
            node = self._get_node_info(node_name)
            callbacks_info = filter(
                lambda x: x['type'] == CallbackType.Timer.name, node['callbacks'])

            callbacks = []
            for info in callbacks_info:
                callbacks.append(
                    TimerCallbackInfo(
                        node['node_name'],
                        info['callback_name'],
                        info['symbol'],
                        info['period_ns'],
                    )
                )
            return callbacks
        except Error:
            return []

    def get_path_aliases(self) -> List[PathAlias]:
        aliases_info = self._arch['path_name_aliases']

        aliases = []
        for alias in aliases_info:
            aliases.append(
                PathAlias(alias['path_name'], *alias['callbacks'])
            )
        return aliases

    def get_variable_passings(self, node_name: str) -> List[VariablePassingInfo]:
        try:
            node = self._get_node_info(node_name)

            if 'variable_passings' not in node.keys():
                return []

            var_passes = []
            for info in node['variable_passings']:
                var_passes.append(
                    VariablePassingInfo(
                        node_name,
                        info['callback_name_write'],
                        info['callback_name_read']
                    )
                )
            return var_passes
        except Error:
            return []

    # def get_executors(self) -> List[ExecutorInfo]:
    #     executors = []
    #     for executor in self._arch['executors']:
    #         executors.append(ExecutorInfo(
    #             executor['type'],
    #             *executor['callbacks']
    #         ))
    #     return executors

    # def get_callback_groups(self, node_name: str) -> List[CallbackGroupInfo]:
    #     try:
    #         node = self._get_node_info(node_name)
    #         cbgs = []
    #         for cbg in node['callback_groups']:
    #             cb_type = CallbackGroupType(cbg['type'])
    #             cbgs.append(CallbackGroupInfo(
    #                 cb_type, node_name, *cbg['callbacks']))
    #         return cbgs

    #     except Error:
    #         return []

    def get_subscription_callbacks(
        self,
        node_name: str,
    ) -> List[SubscriptionCallbackInfo]:
        try:
            node = self._get_node_info(node_name)
            callbacks_info = filter(
                lambda x: x['type'] == CallbackType.Subscription.name, node['callbacks'])

            callbacks = []
            for info in callbacks_info:
                callbacks.append(
                    SubscriptionCallbackInfo(
                        node['node_name'],
                        info['callback_name'],
                        info['symbol'],
                        info['topic_name']
                    )
                )

            return callbacks
        except Error:
            return []

    def get_publishers(
        self,
        node_name: str,
    ) -> List[Publisher]:
        try:
            node = self._get_node_info(node_name)
            publishers = []

            if 'publishes' not in node.keys():
                return []

            for pub in node['publishes']:

                publishers.append(
                    Publisher(
                        node_name, pub['topic_name'], pub['callback_name']
                    )
                )
            return publishers
        except Error:
            return []

    def _get_node_info(self, node_name: str) -> YamlObject:
        nodes = self._arch['nodes']
        nodes = list(filter(lambda x: x['node_name'] == node_name, nodes))

        if len(nodes) == 0:
            message = f'Failed to find node by node_name. target node name = {node_name}'
            raise WrongYamlFormatError(message)

        if len(nodes) >= 2:
            message = (
                'Failed to specify node by node_name. same node_name are exist.'
                f'target node name = {node_name}')
            raise WrongYamlFormatError(message)

        return nodes[0]
