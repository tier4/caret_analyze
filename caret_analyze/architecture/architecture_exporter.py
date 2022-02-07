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

from typing import Dict, List, Optional, Tuple

from .reader_interface import UNDEFINED_STR
from ..exceptions import InvalidArgumentError, UnsupportedTypeError
from ..value_objects import (CallbackStructValue, ExecutorStructValue,
                             NodePathStructValue, NodeStructValue,
                             PathStructValue, PublisherStructValue,
                             SubscriptionCallbackStructValue,
                             SubscriptionStructValue, TimerCallbackStructValue,
                             VariablePassingStructValue)


class ArchitectureExporter():

    def __init__(
        self,
        node_values: Tuple[NodeStructValue, ...],
        executor_values: Tuple[ExecutorStructValue, ...],
        named_path_values: Tuple[PathStructValue, ...],
        force: bool = False
    ) -> None:
        self._named_path_values = named_path_values
        self._executor_values = executor_values
        self._node_values = node_values
        self._force = force

    def execute(self, file_path: str) -> None:
        mode = 'w' if self._force else 'x'
        with open(file_path, mode=mode) as f:
            f.write(str(self))

    def __str__(self) -> str:
        import yaml
        obj = self.to_dict()
        return yaml.dump(obj, indent=2, default_flow_style=False, sort_keys=False)

    def to_dict(self):
        named_path_dicts = NamedPathsDicts(self._named_path_values)
        executor_dicts = ExecutorsDicts(self._executor_values)
        nodes_dicts = NodesDicts(self._node_values)
        return {
            'named_paths': named_path_dicts.data,
            'executors': executor_dicts.data,
            'nodes': nodes_dicts.data
        }


class NamedPathsDicts:
    def __init__(
        self,
        named_path_values: List[PathStructValue]
    ) -> None:
        self._data = [self._to_dict(p) for p in named_path_values]

    @property
    def data(self) -> List[Dict]:
        return self._data

    def _to_dict(self, path_value: PathStructValue):
        obj: Dict = {}
        obj['path_name'] = path_value.path_name
        node_chain = []
        for node_path in path_value.node_paths:
            node_chain.append(
                {
                    'node_name': node_path.node_name,
                    'publish_topic_name': node_path.publish_topic_name or UNDEFINED_STR,
                    'subscribe_topic_name': node_path.subscribe_topic_name or UNDEFINED_STR
                }
            )
        obj['node_chain'] = node_chain
        return obj


class CallbackDicts:
    def __init__(
        self,
        callback_values: Tuple[CallbackStructValue, ...]
    ) -> None:
        callbacks_dicts = [self._cb_to_dict(c) for c in callback_values]
        self._data = sorted(callbacks_dicts, key=lambda x: x['callback_name'])

    def _timer_cb_to_dict(
        self,
        timer_callback: TimerCallbackStructValue
    ) -> Dict:
        return  \
            {
                'callback_name': timer_callback.callback_name,
                'callback_type': 'timer_callback',
                'period_ns': timer_callback.period_ns,
                'symbol': timer_callback.symbol,
            }

    def _sub_cb_to_dict(
        self,
        subscription_callback: SubscriptionCallbackStructValue
    ) -> Dict:
        return {
            'callback_name': subscription_callback.callback_name,
            'callback_type': 'subscription_callback',
            'topic_name': subscription_callback.subscribe_topic_name,
            'symbol': subscription_callback.symbol,
        }

    def _cb_to_dict(
        self,
        callback: CallbackStructValue
    ) -> Dict:
        if isinstance(callback, TimerCallbackStructValue):
            return self._timer_cb_to_dict(callback)
        if isinstance(callback, SubscriptionCallbackStructValue):
            return self._sub_cb_to_dict(callback)

        raise UnsupportedTypeError('')

    @property
    def data(self) -> List[Dict]:
        return self._data


class VarPassDicts:
    def __init__(
        self,
        var_pass_values: Optional[Tuple[VariablePassingStructValue, ...]]
    ) -> None:
        self._data: List[Dict] = []

        if var_pass_values is None:
            self._data = [self._undefined_dict]
            return None

        for var_pass in var_pass_values:
            self._data.append(
                {
                    'callback_name_write': var_pass.callback_name_write,
                    'callback_name_read': var_pass.callback_name_read,
                }
            )

        if len(self._data) == 0:
            self._data.append(self._undefined_dict)

        return None

    @property
    def _undefined_dict(self) -> Dict:
        return \
            {
                'callback_name_write': UNDEFINED_STR,
                'callback_name_read': UNDEFINED_STR,
            }

    @property
    def data(self) -> List[Dict]:
        return self._data


class PubDicts:

    def __init__(self, pubisher_values: Tuple[PublisherStructValue, ...]) -> None:
        dicts = [self._to_dict(p) for p in pubisher_values]
        self._data = sorted(dicts, key=lambda x: x['topic_name'])

    def _to_dict(self, publisher_value: PublisherStructValue):

        if publisher_value.callback_names is None or len(publisher_value.callback_names) == 0:
            callback_names = [UNDEFINED_STR]
        else:
            callback_names = list(publisher_value.callback_names)

        return {
            'topic_name': publisher_value.topic_name,
            'callback_names': callback_names,
        }

    @property
    def data(self) -> List[Dict]:
        return self._data


class SubDicts:

    def __init__(self, subscription_values: Tuple[SubscriptionStructValue, ...]) -> None:
        dicts = [self._to_dict(s) for s in subscription_values]
        self._data = sorted(dicts, key=lambda x: x['topic_name'])

    def _to_dict(self, subscription_value: SubscriptionStructValue):
        return {
            'topic_name': subscription_value.topic_name,
            'callback_name': subscription_value.callback_name or UNDEFINED_STR
        }

    @property
    def data(self) -> List[Dict]:
        return self._data


class NodesDicts:

    def __init__(
        self,
        node_values: List[NodeStructValue],
    ) -> None:
        nodes_dicts = [self._to_dict(n) for n in node_values]
        self._data = sorted(nodes_dicts, key=lambda x: x['node_name'])

    @property
    def data(self) -> List[Dict]:
        return self._data

    def _to_dict(
        self,
        node: NodeStructValue,
    ) -> Dict:
        obj: Dict = {}
        obj['node_name'] = f'{node.node_name}'

        if node.callback_groups is not None:
            obj['callback_groups'] = [{
                'callback_group_type': cbg.callback_group_type_name,
                'callback_group_name': cbg.callback_group_name,
                'callback_names': sorted(cbg.callback_names)
            } for cbg in node.callback_groups]

        if node.callbacks is not None:
            if len(node.callbacks) >= 1:
                obj['callbacks'] = CallbackDicts(node.callbacks).data
            if len(node.callbacks) >= 2:
                obj['variable_passings'] = VarPassDicts(
                    node.variable_passings).data

        if len(node.publishers) >= 1:
            obj['publishes'] = PubDicts(node.publishers).data

        if len(node.subscriptions) >= 1:
            obj['subscribes'] = SubDicts(node.subscriptions).data

        if len(node.subscriptions) >= 1 and len(node.publishers) >= 1:
            obj['message_contexts'] = MessageContextDicts(node.paths).data

        return obj


class MessageContextDicts:
    def __init__(
        self,
        paths: Tuple[NodePathStructValue, ...],
    ) -> None:
        self._data = []
        for path in paths:
            if path.publish_topic_name is None or path.subscribe_topic_name is None:
                continue
            message_context = path.message_context
            if message_context is None:
                self._data.append(
                    {
                        'context_type': UNDEFINED_STR,
                        'subscription_topic_name': path.subscribe_topic_name,
                        'publisher_topic_name': path.publish_topic_name
                    }
                )
            else:
                self._data.append(message_context.to_dict())

    @property
    def data(self) -> List[Dict]:
        return self._data


class ExecutorsDicts:
    def __init__(
        self,
        executor_values: List[ExecutorStructValue],
    ) -> None:
        exec_dicts = [self._to_dict(e) for e in executor_values]
        self._data = sorted(exec_dicts, key=lambda x: x['executor_name'])

    @property
    def data(self) -> List[Dict]:
        return self._data

    @staticmethod
    def _to_dict(executor_value: ExecutorStructValue) -> Dict:
        if executor_value.executor_name is None:
            raise InvalidArgumentError('executor_value.executor_name is None')

        cbgs = list(executor_value.callback_groups)
        cbgs = sorted(cbgs, key=lambda x: x.callback_group_name)

        obj = {
            'executor_type': executor_value.executor_type_name,
            'executor_name': executor_value.executor_name,
            'callback_group_names': [
                cbg.callback_group_name
                for cbg
                in cbgs
            ]
        }
        return obj
