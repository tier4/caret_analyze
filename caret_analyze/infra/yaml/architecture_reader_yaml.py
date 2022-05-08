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

from collections import UserDict
from functools import lru_cache
from typing import Any, Dict, List, Optional, Sequence, Set

import yaml

from ...architecture.reader_interface import ArchitectureReader
from ...common import Util
from ...exceptions import InvalidYamlFormatError
from ...value_objects import (
    CallbackGroupValue,
    CallbackType,
    ExecutorValue,
    NodePathValue,
    NodeValue,
    PathValue,
    PublisherValue,
    SubscriptionCallbackValue,
    SubscriptionValue,
    TimerCallbackValue,
    TimerValue,
    TransformBroadcasterValue,
    TransformBufferValue,
    TransformValue,
    VariablePassingValue,
    ClientCallbackValue,
    ServiceCallbackValue,
)


class YamlDict(UserDict):

    def __init__(self, init=None):
        super().__init__(init)

    def __getitem__(self, key: str) -> Any:
        try:
            v = super().__getitem__(key)
            return v
        except (KeyError, TypeError) as e:
            msg = f'Failed to parse yaml file. {key} not found. obj: {self.data}'
            raise InvalidYamlFormatError(msg) from e

    def get_value(self, key: str) -> str:
        v = self.__getitem__(key)
        if isinstance(v, str):
            return v
        if isinstance(v, int):
            return str(v)

        raise InvalidYamlFormatError('Failed to parse yaml file. ')

    def get_value_with_default(
        self,
        key: str,
        default: Optional[str]
    ) -> Optional[str]:
        if key in self.data:
            v = self.__getitem__(key)
            assert v is None or isinstance(v, str)
            return v
        return default

    def get_values(self, key: str) -> List[str]:
        return [str(v) for v in self.__getitem__(key)]

    def get_dict(self, key: str) -> YamlDict:
        return YamlDict(self.__getitem__(key))

    def get_dicts(self, key: str) -> List[YamlDict]:
        return [YamlDict(d) for d in self.__getitem__(key)]


class ArchitectureReaderYaml(ArchitectureReader):

    def __init__(self, file_path: str):
        with open(file_path, 'r') as f:
            yaml_str = f.read()

        try:
            self._arch_dict = YamlDict(yaml.safe_load(yaml_str))
            if set(self._arch_dict.keys()) != {'executors', 'nodes', 'named_paths'}:
                raise InvalidYamlFormatError('Failed to parse yaml.')
        except ValueError:
            raise InvalidYamlFormatError('Failed to parse yaml.')

    @lru_cache
    def get_node(self, node_name: str) -> NodeValue:
        nodes = self.get_nodes()
        for node in nodes:
            if node.node_name == node_name:
                return node
        raise NotImplementedError('')

    def get_nodes(self) -> Sequence[NodeValue]:
        nodes_dict = self._arch_dict.get_dicts('nodes')
        nodes = []
        for node_dict in nodes_dict:
            node_name = node_dict.get_value('node_name')
            node_id = node_dict.get_value('node_id')
            nodes.append(NodeValue(node_name, node_id))
        return nodes

    def _get_service_callbacks(self, node: NodeValue) -> Sequence[ServiceCallbackValue]:
        return []

    def _get_client_callbacks(self, node: NodeValue) -> Sequence[ClientCallbackValue]:
        return []

    def _get_value_with_default(
        self,
        obj: Dict,
        key_name: str,
        default: Any
    ):
        return obj.get(key_name, default)

    @lru_cache
    def get_tf_frames(self) -> Sequence[TransformValue]:
        nodes = self.get_nodes()
        frames: Set[TransformValue] = set()

        for node in nodes:
            tf_br = self.get_tf_broadcaster(node.node_name)
            if tf_br is None:
                continue
            frames |= set(tf_br.broadcast_transforms)

        return list(frames)

    def _get_publish_topic_names(
        self,
        node_name: str,
        callback_id: str
    ) -> List[str]:
        node_dict = self._get_node_dict(node_name)
        if 'publishes' not in node_dict:
            return []

        publishes = node_dict.get_dicts('publishes')
        topic_names = [
            p.get_value('topic_name')
            for p
            in publishes
            if callback_id in p.get_values('callback_ids')]
        return topic_names

    def _get_timer_callbacks(
        self,
        node: NodeValue
    ) -> List[TimerCallbackValue]:
        node_dict = self._get_node_dict(node.node_name)

        if 'callbacks' not in node_dict:
            return []

        cbs_dict = node_dict.get_dicts('callbacks')
        cbs_dict = Util.filter_items(
            lambda x: x.get_value('callback_type') == str(CallbackType.TIMER),
            cbs_dict
        )
        node = self.get_node(node.node_name)

        callbacks = []
        for _, info in enumerate(cbs_dict):
            callback_name = info.get_value('callback_name')
            callback_id = info.get_value('callback_id')
            publish_topic_names = self._get_publish_topic_names(node.node_name, callback_id)
            node_name = node.node_name
            callbacks.append(
                TimerCallbackValue(
                    callback_id=callback_id,
                    node_name=node_name,
                    node_id=node.node_id,
                    symbol=info.get_value('symbol'),
                    period_ns=int(info.get_value('period_ns')),
                    publish_topic_names=tuple(publish_topic_names),
                    callback_name=callback_name
                )
            )
        return callbacks

    def _get_message_contexts(
        self,
        node: NodeValue
    ) -> List[Dict]:
        node_dict = self._get_node_dict(node.node_name)
        if 'message_contexts' not in node_dict:
            return []

        context_dicts = node_dict.get_dicts('message_contexts')
        for context in context_dicts:
            # Verify the existence of the required key.
            context.get_value_with_default('context_type', None)
            pub_topic_name = context.get_value('publisher_topic_name')
            sub_topic_name = context.get_value('subscription_topic_name')
            if pub_topic_name == '/tf':
                context.get_value('broadcast_frame_id')
                context.get_value('broadcast_child_frame_id')
            if sub_topic_name == '/tf':
                context.get_value('lookup_frame_id')
                context.get_value('lookup_child_frame_id')

        return [d.data for d in context_dicts]

    def _get_tf_buffer(
        self,
        node: NodeValue
    ) -> Optional[TransformBufferValue]:
        node_dict = self._get_node_dict(node.node_name)
        if 'tf_buffer' not in node_dict:
            return None

        tf_buffers = node_dict.get_dicts('tf_buffer')
        lookup_transforms = tuple(TransformValue(
            tf_buff.get_value('lookup_frame_id'),
            tf_buff.get_value('lookup_child_frame_id')
        ) for tf_buff in tf_buffers)
        listen_transforms = self.get_tf_frames()

        buff_val = TransformBufferValue(
            lookup_node_id=node_dict.get_value('node_id'),
            lookup_node_name=node.node_name,
            listener_node_id=None,
            listener_node_name=None,
            lookup_transforms=lookup_transforms,
            listen_transforms=tuple(listen_transforms)
        )
        return buff_val

    def _get_tf_broadcaster(
        self,
        node: NodeValue
    ) -> Optional[TransformBroadcasterValue]:
        node_dict = self._get_node_dict(node.node_name)
        if 'tf_broadcaster' not in node_dict:
            return None

        br_dict = node_dict.get_dict('tf_broadcaster')
        transforms = [
            TransformValue(
                tf_dict.get_value('frame_id'),
                tf_dict.get_value('child_frame_id'),
            )
            for tf_dict
            in br_dict.get_dicts('frames')
        ]
        tf_pub = PublisherValue(
                    topic_name='/tf',
                    node_name=node.node_name,
                    node_id=node.node_id,
                    callback_ids=tuple(br_dict.get_values('callback_ids')),
                )
        cb_ids = []
        if 'callback_ids' in br_dict.keys():
            cb_ids = br_dict.get_values('callback_ids')
            cb_ids = [cb_id for cb_id in cb_ids if cb_id is not None]

        br = TransformBroadcasterValue(tf_pub, tuple(transforms), tuple(cb_ids))
        return br

    def _get_callback_groups(
        self,
        node: NodeValue
    ) -> List[CallbackGroupValue]:
        node_dict = self._get_node_dict(node.node_name)
        if 'callback_groups' not in node_dict.keys():
            return []

        cbgs: List[CallbackGroupValue] = []
        for cbg_dict in node_dict.get_dicts('callback_groups'):
            cbg_name = cbg_dict.get_value('callback_group_name')
            cbg_id = cbg_dict.get_value('callback_group_id')
            cbg = CallbackGroupValue(
                callback_group_type_name=cbg_dict.get_value('callback_group_type'),
                node_name=node.node_name,
                node_id=node.node_name,
                callback_ids=tuple(cbg_dict.get_values('callback_ids')),
                callback_group_id=cbg_id,
                callback_group_name=cbg_name
            )
            cbgs.append(cbg)

        return cbgs

    def get_paths(self) -> List[PathValue]:
        aliases_info = self._arch_dict.get_dicts('named_paths')

        paths_info = []
        for alias in aliases_info:
            path_name = alias.get_value('path_name')

            node_chain: List[NodePathValue] = []
            for node_path_dict in alias.get_dicts('node_chain'):
                args: Dict[str, Optional[str]] = {}

                node_name = node_path_dict.get_value('node_name')
                args['publish_topic_name'] = \
                    node_path_dict.get_value_with_default(
                        'publish_topic_name', None)
                args['subscribe_topic_name'] = \
                    node_path_dict.get_value_with_default(
                        'subscribe_topic_name', None)
                args['broadcast_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'broadcast_frame_id', None)
                args['broadcast_child_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'broadcast_child_frame_id', None)
                args['buffer_listen_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'buffer_listen_frame_id', None)
                args['buffer_listen_child_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'buffer_listen_child_frame_id', None)
                args['buffer_lookup_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'buffer_lookup_frame_id', None)
                args['buffer_lookup_child_frame_id'] = \
                    node_path_dict.get_value_with_default(
                        'buffer_lookup_child_frame_id', None)

                node_path_value = NodePathValue(node_name, **args)
                node_chain.append(node_path_value)

            paths_info.append(
                PathValue(path_name, tuple(node_chain))
            )
        return paths_info

    def _get_variable_passings(
        self,
        node: NodeValue,
    ) -> List[VariablePassingValue]:
        node_dict = self._get_node_dict(node.node_name)

        if 'variable_passings' not in node_dict.keys():
            return []

        var_passes = []
        for val in node_dict.get_dicts('variable_passings'):
            write = val.get_value_with_default('callback_id_write', None)
            read = val.get_value_with_default('callback_id_read', None)
            if write is None or read is None:
                continue
            var_passes.append(
                VariablePassingValue(node.node_name, write, read)
            )
        return var_passes

    def get_executors(self) -> List[ExecutorValue]:
        executors = []
        for e in self._arch_dict.get_dicts('executors'):
            executor = ExecutorValue(
                executor_type_name=e.get_value('executor_type'),
                callback_group_ids=tuple(e.get_values('callback_group_ids')),
                executor_name=e.get_value('executor_name'),
                executor_id=e.get_value('executor_id'))
            executors.append(executor)
        return executors

    def _get_subscription_callbacks(
        self,
        node: NodeValue
    ) -> List[SubscriptionCallbackValue]:
        def is_target(x: YamlDict):
            return x.get_value('callback_type') == CallbackType.SUBSCRIPTION.type_name
        node_dict = self._get_node_dict(node.node_name)

        if 'callbacks' not in node_dict:
            return []

        callback_dicts = node_dict.get_dicts('callbacks')
        callback_dicts = Util.filter_items(is_target, callback_dicts)

        callbacks = []

        for val in callback_dicts:
            callback_name = val.get_value('callback_name')
            callback_id = val.get_value('callback_id')
            publish_topic_names = self._get_publish_topic_names(node.node_name, callback_id)
            callbacks.append(
                SubscriptionCallbackValue(
                    callback_id=callback_id,
                    node_id=node.node_id,
                    node_name=node.node_name,
                    symbol=val.get_value('symbol'),
                    subscribe_topic_name=val.get_value('topic_name'),
                    publish_topic_names=tuple(publish_topic_names),
                    callback_name=callback_name
                )
            )

        return callbacks

    def _get_publishers(
        self,
        node: NodeValue
    ) -> List[PublisherValue]:
        node_dict = self._get_node_dict(node.node_name)
        publishers = []

        if 'publishes' not in node_dict:
            return []

        for pub in node_dict.get_dicts('publishes'):
            publishers.append(
                PublisherValue(
                    topic_name=pub.get_value('topic_name'),
                    node_name=node.node_name,
                    node_id=node.node_id,
                    callback_ids=tuple(pub.get_values('callback_ids')),
                )
            )
        return publishers

    def _get_timers(
        self,
        node: NodeValue
    ) -> List[TimerValue]:
        node_dict = self._get_node_dict(node.node_name)
        timers = []

        if 'callbacks' not in node_dict.keys():
            return []
        cbs_dict = node_dict.get_dicts('callbacks')

        for cb_dict in cbs_dict:
            if cb_dict.get_value('callback_type') != 'timer_callback':
                continue
            timers.append(
                TimerValue(
                    period=int(cb_dict.get_value('period_ns')),
                    node_name=node.node_name,
                    node_id=node.node_id,
                    callback_id=cb_dict.get_value('callback_id'),
                )
            )
        return timers

    def _get_subscriptions(
        self,
        node: NodeValue
    ) -> List[SubscriptionValue]:
        node_dict = self._get_node_dict(node.node_name)
        subscriptions = []

        if 'subscribes' not in node_dict.keys():
            return []

        for pub in node_dict.get_dicts('subscribes'):
            subscriptions.append(
                SubscriptionValue(
                    node_name=node.node_name,
                    node_id=node.node_name,
                    topic_name=pub.get_value('topic_name'),
                    callback_id=pub.get_value_with_default('callback_id', None),
                )
            )
        return subscriptions

    def _get_node_dict(
        self,
        node_name: str
    ) -> YamlDict:
        node_dicts = self._arch_dict.get_dicts('nodes')
        nodes = Util.filter_items(lambda x: x.get_value('node_name') == node_name, node_dicts)

        if len(nodes) == 0:
            message = f'Failed to find node by node_name. target node name = {node_name}'
            raise InvalidYamlFormatError(message)

        if len(nodes) >= 2:
            message = (
                'Failed to specify node by node_name. same node_name are exist.'
                f'target node name = {node_name}')
            raise InvalidYamlFormatError(message)

        return YamlDict(nodes[0])
