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

from typing import Callable, Collection, Dict, List, Optional

from ..exceptions import InvalidArgumentError, UnsupportedTypeError
from ..value_objects import (
    CallbackGroupStructValue,
    CallbackStructValue,
    ExecutorStructValue,
    NodePathStructValue,
    NodeStructValue,
    PathStructValue,
    PublisherStructValue,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    TimerCallbackStructValue,
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)


class ArchitectureDict():

    def __init__(
        self,
        node_values: Collection[NodeStructValue],
        executor_values: Collection[ExecutorStructValue],
        named_path_values: Collection[PathStructValue],
    ) -> None:
        self._named_path_values = named_path_values
        self._executor_values = executor_values
        self._node_values = node_values

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
            node_path_dict = {
                'node_name': node_path.node_name,
                'publish_topic_name': node_path.publish_topic_name,
                'subscribe_topic_name': node_path.subscribe_topic_name
            }
            tf_br_dict = TfFrameBroadcasterDict(node_path.tf_frame_broadcaster)
            node_path_dict.update(tf_br_dict.data)

            tf_buff_dict = TfFrameBufferDict(node_path.tf_frame_buffer)
            node_path_dict.update(tf_buff_dict.data)

            node_chain.append(node_path_dict)
        obj['node_chain'] = node_chain
        return obj


class TfFrameBroadcasterDict:

    def __init__(
        self,
        tf_frame_broadcaster: Optional[TransformFrameBroadcasterStructValue]
    ) -> None:
        self._data = tf_frame_broadcaster

    @property
    def data(self) -> Dict:
        if self._data is None:
            return {}
        return {
            'broadcast_frame_id': self._data.frame_id,
            'broadcast_child_frame_id': self._data.child_frame_id
        }


class TfFrameBufferDict:

    def __init__(
        self,
        tf_frame_buffer: Optional[TransformFrameBufferStructValue]
    ):
        self._data = tf_frame_buffer

    @property
    def data(self) -> Dict:
        if self._data is None:
            return {}
        return {
            'buffer_listen_frame_id': self._data.listen_frame_id,
            'buffer_listen_child_frame_id': self._data.listen_child_frame_id,
            'buffer_lookup_source_frame_id': self._data.lookup_source_frame_id,
            'buffer_lookup_target_frame_id': self._data.lookup_target_frame_id,
        }


class CallbackDicts:
    def __init__(
        self,
        callback_values: Collection[CallbackStructValue]
    ) -> None:
        callbacks_dicts = [self._cb_to_dict(c) for c in callback_values]
        self._data = sorted(callbacks_dicts, key=lambda x: x['callback_name'])

    def _timer_cb_to_dict(
        self,
        timer_callback: TimerCallbackStructValue
    ) -> Dict:
        return  \
            {
                'callback_id': timer_callback.callback_id,
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
            'callback_id': subscription_callback.callback_id,
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
        var_pass_values: Optional[Collection[VariablePassingStructValue]]
    ) -> None:
        self._data: List[Dict] = []

        if var_pass_values is None:
            self._data = [self._undefined_dict]
            return None

        for var_pass in var_pass_values:
            self._data.append(
                {
                    'callback_id_write': var_pass.callback_id_write,
                    'callback_id_read': var_pass.callback_id_read,
                }
            )

        if len(self._data) == 0:
            self._data.append(self._undefined_dict)

        return None

    @property
    def _undefined_dict(self) -> Dict:
        return \
            {
                'callback_id_write': None,
                'callback_id_read': None,
            }

    @property
    def data(self) -> List[Dict]:
        return self._data


class PubDicts:

    def __init__(
        self,
        pubishers: Collection[PublisherStructValue]
    ) -> None:
        dicts = []
        for publisher in pubishers:
            if publisher.topic_name.endswith('/info/pub'):
                continue  # skip tilde createdtopic
            if publisher.topic_name in ['/clock', '/rosout', '/parameter_events']:
                continue  # skip
            dicts.append(self._to_dict(publisher))
        self._data = sorted(dicts, key=lambda x: x['topic_name'])

    @staticmethod
    def _to_dict(publisher: PublisherStructValue):

        callback_ids: List[Optional[str]]
        if isinstance(publisher, PublisherStructValue) \
                and publisher.callback_ids is not None:
            callback_ids = list(publisher.callback_ids)
        else:
            callback_ids = [None]

        return {
            'topic_name': publisher.topic_name,
            'callback_ids': callback_ids,
        }

    @property
    def data(self) -> List[Dict]:
        return self._data


class SubDicts:

    def __init__(
        self,
        subscriptions: Collection[SubscriptionStructValue]
    ) -> None:
        dicts = [self._to_dict(s) for s in subscriptions]
        self._data = sorted(dicts, key=lambda x: x['topic_name'])

    def _to_dict(
        self,
        subscription: SubscriptionStructValue
    ) -> Dict:
        return {
            'topic_name': subscription.topic_name,
            'callback_id': subscription.callback_id
        }

    @property
    def data(self) -> List[Dict]:
        return self._data


class CbgsDicts:

    def __init__(
        self,
        cbgs: Collection[CallbackGroupStructValue]
    ) -> None:
        self._data = cbgs

    @property
    def data(self) -> List[Dict]:
        return [{
            'callback_group_id': cbg.callback_group_id,
            'callback_group_type': cbg.callback_group_type_name,
            'callback_group_name': cbg.callback_group_name,
            'callback_ids': sorted(cbg.callback_ids)
        } for cbg in self._data]


class NodesDicts:

    def __init__(
        self,
        node_values: Collection[NodeStructValue],
    ) -> None:
        nodes_dicts = [
            self._to_dict(n)
            for n
            in node_values
            if self._is_ignored(n)
        ]
        self._data = sorted(nodes_dicts, key=lambda x: x['node_name'])

    @property
    def data(self) -> Collection[Dict]:
        return self._data

    @staticmethod
    def _is_ignored(node: NodeStructValue) -> bool:
        if 'transform_listener_impl' in node.node_name:
            return False
        if 'rviz2' in node.node_name:
            return False
        return True

    def _to_dict(
        self,
        node: NodeStructValue,
    ) -> Dict:
        obj: Dict = {}
        obj['node_id'] = f'{node.node_id}'
        obj['node_name'] = f'{node.node_name}'

        if node.callback_groups is not None:
            obj['callback_groups'] = CbgsDicts(node.callback_groups).data

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

        if len(node.paths) > 0:
            obj['message_contexts'] = MessageContextDicts(node.paths).data

        if node.tf_broadcaster is not None:
            obj['tf_broadcaster'] = TfBroadcasterDicts(
                node.tf_broadcaster).data

        if node.tf_buffer is not None:
            obj['tf_buffer'] = TfBufferDicts(node.tf_buffer).data

        return obj


class MessageContextDicts:
    def __init__(
        self,
        paths: Collection[NodePathStructValue],
    ) -> None:
        self._data = []
        for path in paths:
            if path.publish_topic_name is None or path.subscribe_topic_name is None:
                continue

            if path.publish_topic_name == '/tf' and path.tf_frame_broadcaster is None:
                continue
            message_context = path.message_context
            if message_context is not None:
                self._data.append(message_context.to_dict())

    @property
    def data(self) -> List[Dict]:
        return self._data


class TfBroadcasterDicts:
    def __init__(
        self,
        broadcaster: TransformBroadcasterStructValue,
    ) -> None:
        frames = []
        for transform in broadcaster.transforms:
            frames.append(
                {
                    'frame_id': transform.source_frame_id,
                    'child_frame_id': transform.target_frame_id,
                }
            )
        cb_ids: List[Optional[str]] = []
        if broadcaster.callback_ids is not None:
            cb_ids = [str(cb_id) for cb_id in broadcaster.callback_ids]
        cb_ids = cb_ids if len(cb_ids) > 0 else [None]
        self._data = {
            'frames': frames,
            'callback_ids': cb_ids
        }

    @property
    def data(self) -> Dict:
        return self._data


class TfBufferDicts:
    def __init__(
        self,
        buffer: TransformBufferStructValue,
    ) -> None:
        self._data = []
        if buffer.lookup_transforms is not None:
            for transform in buffer.lookup_transforms:
                self._data.append(
                    {
                        'lookup_source_frame_id': transform.source_frame_id,
                        'lookup_target_frame_id': transform.target_frame_id,
                    }
                )

    @property
    def data(self) -> List[Dict]:
        return self._data


class ExecutorsDicts:

    def __init__(
        self,
        executor_values: List[ExecutorStructValue],
        is_ignored: Optional[Callable[[ExecutorStructValue], bool]] = None,
    ) -> None:
        is_ignored = is_ignored or self._is_ignored
        exec_dicts = [self._to_dict(e)
                      for e in executor_values if not is_ignored(e)]
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
            'executor_id': executor_value.executor_id,
            'executor_type': executor_value.executor_type_name,
            'executor_name': executor_value.executor_name,
            'callback_group_ids': [
                cbg.callback_group_id
                for cbg
                in cbgs
            ]
        }
        return obj

    @staticmethod
    def _is_ignored(executor: ExecutorStructValue) -> bool:
        if len(executor.callback_groups) == 1:
            cbg = executor.callback_groups[0]
            if 'transform_listener_impl' in cbg.node_name:
                return True
        return False
