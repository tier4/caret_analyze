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

from itertools import product
from typing import Dict, List, Optional

from caret_analyze.record import RecordsContainer
from caret_analyze.util import Util

from .interface import ArchitectureReader, IGNORE_TOPICS, PathAlias, UNDEFINED_STR
from ..callback import CallbackBase, SubscriptionCallback, TimerCallback
from ..communication import Communication, VariablePassing
from ..node import Node
from ..value_objects.publisher import Publisher
from ..value_objects.subscription import Subscription


class Architecture():
    def __init__(
        self,
        reader: ArchitectureReader,
        records_container: Optional[RecordsContainer],
        ignore_topics: List[str] = IGNORE_TOPICS,
    ):
        importer = ArchitectureImporter(reader, records_container, ignore_topics)

        self._nodes: List[Node] = importer.nodes
        self._path_aliases: List[PathAlias] = importer.path_aliases
        self._communications: List[Communication] = importer.communications
        self._records_container = records_container

    def add_path_alias(self, path_name: str, callbacks: List[CallbackBase]):
        assert path_name not in [
            alias.path_name for alias in self._path_aliases]
        callback_names = [callback.callback_unique_name for callback in callbacks]
        alias = PathAlias(path_name, callback_names)
        self._path_aliases.append(alias)

    def has_path_alias(self, path_name: str):
        return path_name in [alias.path_name for alias in self._path_aliases]

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    @property
    def path_aliases(self) -> List[PathAlias]:
        return self._path_aliases

    @property
    def communications(self) -> List[Communication]:
        return self._communications

    @property
    def variable_passings(self) -> List[VariablePassing]:
        return Util.flatten([node.variable_passings for node in self._nodes])

    def export(self, file_path: str):
        exporter = ArchitectureExporter(self)
        exporter.execute(file_path)


class ArchitectureImporter():
    def __init__(
        self,
        reader: ArchitectureReader,
        records_container: Optional[RecordsContainer],
        ignore_topics: List[str]
    ) -> None:
        self.path_aliases = reader.get_path_aliases()
        self._records_container = records_container

        self.nodes = []
        for name in reader.get_node_names():
            self.nodes.append(self._create_node(name, reader, ignore_topics))

        self.communications = []
        publishes = Util.flatten([node.publishes for node in self.nodes])
        subscriptions = Util.flatten([node.subscriptions for node in self.nodes])

        for publish, subscription in product(publishes, subscriptions):
            if publish.topic_name != subscription.topic_name:
                continue
            communication = self._create_communicateion(publish, subscription)
            if communication is None:
                continue
            self.communications.append(communication)

        return None

    def _create_communicateion(
        self,
        publish: Publisher,
        subscription: Subscription
    ) -> Optional[Communication]:
        publish_callback_name = publish.callback_name or ''

        callback_publish = self._find_callback(
            publish.node_name, publish_callback_name)

        callback_subscription = self._find_callback(
            subscription.node_name, subscription.callback_name
        )

        if callback_subscription is None:
            return None

        if not isinstance(callback_subscription, SubscriptionCallback):
            return None

        return Communication(
            self._records_container, callback_publish, callback_subscription, publish
        )

    def _create_node(
        self,
        node_name,
        reader: ArchitectureReader,
        ignore_topics: List[str]
    ) -> Node:

        node: Node = Node(node_name)

        for subscription_callback in reader.get_subscription_callbacks(node_name):
            if subscription_callback.topic_name in ignore_topics:
                continue
            node.callbacks.append(
                SubscriptionCallback(
                    self._records_container,
                    node_name,
                    subscription_callback.callback_name,
                    subscription_callback.symbol,
                    subscription_callback.topic_name
                ))

        for timer_callback in reader.get_timer_callbacks(node_name):
            node.callbacks.append(
                TimerCallback(
                    self._records_container,
                    timer_callback.node_name,
                    timer_callback.callback_name,
                    timer_callback.symbol,
                    timer_callback.period_ns
                ))

        for publisher in reader.get_publishers(node_name):
            if publisher.topic_name in ignore_topics:
                continue

            if publisher.callback_name not in [None, UNDEFINED_STR]:
                callback = Util.find_one(
                    node.callbacks,
                    lambda x: x.callback_name == publisher.callback_name)
                callback.publishes.append(publisher)
                continue

            if len(node.callbacks) == 1:
                # automatically assign if there is only one callback.
                callback = node.callbacks[0]
                publisher = Publisher(
                    publisher.node_name, publisher.topic_name, callback.callback_name
                )
                callback.publishes.append(publisher)
                continue

            node.unlinked_publishes.append(publisher)

        variable_passings = reader.get_variable_passings(node_name)
        for var_pass in variable_passings:
            if UNDEFINED_STR in [var_pass.write_callback_name, var_pass.read_callback_name]:
                continue

            callback_write = Util.find_one(
                node.callbacks, lambda cb: cb.callback_name == var_pass.write_callback_name
            )
            callback_read = Util.find_one(
                node.callbacks, lambda cb: cb.callback_name == var_pass.read_callback_name
            )

            node.variable_passings.append(
                VariablePassing(
                    self._records_container,
                    callback_write=callback_write,
                    callback_read=callback_read,
                )
            )

        return node

    def _find_callback(
        self,
        node_name: str,
        callback_name: str
    ) -> Optional[CallbackBase]:
        for node in self.nodes:
            for callback in node.callbacks:
                if callback.node_name == node_name and callback.callback_name == callback_name:
                    return callback
        return None


class ArchitectureExporter():
    def __init__(
        self,
        architecture: Architecture,
    ) -> None:
        self._arch = architecture

    def execute(self, file_path: str) -> None:
        with open(file_path, mode='w') as f:
            f.write(self.get_str())

    def get_str(self) -> str:
        import yaml

        aliases = [self._path_alias_to_yaml_obj(
            alias) for alias in self._arch.path_aliases]

        obj: Dict = {
            'path_name_aliases': aliases,
            'nodes': self._nodes_to_yaml_objs(self._arch.nodes)
        }

        return yaml.dump(obj, indent=2, default_flow_style=False, sort_keys=False)

    def _path_alias_to_yaml_obj(self, alias: PathAlias):
        obj: Dict = {}
        obj['path_name'] = alias.path_name
        obj['callbacks'] = alias.callback_unique_names
        return obj

    def _nodes_to_yaml_objs(self, nodes: List[Node]) -> List[Dict]:
        objs: List[Dict] = []
        for node in nodes:
            obj: Dict = {'node_name': f'{node.node_name}'}

            if len(node.callbacks) >= 1:
                obj['callbacks'] = self._callbacks_to_yaml_objs(node.callbacks)
            if len(node.callbacks) >= 2:
                obj['variable_passings'] = self._variable_passings_to_yaml_objs(
                    node.variable_passings
                )
            if len(node.publishes) >= 1:
                obj['publishes'] = self._pubs_to_yaml_objs(
                    node, node.publishes)

            objs.append(obj)

        return objs

    def _variable_passings_to_yaml_objs(
        self,
        variable_passings: List[VariablePassing]
    ) -> List[Dict]:
        objs: List[Dict] = []
        for variable_passing in variable_passings:
            objs.append(
                {
                    'callback_name_write': variable_passing.callback_write.callback_name,
                    'callback_name_read': variable_passing.callback_read.callback_name,
                }
            )

        if len(objs) == 0:
            objs.append(
                {
                    'callback_name_write': UNDEFINED_STR,
                    'callback_name_read': UNDEFINED_STR,
                }
            )
        return objs

    def _get_publish_callback(self, node: Node, publish: Publisher) -> Optional[CallbackBase]:
        for callback in node.callbacks:
            if publish in callback.publishes:
                return callback
        return None

    def _pubs_to_yaml_objs(self, node: Node, pubs: List[Publisher]) -> List[Dict]:
        objs: List[Dict] = []
        for pub in pubs:
            obj = {'topic_name': pub.topic_name, 'callback_name': None}

            cb = self._get_publish_callback(node, pub)
            if cb is None:
                obj['callback_name'] = UNDEFINED_STR
            else:
                obj['callback_name'] = cb.callback_name
            objs.append(obj)

        return objs

    def _timer_cb_to_yaml_obj(self, timer_callback: TimerCallback) -> Dict:
        return {
            'callback_name': timer_callback.callback_name,
            'type': 'timer_callback',
            'period_ns': timer_callback.period_ns,
            'symbol': timer_callback.symbol,
        }

    def _sub_cb_to_yaml_obj(self, subscription_callback: SubscriptionCallback) -> Dict:
        return {
            'callback_name': subscription_callback.callback_name,
            'type': 'subscription_callback',
            'topic_name': subscription_callback.topic_name,
            'symbol': subscription_callback.symbol,
        }

    def _callbacks_to_yaml_objs(self, callbacks: List[CallbackBase]) -> List[Dict]:
        objs: List[Dict] = []
        for callback in callbacks:
            if isinstance(callback, TimerCallback):
                objs.append(self._timer_cb_to_yaml_obj(callback))
            elif isinstance(callback, SubscriptionCallback):
                objs.append(self._sub_cb_to_yaml_obj(callback))

        return objs
