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

from typing import List, Dict, Any, Optional

from trace_analysis.node import Node
from trace_analysis.callback import CallbackBase, TimerCallback, SubscriptionCallback
from trace_analysis.communication import Communication
from trace_analysis.pub_sub import Publisher, Subscription
from trace_analysis.record.interface import LatencyComposer
from trace_analysis.architecture.interface import (
    ArchitectureInterface,
    ArchitectureExporter,
    ArchitectureImporter,
    PathAlias,
    VariablePassing,
    UNDEFINED_STR,
)
from trace_analysis.util import Util

YamlObject = Dict[str, Any]


class YamlArchitectureExporter(ArchitectureExporter):
    def exec(self, architecture: ArchitectureInterface, file_path: str) -> None:
        import yaml

        with open(file_path, mode="w") as f:
            obj: Dict[str, Any] = {
                "path_name_alias": self._aliases_to_yaml_objs(architecture.path_aliases),
                "nodes": self._nodes_to_yaml_objs(architecture.nodes),
            }
            f.write(yaml.dump(obj, indent=2, default_flow_style=False, sort_keys=False))

    def _aliases_to_yaml_objs(self, aliases: List[PathAlias]) -> List[YamlObject]:
        objs: List[YamlObject] = []
        # TODO(hsgwa): implement here
        # for path in aliases:
        #     objs.append({"path_name": path.alias, "callbacks": path.callback_names})

        # if len(objs) == 0:
        #     objs.append({"path_name": UNDEFINED_STR, "callbacks": []})

        return objs

    def _nodes_to_yaml_objs(self, nodes: List[Node]) -> List[YamlObject]:
        objs: List[YamlObject] = []
        for node in nodes:
            obj: YamlObject = {"node_name": f"{node.node_name}"}

            if len(node.callbacks) >= 1:
                obj["callbacks"] = self._callbacks_to_yaml_objs(node.callbacks)
            if len(node.callbacks) >= 2:
                obj["callback_dependencies"] = self._variable_passings_to_yaml_objs(
                    node.variable_passings
                )
            if len(node.publishes) >= 1:
                obj["publish"] = self._pubs_to_yaml_objs(node, node.publishes)

            objs.append(obj)

        return objs

    def _variable_passings_to_yaml_objs(
        self, variable_passings: List[VariablePassing]
    ) -> List[YamlObject]:
        objs: List[YamlObject] = []
        for variable_passing in variable_passings:
            objs.append(
                {
                    "callback_name_from": variable_passing.callback_write.callback_name,
                    "callback_name_to": variable_passing.callback_read.callback_name,
                }
            )

        if len(objs) == 0:
            objs.append(
                {
                    "callback_name_from": UNDEFINED_STR,
                    "callback_name_to": UNDEFINED_STR,
                }
            )
        return objs

    def _get_publish_callback(self, node: Node, publish: Publisher) -> Optional[CallbackBase]:
        for callback in node.callbacks:
            if publish in callback.publishes:
                return callback
        return None

    def _pubs_to_yaml_objs(self, node: Node, pubs: List[Publisher]) -> List[YamlObject]:
        objs: List[YamlObject] = []
        for pub in pubs:
            obj: Dict[str, Any] = {"topic_name": pub.topic_name, "callback_name": None}

            cb = self._get_publish_callback(node, pub)
            if cb is None:
                obj["callback_name"] = UNDEFINED_STR
            else:
                obj["callback_name"] = cb.callback_name
            objs.append(obj)

        return objs

    def _timer_cb_to_yaml_obj(self, timer_callback: TimerCallback) -> YamlObject:
        return {
            "callback_name": timer_callback.callback_name,
            "type": "timer_callback",
            "period_ns": timer_callback.period_ns,
            "symbol": timer_callback.symbol,
        }

    def _sub_cb_to_yaml_obj(self, subscription_callback: SubscriptionCallback) -> YamlObject:
        return {
            "callback_name": subscription_callback.callback_name,
            "type": "subscription_callback",
            "topic_name": subscription_callback.topic_name,
            "symbol": subscription_callback.symbol,
        }

    def _callbacks_to_yaml_objs(self, callbacks: List[CallbackBase]) -> List[YamlObject]:
        objs: List[YamlObject] = []
        for callback in callbacks:
            if isinstance(callback, TimerCallback):
                objs.append(self._timer_cb_to_yaml_obj(callback))
            elif isinstance(callback, SubscriptionCallback):
                objs.append(self._sub_cb_to_yaml_obj(callback))

        return objs


class YamlArchitectureImporter(ArchitectureImporter):
    def __init__(self, latency_composer: Optional[LatencyComposer]):
        self._latency_composer: Optional[LatencyComposer] = latency_composer
        self._nodes: List[Node] = []
        self._path_aliases: List[PathAlias] = []
        self._communications: List[Communication] = []
        self._variable_passings: List[VariablePassing] = []

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
        return self._variable_passings

    @property
    def latency_composer(self):
        return self._latency_composer

    def _create_communicateion(
        self, publish: Publisher, subscription: Subscription
    ) -> Optional[Communication]:

        publish_callback_name = publish.callback_name or ""
        callbacks = Util.flatten([node.callbacks for node in self._nodes])
        callback_publish = Util.find_one(
            callbacks,
            lambda cb: cb.node_name == publish.node_name
            and cb.callback_name == publish_callback_name,
        )
        callback_subscription = Util.find_one(
            callbacks,
            lambda cb: cb.node_name == subscription.node_name
            and cb.callback_name == subscription.callback_name,
        )
        if callback_publish is None:
            print('f"callback_name is UNDEFINED. Ignoring {publish.topic_name} topic."')
            return None
        if callback_subscription is None or not isinstance(
            callback_subscription, SubscriptionCallback
        ):
            return None

        return Communication(self._latency_composer, callback_publish, callback_subscription)

    def exec(self, path: str, ignore_topics: Optional[List[str]] = None) -> None:
        ignore_topics = ignore_topics or []
        import yaml
        import itertools

        with open(path, "r") as f:
            arch_yaml = yaml.safe_load(f)

        for node_info in arch_yaml["nodes"]:
            self._nodes.append(self._create_node(node_info))

        publishes = Util.flatten([node.publishes for node in self._nodes])
        subscriptions = Util.flatten([node.subscriptions for node in self._nodes])
        for publish, subscription in itertools.product(publishes, subscriptions):
            if publish.topic_name != subscription.topic_name:
                continue
            communication = self._create_communicateion(publish, subscription)
            if communication is None:
                continue
            self._communications.append(communication)

        for alias_dict in arch_yaml["path_name_alias"]:
            self._path_aliases.append(
                PathAlias(alias=alias_dict["path_name"], callbacks=alias_dict["callbacks"])
            )

        return None

    def _create_callback_with_empty_publish(
        self, node_name: str, callback_info
    ) -> Optional[CallbackBase]:
        ignore_topic_name = ["/rosout", "/parameter_events"]

        if callback_info["type"] == "timer_callback":
            return TimerCallback(
                latency_composer=self._latency_composer,
                node_name=node_name,
                callback_name=callback_info["callback_name"],
                symbol=callback_info["symbol"],
                period_ns=int(callback_info["period_ns"]),
            )
        elif callback_info["type"] == "subscription_callback":
            if callback_info["topic_name"] in ignore_topic_name:
                return None
            return SubscriptionCallback(
                latency_composer=self._latency_composer,
                node_name=node_name,
                callback_name=callback_info["callback_name"],
                symbol=callback_info["symbol"],
                topic_name=callback_info["topic_name"],
            )
        return None

    def _attach_publish_to_callback(self, node, publish_info) -> None:
        callback = Util.find_one(
            node.callbacks,
            lambda callback: callback.callback_name == publish_info["callback_name"],
        )

        if callback is not None:
            publish = Publisher(node.node_name, publish_info["topic_name"], callback.callback_name)
            callback.publishes.append(publish)
            return None

        if len(node.callbacks) == 1:
            callback = node.callbacks[0]
            publish = Publisher(node.node_name, publish_info["topic_name"], callback.callback_name)
            callback.publishes.append(publish)
            return None

        publish = Publisher(node.node_name, publish_info["topic_name"], None)
        node.unlinked_publishes.append(publish)
        return None

    def _create_node(self, node_info):
        node = Node(node_name=node_info["node_name"])

        callbacks_info = node_info.get("callbacks", [])
        for callback_info in callbacks_info:
            callback = self._create_callback_with_empty_publish(node.node_name, callback_info)
            if callback is None:
                continue
            node.callbacks.append(callback)

        callback_dependencies = node_info.get("callback_dependencies", [])
        for depend in callback_dependencies:
            callback_from = Util.find_one(
                node.callbacks, lambda cb: cb.callback_name == depend["callback_name_from"]
            )
            callback_to = Util.find_one(
                node.callbacks, lambda cb: cb.callback_name == depend["callback_name_to"]
            )

            if callback_from is None or callback_to is None:
                continue

            node.variable_passings.append(
                VariablePassing(
                    self._latency_composer,
                    callback_write=callback_from,
                    callback_read=callback_to,
                )
            )

        publishes_info = node_info.get("publish", [])
        for publish_info in publishes_info:
            self._attach_publish_to_callback(node, publish_info)

        return node
