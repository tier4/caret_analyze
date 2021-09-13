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

from typing import List

from caret_analyze.pub_sub import Publisher, Subscription
from caret_analyze.callback import (
    CallbackBase,
    TimerCallback,
    SubscriptionCallback,
)

# from caret_analyze.callback_depend import CallbackDepend, CallbackDependCollection
from caret_analyze.node import Node
from caret_analyze.communication import VariablePassing
from caret_analyze.architecture.yaml import YamlArchitectureExporter, YamlArchitectureImporter
from caret_analyze.architecture.interface import UNDEFINED_STR, IGNORE_TOPICS


class TestYamlExporter:
    def test_pub_to_yml_obj(self):
        exporter = YamlArchitectureExporter()
        pubs: List[Publisher] = []
        node = Node("node")
        publisher = Publisher(node.node_name, "/topic", None)
        pubs.append(publisher)
        callback_name = SubscriptionCallback.to_callback_name(0)

        objs = exporter._pubs_to_yaml_objs(node, pubs)
        assert len(objs) == 1
        assert objs[0] == {"topic_name": publisher.topic_name, "callback_name": UNDEFINED_STR}

        cb = SubscriptionCallback(None, node.node_name, callback_name, "symbol0", "/topic0")
        cb.publishes.append(publisher)
        node.callbacks.append(cb)

        objs = exporter._pubs_to_yaml_objs(node, pubs)
        assert len(objs) == 1
        assert objs[0] == {"topic_name": publisher.topic_name, "callback_name": callback_name}

    def test_timer_cb_to_yml_obj(self):
        exporter = YamlArchitectureExporter()

        symbol_name = "symbol0"
        callback_name = TimerCallback.to_callback_name(0)
        node_name = "node0"
        period_ns = 100
        cb = TimerCallback(None, node_name, callback_name, symbol_name, period_ns)
        obj = exporter._timer_cb_to_yaml_obj(cb)
        obj_expect = {
            "callback_name": callback_name,
            "type": TimerCallback.TYPE_NAME,
            "period_ns": period_ns,
            "symbol": symbol_name,
        }
        assert obj == obj_expect

    def test_subscription_cb_to_yml_obj(self):
        exporter = YamlArchitectureExporter()
        symbol_name = "symbol0"
        callback_name = SubscriptionCallback.to_callback_name(0)
        node_name = "node0"
        topic_name = "/topic"
        cb = SubscriptionCallback(None, node_name, callback_name, symbol_name, topic_name)
        obj = exporter._sub_cb_to_yaml_obj(cb)
        obj_expect = {
            "callback_name": callback_name,
            "type": "subscription_callback",
            "topic_name": topic_name,
            "symbol": symbol_name,
        }
        assert obj == obj_expect

    def test_create_node(self):
        exporter = YamlArchitectureExporter()
        symbol_name = "symbol0"
        callback_name = SubscriptionCallback.to_callback_name(0)
        topic_name = "/topic"
        node_name = "node0"
        cb = SubscriptionCallback(None, node_name, callback_name, symbol_name, topic_name)
        obj = exporter._sub_cb_to_yaml_obj(cb)
        obj_expect = {
            "callback_name": callback_name,
            "type": SubscriptionCallback.TYPE_NAME,
            "topic_name": topic_name,
            "symbol": symbol_name,
        }
        assert obj == obj_expect

    def test_callbacks_to_yml_obj(self):
        exporter = YamlArchitectureExporter()

        cbs: List[CallbackBase] = []

        objs = exporter._callbacks_to_yaml_objs(cbs)
        assert len(objs) == 0

        cbs.clear()
        cbs.append(TimerCallback(None, "", "", "", 0))
        objs = exporter._callbacks_to_yaml_objs(cbs)
        assert len(objs) == 1
        assert objs[0]["type"] == TimerCallback.TYPE_NAME

        cbs.clear()
        sub = Subscription("", "", "")
        cbs.append(SubscriptionCallback(None, "", "", "", sub))
        objs = exporter._callbacks_to_yaml_objs(cbs)
        assert len(objs) == 1
        assert objs[0]["type"] == SubscriptionCallback.TYPE_NAME

    def test_variable_passings_to_yml_objs(self):
        exporter = YamlArchitectureExporter()

        passings: List[VariablePassing] = []
        objs = exporter._variable_passings_to_yaml_objs(passings)
        assert len(objs) == 1
        assert objs[0] == {
            "callback_name_write": UNDEFINED_STR,
            "callback_name_read": UNDEFINED_STR,
        }
        callback_name0 = SubscriptionCallback.to_callback_name(0)
        callback_name1 = SubscriptionCallback.to_callback_name(1)

        passing = VariablePassing(
            None,
            callback_write=SubscriptionCallback(
                None, "node0", callback_name0, "symbol0", "/topic0"
            ),
            callback_read=SubscriptionCallback(
                None, "node1", callback_name1, "symbol1", "/topic1"
            ),
        )
        passings.append(passing)

        objs = exporter._variable_passings_to_yaml_objs(passings)
        assert len(objs) == 1
        assert objs[0] == {
            "callback_name_write": callback_name0,
            "callback_name_read": callback_name1,
        }

    def test_nodes_to_yml_objs(self):
        exporter = YamlArchitectureExporter()

        nodes: List[Node] = []
        objs = exporter._nodes_to_yaml_objs(nodes)
        assert len(objs) == 0

        nodes.append(Node("name1"))
        objs = exporter._nodes_to_yaml_objs(nodes)
        assert len(objs) == 1
        assert "variable_passings" not in objs[0].keys()
        assert "publishes" not in objs[0].keys()

        node = Node("name2")
        node.callbacks.append(
            SubscriptionCallback(
                None,
                node.node_name,
                "callback1",
                "symbol1",
                "/topic1",
                [Publisher(node.node_name, "/topic_name1", None)],
            )
        )

        node.callbacks.append(
            SubscriptionCallback(None, node.node_name, "callback2", "symbol2", "/topic2")
        )
        nodes.append(node)
        objs = exporter._nodes_to_yaml_objs(nodes)
        assert len(objs) == 2
        assert "variable_passings" in objs[1].keys()
        assert "publishes" in objs[1].keys()
        assert "publishes" in objs[1].keys()


class TestYamlImporter:
    def test_create_callback_with_empty_publish(self):
        importer = YamlArchitectureImporter(None)

        topic_name = "topic0"
        callback_name = SubscriptionCallback.to_callback_name(0)
        symbol = "symbol0"
        callback_obj = {
            "callback_name": callback_name,
            "type": SubscriptionCallback.TYPE_NAME,
            "topic_name": topic_name,
            "symbol": symbol,
        }
        node_name = "node0"
        callback = importer._create_callback_with_empty_publish(
            node_name, callback_obj, IGNORE_TOPICS
        )
        assert callback is not None
        assert isinstance(callback, SubscriptionCallback)
        assert callback.topic_name == topic_name
        assert callback.callback_name == callback_name
        assert callback.symbol == symbol
        assert callback.publishes == []

        callback_obj["topic_name"] = IGNORE_TOPICS[0]
        callback = importer._create_callback_with_empty_publish(
            node_name, callback_obj, IGNORE_TOPICS
        )
        assert callback is None

        period_ns = 100
        callback_name = TimerCallback.to_callback_name(0)
        symbol = "symbol0"
        callback_obj = {
            "callback_name": callback_name,
            "type": TimerCallback.TYPE_NAME,
            "period_ns": str(period_ns),
            "topic_name": topic_name,
            "symbol": symbol,
        }
        node_name = "node0"
        callback = importer._create_callback_with_empty_publish(
            node_name, callback_obj, IGNORE_TOPICS
        )
        assert callback is not None
        assert isinstance(callback, TimerCallback)
        assert callback.period_ns == period_ns
        assert callback.callback_name == callback_name
        assert callback.symbol == symbol
        assert callback.publishes == []

    def test_attach_publish_to_callback(self):
        importer = YamlArchitectureImporter(None)

        publish_info = {
            "topic_name": "/topic0",
            "callback_name": UNDEFINED_STR,
        }

        node = Node("node0")

        node.callbacks.append(
            SubscriptionCallback(None, "node0", "callback0", "symbol0", "/topic0")
        )
        importer._attach_publish_to_callback(node, publish_info)
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks[0].publishes) == 1

        node = Node("node0")
        node.callbacks.append(
            SubscriptionCallback(None, "node0", "callback0", "symbol0", "/topic0")
        )
        node.callbacks.append(
            SubscriptionCallback(None, "node1", "callback1", "symbol1", "/topic1")
        )
        importer._attach_publish_to_callback(node, publish_info)
        assert len(node.unlinked_publishes) == 1
        assert len(node.callbacks[0].publishes) == 0
        assert len(node.callbacks[1].publishes) == 0

        publish_info = {
            "topic_name": "/topic0",
            "callback_name": "callback0",
        }
        node = Node("node0")
        node.callbacks.append(
            SubscriptionCallback(None, "node0", "callback0", "symbol0", "/topic0")
        )
        node.callbacks.append(
            SubscriptionCallback(None, "node1", "callback1", "symbol1", "/topic1")
        )
        importer._attach_publish_to_callback(node, publish_info)
        assert len(node.unlinked_publishes) == 0
        assert len(node.callbacks[0].publishes) == 1
        assert len(node.callbacks[1].publishes) == 0

    def test_create_node(self):
        importer = YamlArchitectureImporter(None)

        node_obj = {
            "node_name": "node0",
            "callbacks": [
                {
                    "callback_name": SubscriptionCallback.to_callback_name(0),
                    "type": SubscriptionCallback.TYPE_NAME,
                    "topic_name": "/topic1",
                    "symbol": "symbol0",
                },
                {
                    "callback_name": SubscriptionCallback.to_callback_name(1),
                    "type": SubscriptionCallback.TYPE_NAME,
                    "topic_name": "/topic0",
                    "symbol": "symbol1",
                },
            ],
            "variable_passings": [
                {
                    "callback_name_write": SubscriptionCallback.to_callback_name(0),
                    "callback_name_read": SubscriptionCallback.to_callback_name(1),
                }
            ],
            "publishes": [
                {
                    "topic_name": "/topic0",
                    "callback_name": UNDEFINED_STR,
                },
                {
                    "topic_name": "/topic1",
                    "callback_name": SubscriptionCallback.to_callback_name(0),
                },
            ],
        }

        node = importer._create_node(node_obj, IGNORE_TOPICS)

        assert len(node.variable_passings) == 1
        assert len(node.callbacks) == 2
        assert len(node.publishes) == 2
        assert len(node.subscriptions) == 2

    def test_crate_arch(self):
        importer = YamlArchitectureImporter(None)

        arch_obj = {"path_name_aliases": [], "nodes": []}
        arch_obj["nodes"].append(
            {
                "node_name": "node0",
                "callbacks": [
                    {
                        "callback_name": SubscriptionCallback.to_callback_name(0),
                        "type": SubscriptionCallback.TYPE_NAME,
                        "topic_name": "/topic1",
                        "symbol": "symbol0",
                    },
                    {
                        "callback_name": SubscriptionCallback.to_callback_name(1),
                        "type": SubscriptionCallback.TYPE_NAME,
                        "topic_name": "/topic0",
                        "symbol": "symbol1",
                    },
                ],
                "variable_passings": [
                    {
                        "callback_name_write": SubscriptionCallback.to_callback_name(0),
                        "callback_name_read": SubscriptionCallback.to_callback_name(1),
                    }
                ],
                "publishes": [
                    {
                        "topic_name": "/topic3",
                        "callback_name": UNDEFINED_STR,
                    },
                    {
                        "topic_name": "/topic2",
                        "callback_name": SubscriptionCallback.to_callback_name(0),
                    },
                ],
            }
        )
        arch_obj["nodes"].append(
            {
                "node_name": "node1",
                "callbacks": [
                    {
                        "callback_name": SubscriptionCallback.to_callback_name(0),
                        "type": SubscriptionCallback.TYPE_NAME,
                        "topic_name": "/topic2",
                        "symbol": "symbol2",
                    },
                    {
                        "callback_name": SubscriptionCallback.to_callback_name(1),
                        "type": SubscriptionCallback.TYPE_NAME,
                        "topic_name": "/topic3",
                        "symbol": "symbol3",
                    },
                ],
                "publishes": [],
            }
        )

        arch = importer._create_arch(arch_obj, IGNORE_TOPICS)

        assert len(arch.variable_passings) == 1
        assert len(arch.communications) == 2
        assert len(arch.nodes) == 2
        assert len(arch.path_aliases) == 0
