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

from caret_analyze.architecture.yaml import ArchitectureYaml
from caret_analyze.value_objects.callback_info import SubscriptionCallbackInfo, TimerCallbackInfo
from caret_analyze.value_objects.callback_type import CallbackType
from caret_analyze.value_objects.subscription import Subscription
from pytest_mock import MockerFixture


class TestArchitectureYaml:

    def test_empty_architecture(self, mocker: MockerFixture):
        archtecture_text = """
path_name_aliases: []
nodes: []
        """

        mocker.patch('builtins.open', mocker.mock_open(read_data=archtecture_text))
        reader = ArchitectureYaml('file_name')

        node_names = reader.get_node_names()
        assert len(node_names) == 0

        timer_cbs = reader.get_timer_callbacks('')
        assert len(timer_cbs) == 0

        sub_cbs = reader.get_subscription_callbacks('')
        assert len(sub_cbs) == 0

    def test_full_architecture(self, mocker: MockerFixture):
        architecture_text = """
path_name_aliases:
- path_name: target_path
  callbacks:
  - /talker/timer_callback_0
  - /listener/subscription_callback_0
nodes:
- node_name: /listener
  callbacks:
  - callback_name: subscription_callback_0
    type: subscription_callback
    topic_name: /chatter
    symbol: listener_sub_symbol
  - callback_name: timer_callback_0
    type: timer_callback
    period_ns: 1
    symbol: listener_timer_symbol
  variable_passings:
  - callback_name_write: subscription_callback_0
    callback_name_read: timer_callback_0
  publishes:
  - topic_name: /xxx
    callback_name: timer_callback_0
- node_name: /talker
  callbacks:
  - callback_name: timer_callback_0
    type: timer_callback
    period_ns: 2
    symbol: talker_symbol
  publishes:
  - topic_name: /chatter
    callback_name: timer_callback_0
        """
        mocker.patch('builtins.open', mocker.mock_open(read_data=architecture_text))
        reader = ArchitectureYaml('file_name')

        node_names = reader.get_node_names()
        assert len(node_names) == 2
        assert '/talker' in node_names
        assert '/listener' in node_names

        timer_cbs = reader.get_timer_callbacks('/talker')
        assert len(timer_cbs) == 1
        timer_cb = timer_cbs[0]
        assert isinstance(timer_cb, TimerCallbackInfo)
        assert timer_cb.callback_type == CallbackType.Timer
        assert timer_cb.symbol == 'talker_symbol'
        assert timer_cb.node_name == '/talker'
        assert timer_cb.subscription is None
        assert timer_cb.period_ns == 2

        timer_pubs = reader.get_publishers('/talker')
        assert len(timer_pubs) == 1
        timer_pub = timer_pubs[0]
        assert timer_pub.node_name == '/talker'
        assert timer_pub.topic_name == '/chatter'
        assert timer_pub.callback_name == 'timer_callback_0'
        assert timer_pub.callback_unique_name == '/talker/timer_callback_0'

        sub_cbs = reader.get_subscription_callbacks('/listener')
        assert len(sub_cbs) == 1
        sub_cb = sub_cbs[0]
        assert isinstance(sub_cb, SubscriptionCallbackInfo)
        assert sub_cb.callback_type == CallbackType.Subscription
        assert sub_cb.symbol == 'listener_sub_symbol'
        assert sub_cb.node_name == '/listener'
        assert sub_cb.topic_name == '/chatter'
        subscription_expect = Subscription('/listener', '/chatter', 'subscription_callback_0')
        assert sub_cb.subscription == subscription_expect
        assert sub_cb.subscription.callback_name == subscription_expect.callback_name
        assert sub_cb.subscription.topic_name == subscription_expect.topic_name
        assert sub_cb.subscription.node_name == subscription_expect.node_name

        var_passes = reader.get_variable_passings('/listener')
        assert len(var_passes) == 1
        var_pass = var_passes[0]
        assert var_pass.node_name == '/listener'
        assert var_pass.write_callback_unique_name == '/listener/subscription_callback_0'
        assert var_pass.read_callback_unique_name == '/listener/timer_callback_0'

        path_aliases = reader.get_path_aliases()
        assert len(path_aliases) == 1
        alias = path_aliases[0]
        assert len(alias.callback_unique_names) == 2
        assert alias.callback_unique_names[0] == '/talker/timer_callback_0'
        assert alias.callback_unique_names[1] == '/listener/subscription_callback_0'

        timer_pubs = reader.get_publishers('/listener')
        assert len(timer_pubs) == 1
        timer_pub = timer_pubs[0]
        assert timer_pub.node_name == '/listener'
        assert timer_pub.topic_name == '/xxx'
        assert timer_pub.callback_name == 'timer_callback_0'
        assert timer_pub.callback_unique_name == '/listener/timer_callback_0'
