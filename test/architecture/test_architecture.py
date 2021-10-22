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


from caret_analyze.architecture import Architecture
from caret_analyze.architecture.architecture import ArchitectureExporter
from caret_analyze.architecture.interface import ArchitectureReader
from caret_analyze.architecture.yaml import ArchitectureYaml

from pytest_mock import MockerFixture

archtecture_yaml = """path_name_aliases:
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


class TestArchiteture:

    def test_empty_architecture(self, mocker: MockerFixture):
        reader_mock = mocker.Mock(spec=ArchitectureReader)

        mocker.patch.object(reader_mock, 'get_node_names', return_value=[])
        mocker.patch.object(reader_mock, 'get_timer_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_path_aliases', return_value=[])
        mocker.patch.object(reader_mock, 'get_variable_passings', return_value=[])
        mocker.patch.object(reader_mock, 'get_subscription_callbacks', return_value=[])
        mocker.patch.object(reader_mock, 'get_publishers', return_value=[])

        arch = Architecture(reader_mock, None)

        assert len(arch.nodes) == 0
        assert len(arch.path_aliases) == 0
        assert len(arch.communications) == 0
        assert len(arch.variable_passings) == 0

    def test_yaml_architecture(self, mocker: MockerFixture):
        mocker.patch('builtins.open', mocker.mock_open(read_data=archtecture_yaml))
        yaml_reader = ArchitectureYaml('file_name')
        arch = Architecture(yaml_reader, None)

        assert len(arch.nodes) == 2
        assert len(arch.path_aliases) == 1
        assert len(arch.communications) == 1
        assert len(arch.variable_passings) == 1

    def test_architecture_export(self, mocker: MockerFixture):
        mocker.patch('builtins.open', mocker.mock_open(read_data=archtecture_yaml))
        yaml_reader = ArchitectureYaml('file_name')
        arch = Architecture(yaml_reader, None)

        exporter = ArchitectureExporter(arch)
        arch_str = exporter.get_str()
        assert arch_str == archtecture_yaml
