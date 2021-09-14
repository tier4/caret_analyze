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

import pytest


class TestArchitecture:
    @pytest.mark.parametrize(
        'yaml_path, nodes_len, aliases_len, comm_len, var_passings_len',
        [
            ('sample/lttng_samples/talker_listener/architecture.yaml', 2, 0, 1, 0),
            ('sample/lttng_samples/multi_talker_listener/architecture.yaml', 4, 0, 2, 0),
            ('sample/lttng_samples/cyclic_pipeline_intra_process/architecture.yaml', 2, 0, 2, 0),
            ('sample/lttng_samples/end_to_end_sample/architecture.yaml', 6, 0, 5, 0),
            ('sample/lttng_samples/end_to_end_sample/architecture_modified.yaml', 6, 1, 5, 2),
        ],
    )
    def test_import_file(self, yaml_path, nodes_len, aliases_len, comm_len, var_passings_len):
        arch = Architecture(yaml_path, 'yaml', None)

        assert len(arch.nodes) == nodes_len
        assert len(arch._path_aliases) == aliases_len
        assert len(arch.communications) == comm_len
        assert len(arch.variable_passings) == var_passings_len

    @pytest.mark.parametrize(
        'trace_path',
        [
            ('sample/lttng_samples/talker_listener'),
            ('sample/lttng_samples/multi_talker_listener'),
            ('sample/lttng_samples/cyclic_pipeline_intra_process'),
            ('sample/lttng_samples/end_to_end_sample'),
        ],
    )
    def test_export_and_import_yaml(self, tmpdir, trace_path):
        arch = Architecture(trace_path, 'lttng', None)

        trace_name = trace_path.split('/')[-1]
        yaml_path = tmpdir.mkdir('architecture').join(f'{trace_name}.yaml')

        arch.export(yaml_path)

        arch_ = Architecture(yaml_path, 'yaml', None)

        assert len(arch.nodes) == len(arch_.nodes)
        assert len(arch.path_aliases) == len(arch_.path_aliases)
        assert len(arch.communications) == len(arch_.communications)
        assert len(arch.variable_passings) == len(arch_.variable_passings)

    def test_add_path_alias(self):
        yaml_path = 'sample/lttng_samples/talker_listener/architecture.yaml'
        arch = Architecture(yaml_path, 'yaml', None)

        assert len(arch.path_aliases) == 0

        callback = arch.nodes[0].callbacks[0]
        arch.add_path_alias('path_name', [callback])

        assert len(arch.path_aliases) == 1
        alias = arch.path_aliases[0]
        assert alias.path_name == 'path_name'
        assert alias.callback_names == [callback.unique_name]

    def test_has_path_alias(self):
        yaml_path = 'sample/lttng_samples/talker_listener/architecture.yaml'
        arch = Architecture(yaml_path, 'yaml', None)

        assert arch.has_path_alias('path_name') is False

        callback = arch.nodes[0].callbacks[0]
        arch.add_path_alias('path_name', [callback])

        assert arch.has_path_alias('path_name') is True
