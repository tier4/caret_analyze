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


import os

from caret_analyze.cli import Create

import pytest


class TestCreate:

    def test_callback_graph(self, tmpdir):
        create = Create()
        architecture_path = 'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml'

        result_dir = tmpdir.mkdir('cli_test')
        export_path = result_dir.join('callback_graph.svg').strpath

        create.callback_graph(architecture_path, export_path)
        assert os.path.exists(export_path)
        os.remove(export_path)

        callback_names = (
            '/sensor_dummy_node/timer_callback_0',
            '/filter_node/subscription_callback_0',
            '/message_driven_node/subscription_callback_0',
            '/message_driven_node/subscription_callback_1',
            '/timer_driven_node/subscription_callback_0',
            '/timer_driven_node/timer_callback_0',
            '/actuator_dummy_node/subscription_callback_0'
        )
        create.callback_graph(
            architecture_path, export_path, *callback_names)
        assert os.path.exists(export_path)
        os.remove(export_path)

    def test_architecture(self, tmpdir):
        create = Create()
        trace_dir = 'sample/lttng_samples/talker_listener'

        result_dir = tmpdir.mkdir('cli_test')
        export_path = result_dir.join('architecture.yaml').strpath

        create.architecture(trace_dir, export_path)
        assert os.path.exists(export_path)

    @pytest.mark.parametrize(
        'trace_dir, arch_path', [
            (
                'sample/lttng_samples/end_to_end_sample/fastrtps',
                'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml'
            ),
            (
                'sample/lttng_samples/end_to_end_sample/cyclonedds',
                'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml'
            ),
        ]
    )
    def test_chain_latency(self, tmpdir, trace_dir, arch_path):
        create = Create()

        result_dir = tmpdir.mkdir('cli_test')
        export_path = result_dir.join('chain_latency.svg').strpath

        create.chain_latency(trace_dir, arch_path, 'end_to_end', export_path)

        assert os.path.exists(export_path)
        os.remove(export_path)

    def test_message_flow(self, tmpdir):
        create = Create()
        trace_dir = 'sample/lttng_samples/end_to_end_sample/fastrtps'
        architecture_path = 'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml'

        result_dir = tmpdir.mkdir('cli_test')
        export_path = result_dir.join('message_flow.html').strpath

        create.message_flow(trace_dir, architecture_path,
                            'end_to_end', export_path)

        assert os.path.exists(export_path)
