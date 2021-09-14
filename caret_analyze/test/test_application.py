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

from caret_analyze.application import Application
from caret_analyze.record.lttng import Lttng

import pytest


class TestApplication:

    @pytest.mark.parametrize(
        'trace_path, yaml_path, start_cb_name, end_cb_name, paths_len',
        [
            (
                'sample/lttng_samples/talker_listener/',
                'sample/lttng_samples/talker_listener/architecture.yaml',
                '/talker/timer_callback_0',
                '/listener/subscription_callback_0',
                1,
            ),
            (  # cyclic case
                'sample/lttng_samples/cyclic_pipeline_intra_process',
                'sample/lttng_samples/cyclic_pipeline_intra_process/architecture.yaml',
                '/pipe1/subscription_callback_0',
                '/pipe1/subscription_callback_0',
                1,
            ),
            (
                'sample/lttng_samples/end_to_end_sample/fastrtps',
                'sample/lttng_samples/end_to_end_sample/architecture.yaml',
                '/sensor_dummy_node/timer_callback_0',
                '/actuator_dummy_node/subscription_callback_0',
                0,
            ),
            (  # publisher callback_name and callback depencency added
                'sample/lttng_samples/end_to_end_sample/fastrtps',
                'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml',
                '/sensor_dummy_node/timer_callback_0',
                '/actuator_dummy_node/subscription_callback_0',
                1,
            ),
        ],
    )
    def test_search_paths(self, trace_path, yaml_path, start_cb_name, end_cb_name, paths_len):
        lttng = Lttng(trace_path)
        app = Application(yaml_path, 'yaml', lttng)

        paths = app.search_paths(start_cb_name, end_cb_name)
        assert len(paths) == paths_len

        for path in paths:
            assert path[0].unique_name == start_cb_name
            assert path[-1].unique_name == end_cb_name
