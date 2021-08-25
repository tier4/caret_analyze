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

from trace_analysis.architecture import Architecture
from trace_analysis.application import Application
from trace_analysis.record.lttng import Lttng


class TestNode:
    def test_init_satus(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[0]

        assert len(node.variable_passings) == 0

    def test_search_paths(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[0]

        assert len(node.variable_passings) == 0

    def test_compose_callback_duration(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")

        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[0]
        callback = node.callbacks[0]

        df = callback.to_dataframe()
        columns_exepct = {"callback_start_timestamp", "callback_end_timestamp"}
        assert set(df.columns) == columns_exepct

        t, latencies = callback.to_timeseries()
        bin_latencies, hist = callback.to_histogram()

        assert len(t) == 3 and len(latencies) == 3
        assert len(bin_latencies) == 1 and len(hist) == 2
