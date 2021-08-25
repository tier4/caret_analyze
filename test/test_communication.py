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

import pytest

from trace_analysis.record.lttng import Lttng
from trace_analysis.architecture import Architecture
from trace_analysis.application import Application


class TestCommunication:
    def test_to_dataframe(self):
        lttng = Lttng("test/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("test/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        comm = app.communications[0]

        df = comm.to_dataframe()
        columns_expect = {
            "callback_start_timestamp",
            "dds_write_timestamp",
            "on_data_available_timestamp",
            "rcl_publish_timestamp",
            "rclcpp_publish_timestamp",
        }
        assert set(df.columns) == columns_expect

    @pytest.mark.parametrize(
        "trace_dir, comm_idx, is_intra_process",
        [
            ("test/lttng_samples/talker_listener/", 0, False),
            ("test/lttng_samples/cyclic_pipeline_intra_process", 0, True),
        ],
    )
    def test_is_intra_process(self, trace_dir, comm_idx, is_intra_process):
        lttng = Lttng(trace_dir)
        arch = Architecture()
        arch.import_file(trace_dir, "lttng", lttng)
        app = Application(arch)
        comm = app.communications[comm_idx]
        assert comm.is_intra_process == is_intra_process

    @pytest.mark.parametrize(
        "trace_dir, comm_idx, binsize_ns, timeseries_len, histogram_len",
        [
            ("test/lttng_samples/talker_listener/", 0, 100000, 3, 4),
            ("test/lttng_samples/cyclic_pipeline_intra_process", 0, 100000, 5, 22),
        ],
    )
    def test_to_timeseries_and_to_histogram(
        self, trace_dir, comm_idx, binsize_ns, timeseries_len, histogram_len
    ):
        lttng = Lttng(trace_dir)
        arch = Architecture()
        arch.import_file(trace_dir, "lttng", lttng)
        app = Application(arch)
        comm = app.communications[comm_idx]

        t, latencies = comm.to_timeseries()
        assert len(t) == timeseries_len and len(latencies) == timeseries_len

        latencies, hist = comm.to_histogram(binsize_ns=binsize_ns)
        assert len(latencies) == histogram_len and len(hist) == histogram_len + 1

    def test_to_pubsub_latency(self):
        lttng = Lttng("test/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("test/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        comm = app.communications[0]

        latency = comm.to_pubsub_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 3 and len(latencies) == 3

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 4 and len(hist) == 5

    def test_to_dds_latency(self):
        lttng = Lttng("test/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("test/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        comm = app.communications[0]

        latency = comm.to_dds_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 3 and len(latencies) == 3

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 1 and len(hist) == 2
