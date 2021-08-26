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

from typing import List, Optional

import numpy as np
import pandas as pd

from trace_analysis.record.lttng import Lttng
from trace_analysis.architecture import Architecture
from trace_analysis.application import Application
from trace_analysis.communication import Communication, PubSubLatency, DDSLatency, VariablePassing
from trace_analysis.callback import SubscriptionCallback


class TestCommunication:
    def test_to_dataframe(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
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
            ("sample/lttng_samples/talker_listener/", 0, False),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", 0, True),
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
        "is_intra_process, columns",
        [
            (True, Communication.column_names_intra_process),
            (False, Communication.column_names_inter_process),
        ],
    )
    def test_column_names(self, mocker, is_intra_process: bool, columns: List[str]):
        def custom_to_dataframe(
            self, remove_dropped=False, *, column_names: Optional[List[str]] = None
        ) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, "", "", "", "")
        comm = Communication(None, callback, callback)
        mocker.patch.object(comm, "to_dataframe", custom_to_dataframe)

        comm.is_intra_process = is_intra_process
        comm.to_histogram()
        comm.to_timeseries()

    @pytest.mark.parametrize(
        "trace_dir, comm_idx, binsize_ns, timeseries_len, histogram_len",
        [
            ("sample/lttng_samples/talker_listener/", 0, 100000, 3, 4),
            ("sample/lttng_samples/cyclic_pipeline_intra_process", 0, 100000, 5, 22),
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
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        comm = app.communications[0]

        latency = comm.to_pubsub_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 3 and len(latencies) == 3

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 4 and len(hist) == 5

    def test_to_dds_latency(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        comm = app.communications[0]

        latency = comm.to_dds_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 3 and len(latencies) == 3

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 1 and len(hist) == 2


class TestPubSubLatency:
    def test_column_names(self, mocker):
        columns = PubSubLatency.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(
            self, remove_dropped=False, *, column_names: Optional[List[str]] = None
        ) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, "", "", "", "")
        comm = PubSubLatency(None, callback, callback)
        mocker.patch.object(comm, "to_dataframe", custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()


class TestDDSLatency:
    def test_column_names(self, mocker):
        columns = DDSLatency.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(
            self, remove_dropped=False, *, column_names: Optional[List[str]] = None
        ) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, "", "", "", "")
        comm = DDSLatency(None, callback, callback)
        mocker.patch.object(comm, "to_dataframe", custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()


class TestVariablePassingLatency:
    def test_column_names(self, mocker):
        columns = VariablePassing.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(
            self, remove_dropped=False, *, column_names: Optional[List[str]] = None
        ) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, "", "", "", "")
        comm = VariablePassing(None, callback, callback)
        mocker.patch.object(comm, "to_dataframe", custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()
