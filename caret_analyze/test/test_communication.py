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

from typing import List, Optional

from caret_analyze.application import Application
from caret_analyze.callback import SubscriptionCallback
from caret_analyze.communication import Communication
from caret_analyze.communication import DDSLatency
from caret_analyze.communication import PubSubLatency
from caret_analyze.communication import VariablePassing
from caret_analyze.pub_sub import Publisher
from caret_analyze.record.lttng import Lttng

import numpy as np
import pandas as pd
import pytest


class TestCommunication:

    def test_to_dataframe(self):
        lttng = Lttng('sample/lttng_samples/talker_listener/')
        app = Application('sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
        comm = app.communications[0]

        df = comm.to_dataframe()
        columns_expect = {
            'callback_start_timestamp',
            'dds_write_timestamp',
            'on_data_available_timestamp',
            'rcl_publish_timestamp',
            'rclcpp_publish_timestamp',
        }
        assert set(df.columns) == columns_expect

    @pytest.mark.parametrize(
        'trace_dir, comm_idx, is_intra_process',
        [
            ('sample/lttng_samples/talker_listener/', 0, False),
            ('sample/lttng_samples/cyclic_pipeline_intra_process', 0, True),
        ],
    )
    def test_is_intra_process(self, trace_dir, comm_idx, is_intra_process):
        lttng = Lttng(trace_dir)
        app = Application(trace_dir, 'lttng', lttng)
        comm = app.communications[comm_idx]
        assert comm.is_intra_process == is_intra_process

    @pytest.mark.parametrize(
        'is_intra_process, columns',
        [
            (True, Communication.column_names_intra_process),
            (False, Communication.column_names_inter_process),
        ],
    )
    def test_column_names(self, mocker, is_intra_process: bool, columns: List[str]):
        def custom_to_dataframe(self, *, column_names: Optional[List[str]] = None) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, '', '', '', '')
        pub = Publisher('node_name', 'topic_name', 'callback_name')
        comm = Communication(None, callback, callback, pub)
        mocker.patch.object(comm, 'to_dataframe', custom_to_dataframe)

        comm.is_intra_process = is_intra_process
        comm.to_histogram()
        comm.to_timeseries()

    @pytest.mark.parametrize(
        'trace_dir, comm_idx, binsize_ns, timeseries_len, histogram_len',
        [
            ('sample/lttng_samples/talker_listener/', 0, 100000, 5, 5),
            ('sample/lttng_samples/cyclic_pipeline_intra_process', 0, 100000, 5, 23),
        ],
    )
    def test_to_timeseries_and_to_histogram(
        self, trace_dir, comm_idx, binsize_ns, timeseries_len, histogram_len
    ):
        lttng = Lttng(trace_dir)
        app = Application(trace_dir, 'lttng', lttng)
        comm = app.communications[comm_idx]

        t, latencies = comm.to_timeseries()
        assert len(t) == timeseries_len and len(latencies) == timeseries_len

        latencies, hist = comm.to_histogram(binsize_ns=binsize_ns)
        assert len(latencies) == histogram_len and len(hist) == histogram_len + 1

    def test_to_pubsub_latency(self):
        lttng = Lttng('sample/lttng_samples/talker_listener/')
        app = Application('sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
        comm = app.communications[0]

        latency = comm.to_pubsub_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 5 and len(latencies) == 5

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 5 and len(hist) == 6

    def test_to_dds_latency(self):
        lttng = Lttng('sample/lttng_samples/talker_listener/')
        app = Application('sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
        comm = app.communications[0]

        latency = comm.to_dds_latency()

        t, latencies = latency.to_timeseries()
        assert len(t) == 5 and len(latencies) == 5

        latencies, hist = latency.to_histogram(binsize_ns=100000)
        assert len(latencies) == 2 and len(hist) == 3


class TestPubSubLatency:

    def test_column_names(self, mocker):
        columns = PubSubLatency.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(self, *, column_names: Optional[List[str]] = None) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, '', '', '', '')
        pub = Publisher('node_name', 'topic_name', 'callback_name')
        comm = PubSubLatency(None, callback, callback, pub)
        mocker.patch.object(comm, 'to_dataframe', custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()


class TestDDSLatency:

    def test_column_names(self, mocker):
        columns = DDSLatency.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(self, *, column_names: Optional[List[str]] = None) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        pub = Publisher('node_name', 'topic_name', 'callback_name')
        callback = SubscriptionCallback(None, '', '', '', '')
        comm = DDSLatency(None, callback, callback, pub)
        mocker.patch.object(comm, 'to_dataframe', custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()


class TestVariablePassingLatency:

    def test_column_names(self, mocker):
        columns = VariablePassing.column_names
        Communication.column_names_intra_process

        def custom_to_dataframe(self, *, column_names: Optional[List[str]] = None) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, '', '', '', '')
        comm = VariablePassing(None, callback, callback)
        mocker.patch.object(comm, 'to_dataframe', custom_to_dataframe)

        comm.to_histogram()
        comm.to_timeseries()
