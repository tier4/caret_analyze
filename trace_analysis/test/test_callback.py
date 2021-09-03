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
import numpy as np
import pandas as pd

from trace_analysis.callback import TimerCallback, SubscriptionCallback, CallbackBase
from trace_analysis.record.lttng import Lttng
from trace_analysis.application import Application
from trace_analysis.architecture import Architecture


class TestTimerCallback:
    def test_to_callback_name(self):
        assert TimerCallback.to_callback_name(0) == "timer_callback_0"
        assert TimerCallback.to_callback_name(1) == "timer_callback_1"

    def test_to_dataframe(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[1]
        callback = node.callbacks[0]

        callback.latency_composer = lttng

        df = callback.to_dataframe()
        columns_expect = {"callback_start_timestamp", "callback_end_timestamp"}
        assert set(df.columns) == columns_expect

    def test_to_timeseries_and_to_histogram(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[1]
        callback = node.callbacks[0]

        t, latencies = callback.to_timeseries()
        assert len(t) == 5 and len(latencies) == 5

        latencies, hist = callback.to_histogram(binsize_ns=100000)
        assert len(latencies) == 6 and len(hist) == 7


class TestSubscriptionCallback:
    def test_to_callback_name(self):
        assert SubscriptionCallback.to_callback_name(0) == "subscription_callback_0"
        assert SubscriptionCallback.to_callback_name(1) == "subscription_callback_1"

    def test_to_dataframe(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[0]
        callback = node.callbacks[0]

        callback.latency_composer = lttng

        df = callback.to_dataframe()
        columns_expect = {"callback_start_timestamp", "callback_end_timestamp"}
        assert set(df.columns) == columns_expect

    def test_to_timeseries_and_to_histogram(self):
        lttng = Lttng("sample/lttng_samples/talker_listener/")
        arch = Architecture()
        arch.import_file("sample/lttng_samples/talker_listener/architecture.yaml", "yaml", lttng)
        app = Application(arch)
        node = app.nodes[0]
        callback = node.callbacks[0]

        t, latencies = callback.to_timeseries()
        assert len(t) == 5 and len(latencies) == 5

        latencies, hist = callback.to_histogram(binsize_ns=100000)
        assert len(latencies) == 4 and len(hist) == 5

    def test_column_names(self, mocker):
        columns = CallbackBase.column_names

        def custom_to_dataframe(self, *, column_names: Optional[List[str]] = None) -> pd.DataFrame:
            assert column_names == columns
            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        callback = SubscriptionCallback(None, "", "", "", "")
        mocker.patch.object(callback, "to_dataframe", custom_to_dataframe)

        callback.to_histogram()
        callback.to_timeseries()
