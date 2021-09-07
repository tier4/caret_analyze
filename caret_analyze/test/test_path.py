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

from typing import Optional, List

import pandas as pd
import numpy as np

from caret_analyze.callback import TimerCallback
from caret_analyze.communication import Communication, VariablePassing
from caret_analyze.path import ColumnNameCounter, PathLatencyMerger
from caret_analyze.record import Record, Records
from caret_analyze import Application, Architecture, Lttng


class TestPathLatencyManager:
    def test_init(self, mocker):
        records: Records

        def custom_to_records() -> Records:
            return records

        callback0 = TimerCallback(None, "/node0", "callback0", "symbol0", 100)

        mocker.patch.object(callback0, "to_records", custom_to_records)

        records = Records(
            [
                Record({"callback_start_timestamp": 0, "callback_end_timestamp": 1}),
                Record({"callback_start_timestamp": 5, "callback_end_timestamp": 6}),
            ]
        )

        merger = PathLatencyMerger(callback0)

        records_expect = Records(
            [
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 0,
                        "/node0/callback0/callback_end_timestamp/0": 1,
                    }
                ),
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 5,
                        "/node0/callback0/callback_end_timestamp/0": 6,
                    }
                ),
            ]
        )
        records_expect.sort(key="/node0/callback0/callback_start_timestamp/0")
        merger.records.sort(key="/node0/callback0/callback_start_timestamp/0")
        assert merger.records.equals(records_expect)

    def test_merge(self, mocker):
        records: Records

        def custom_to_records() -> Records:
            return records

        callback0 = TimerCallback(None, "/node0", "callback0", "symbol0", 100)
        callback1 = TimerCallback(None, "/node1", "callback1", "symbol1", 100)
        variable_passing = VariablePassing(None, callback0, callback1)

        mocker.patch.object(callback0, "to_records", custom_to_records)
        mocker.patch.object(variable_passing, "to_records", custom_to_records)

        records = Records(
            [
                Record({"callback_start_timestamp": 0, "callback_end_timestamp": 1}),
                Record({"callback_start_timestamp": 5, "callback_end_timestamp": 6}),
            ]
        )

        merger = PathLatencyMerger(callback0)

        records = Records(
            [
                Record({"callback_end_timestamp": 1, "callback_start_timestamp": 2}),
                Record({"callback_end_timestamp": 6, "callback_start_timestamp": 7}),
            ]
        )
        merger.merge(variable_passing, "callback_end_timestamp")

        records_expect = Records(
            [
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 0,
                        "/node0/callback0/callback_end_timestamp/0": 1,
                        "/node1/callback1/callback_start_timestamp/0": 2,
                    }
                ),
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 5,
                        "/node0/callback0/callback_end_timestamp/0": 6,
                        "/node1/callback1/callback_start_timestamp/0": 7,
                    }
                ),
            ]
        )
        records_expect.sort(key="/node0/callback0/callback_start_timestamp/0", inplace=True)
        merger.records.sort(key="/node0/callback0/callback_start_timestamp/0", inplace=True)
        assert merger.records.equals(records_expect)

    def test_merge_sequential(self, mocker):
        records: Records

        def custom_to_records() -> Records:
            return records

        callback0 = TimerCallback(None, "/node0", "callback0", "symbol0", 100)
        callback1 = TimerCallback(None, "/node1", "callback1", "symbol1", 100)
        communication = Communication(None, callback0, callback1)
        communication.is_intra_process = True

        mocker.patch.object(callback0, "to_records", custom_to_records)
        mocker.patch.object(communication, "to_records", custom_to_records)

        records = Records(
            [
                Record({"callback_start_timestamp": 0, "callback_end_timestamp": 1}),
                Record({"callback_start_timestamp": 5, "callback_end_timestamp": 6}),
            ]
        )

        merger = PathLatencyMerger(callback0)

        records = Records(
            [
                Record({"rclcpp_intra_publish_timestamp": 2, "callback_start_timestamp": 3}),
                Record({"rclcpp_intra_publish_timestamp": 7, "callback_start_timestamp": 8}),
            ]
        )
        merger.merge_sequencial(
            communication, "callback_start_timestamp", "rclcpp_intra_publish_timestamp"
        )

        records_expect = Records(
            [
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 0,
                        "/node0/callback0/callback_end_timestamp/0": 1,
                        "/node0/callback0/rclcpp_intra_publish_timestamp/0": 2,
                        "/node1/callback1/callback_start_timestamp/0": 3,
                    }
                ),
                Record(
                    {
                        "/node0/callback0/callback_start_timestamp/0": 5,
                        "/node0/callback0/callback_end_timestamp/0": 6,
                        "/node0/callback0/rclcpp_intra_publish_timestamp/0": 7,
                        "/node1/callback1/callback_start_timestamp/0": 8,
                    }
                ),
            ]
        )
        records_expect.sort(key="/node0/callback0/callback_start_timestamp/0")
        merger.records.sort(key="/node0/callback0/callback_start_timestamp/0")

        assert merger.records.equals(records_expect)


class TestColumnNameCounter:
    def test_to_column_name(self):
        callback0 = TimerCallback(None, "/node0", "callback0", "symbol0", 100)
        callback1 = TimerCallback(None, "/node1", "callback1", "symbol1", 100)
        communication = Communication(None, callback0, callback1)

        counter = ColumnNameCounter()

        name = counter.to_column_name(callback0, "callback_start_timestamp")
        assert name == "/node0/callback0/callback_start_timestamp/0"

        name = counter.to_column_name(communication, "rclcpp_publish_timestamp")
        assert name == "/node0/callback0/rclcpp_publish_timestamp/0"

        name = counter.to_column_name(communication, "callback_start_timestamp")
        assert name == "/node1/callback1/callback_start_timestamp/0"

    def test_increment_count(self):
        node_name = "/node0"
        callback_name = "callback0"
        callback = TimerCallback(None, node_name, callback_name, "symbol0", 100)

        counter = ColumnNameCounter()
        key = "callback_start_timestamp"
        name = counter._to_column_name(callback, key)
        assert name == f"{node_name}/{callback_name}/{key}/0"

        counter.increment_count(callback, [key])
        name = counter._to_column_name(callback, key)
        assert name == f"{node_name}/{callback_name}/{key}/0"

        counter.increment_count(callback, [key])
        name = counter._to_column_name(callback, key)
        assert name == f"{node_name}/{callback_name}/{key}/1"

    def test_private_to_column_name(self):
        callback0 = TimerCallback(None, "/node0", "callback0", "symbol0", 100)

        counter = ColumnNameCounter()
        name = counter._to_column_name(callback0, "callback_start_timestamp")
        assert name == "/node0/callback0/callback_start_timestamp/0"

    def test_to_key(self):
        counter = ColumnNameCounter()
        callback = TimerCallback(None, "/node0", "callback0", "symbol0", 100)
        key = counter._to_key(callback, "callback_start_timestamp")
        assert key == "/node0/callback0/callback_start_timestamp"


class TestPath:
    def test_column_names(
        self,
        mocker,
    ):
        columns = [
            "/message_driven_node/subscription_callback_0/callback_start_timestamp/0",
            "/message_driven_node/subscription_callback_0/callback_end_timestamp/0",
            "/message_driven_node/subscription_callback_1/callback_start_timestamp/0",
            "/message_driven_node/subscription_callback_1/callback_end_timestamp/0",
            "/message_driven_node/subscription_callback_1/rclcpp_publish_timestamp/0",
            "/message_driven_node/subscription_callback_1/rcl_publish_timestamp/0",
            "/message_driven_node/subscription_callback_1/dds_write_timestamp/0",
            "/timer_driven_node/subscription_callback_0/on_data_available_timestamp/0",
            "/timer_driven_node/subscription_callback_0/callback_start_timestamp/0",
            "/timer_driven_node/subscription_callback_0/callback_end_timestamp/0",
        ]

        def custom_to_dataframe(
            self, remove_dropped=False, *, column_names: Optional[List[str]] = None
        ) -> pd.DataFrame:

            assert column_names == columns

            dummy_data = np.arange(5 * len(columns)).reshape(5, len(columns))
            df = pd.DataFrame(dummy_data, columns=columns)
            return df

        lttng = Lttng("sample/lttng_samples/end_to_end_sample")
        arch = Architecture()
        arch.import_file(
            "sample/lttng_samples/end_to_end_sample/architecture_modified.yaml", "yaml", lttng
        )
        app = Application(arch)

        start_cb_name = "/message_driven_node/subscription_callback_0"
        end_cb_name = "/timer_driven_node/subscription_callback_0"
        paths = app.search_paths(start_cb_name, end_cb_name)
        path = paths[0]

        mocker.patch.object(path, "to_dataframe", custom_to_dataframe)

        path.to_histogram()
        path.to_timeseries()
