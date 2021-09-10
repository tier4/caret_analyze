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

from caret_analyze.pub_sub import Publisher
from typing import Optional, List, Dict

import pandas as pd
import numpy as np

from caret_analyze.callback import SubscriptionCallback, TimerCallback
from caret_analyze.communication import Communication, VariablePassing
from caret_analyze.path import (
    ColumnNameCounter,
    PathLatencyMerger,
    Path,
)
from caret_analyze.record import Record, Records
from caret_analyze import Application, Architecture, Lttng
from caret_analyze.record.interface import (
    LatencyComposer,
    CallbackInterface,
    RecordsInterface,
    SubscriptionCallbackInterface,
)


class LatencyComposerMock(LatencyComposer):
    def __init__(
        self,
        callback_records: Records,
        inter_process_communication_records: Records,
        intra_process_communication_records: Records,
        variable_passing_records: Records,
    ):
        self.callback_records = callback_records
        self.inter_process_communication_records = inter_process_communication_records
        self.intra_process_communication_records = intra_process_communication_records
        self.variable_passing_records = variable_passing_records
        self.to_callback_object: Dict[CallbackInterface, int] = {}
        self.to_publisher_handle: Dict[CallbackInterface, int] = {}

    def compose_callback_records(self, callback_attr: CallbackInterface) -> RecordsInterface:
        callback_object = self.to_callback_object[callback_attr]

        def is_target(record: Record):
            if "callback_object" not in record.columns:
                return False
            return record.get("callback_object") == callback_object

        records = self.callback_records.filter(is_target)
        assert records is not None
        runtime_info_columns = ["callback_object"]
        records_dropped = records.drop_columns(runtime_info_columns)
        assert records_dropped is not None
        return records_dropped

    def compose_inter_process_communication_records(
        self,
        subscription_callback_attr: SubscriptionCallbackInterface,
        publish_callback_attr: CallbackInterface,
    ) -> RecordsInterface:
        subscription_callback_attr
        assert False, "not implemented"
        publisher_handle = self.to_publisher_handle[publish_callback_attr]

        def is_target(record: Record):
            return record.get("publisher_handle") == publisher_handle

        records = self.inter_process_communication_records.filter(is_target)
        assert records is not None
        return records

    def compose_intra_process_communication_records(
        self,
        subscription_callback_attr: SubscriptionCallbackInterface,
        publish_callback_attr: CallbackInterface,
    ) -> RecordsInterface:
        subscription_callback_attr
        publisher_handle = self.to_publisher_handle[publish_callback_attr]

        def is_target(record: Record):
            return record.get("publisher_handle") == publisher_handle

        records = self.intra_process_communication_records.filter(is_target)
        assert records is not None
        runtime_info_columns = ["callback_object", "publisher_handle"]
        records = records.drop_columns(runtime_info_columns)
        assert records is not None
        return records

    def compose_variable_passing_records(
        self, callback_write_attr: CallbackInterface, callback_read_attr: CallbackInterface
    ) -> RecordsInterface:
        callback_write_attr
        callback_read_attr
        assert False, "not implemented"
        return self.variable_passing_records


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

    def test_merge_sequential(self):
        timer_cb_callback_object = 0
        sub_cb_callback_object = 8
        timer_cb_publisher_handle = 2
        sub_cb_publisher_handle = 12
        callback_records = Records(
            [
                Record(
                    {
                        "callback_start_timestamp": 0,
                        "callback_end_timestamp": 15,
                        "callback_object": timer_cb_callback_object,
                    }
                ),
                Record(
                    {
                        "callback_start_timestamp": 5,
                        "callback_end_timestamp": 16,
                        "callback_object": sub_cb_callback_object,
                    }
                ),
            ]
        )
        intra_process_communication_records = Records(
            [
                Record(
                    {
                        "rclcpp_intra_publish_timestamp": 7,
                        "callback_start_timestamp": 8,
                        "publisher_handle": sub_cb_publisher_handle,
                    }
                ),
                Record(
                    {
                        "rclcpp_intra_publish_timestamp": 9,
                        "callback_start_timestamp": 10,
                        "publisher_handle": timer_cb_publisher_handle,
                    }
                ),
            ]
        )
        inter_process_communication_records = Records()
        variable_passing_records = Records()
        latency_composer_mock = LatencyComposerMock(
            callback_records,
            inter_process_communication_records,
            intra_process_communication_records,
            variable_passing_records,
        )

        timer_cb = TimerCallback(latency_composer_mock, "/node0", "callback0", "symbol0", 100)
        sub_cb = SubscriptionCallback(
            latency_composer_mock, "/node1", "callback1", "symbol1", "/topic1"
        )
        latency_composer_mock.to_callback_object[timer_cb] = timer_cb_callback_object
        latency_composer_mock.to_callback_object[sub_cb] = sub_cb_callback_object

        latency_composer_mock.to_publisher_handle[timer_cb] = timer_cb_publisher_handle
        communication = Communication(latency_composer_mock, timer_cb, sub_cb)
        communication.is_intra_process = True

        merger = PathLatencyMerger(timer_cb)

        merger.merge_sequencial(
            communication, "callback_start_timestamp", "rclcpp_intra_publish_timestamp"
        )

        records_expect = Records(
            [
                Record(
                    {
                        # "callback_object": timer_cb_callback_object,
                        # "publisher_handle": timer_cb_publisher_handle,
                        "/node0/callback0/callback_start_timestamp/0": 0,
                        "/node0/callback0/callback_end_timestamp/0": 15,
                        "/node0/callback0/rclcpp_intra_publish_timestamp/0": 9,
                        "/node1/callback1/callback_start_timestamp/0": 10,
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
        arch = Architecture(
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

    def test_merge_path(self):
        # callback object にpublisher_handleを紐付けるのを確認。
        timer0_cb_obj = 0
        timer1_cb_obj = 1
        sub0_cb_obj = 2
        timer0_pub_handle = 5
        timer1_pub_handle = 6
        callback_records = Records(
            [
                Record(
                    {
                        "callback_object": timer0_cb_obj,
                        "callback_start_timestamp": 1,
                        "callback_end_timestamp": 5,
                    }
                ),
                Record(
                    {
                        "callback_object": timer1_cb_obj,
                        "callback_start_timestamp": 2,
                        "callback_end_timestamp": 6,
                    }
                ),
                Record(
                    {
                        "callback_object": sub0_cb_obj,
                        "callback_start_timestamp": 10,
                        "callback_end_timestamp": 11,
                    }
                ),
            ]
        )
        inter_process_communication_records = Records()
        intra_process_communication_records = Records(
            [
                Record(
                    {
                        "rclcpp_intra_publish_timestamp": 3,
                        "callback_start_timestamp": 8,
                        "publisher_handle": timer1_pub_handle,
                    }
                ),
                Record(
                    {
                        "rclcpp_intra_publish_timestamp": 4,
                        "callback_start_timestamp": 10,
                        "publisher_handle": timer0_pub_handle,
                    }
                ),
            ]
        )
        variable_passing_records = Records()

        latency_composer_mock = LatencyComposerMock(
            callback_records,
            inter_process_communication_records,
            intra_process_communication_records,
            variable_passing_records,
        )
        publisher = Publisher("timer_node", "/topic", "timer_cb")
        timer0_cb = TimerCallback(
            latency_composer_mock, "/timer_node", "timer_cb", "pub_symbol", 100, [publisher]
        )
        latency_composer_mock.to_callback_object[timer0_cb] = timer0_cb_obj
        sub_cb0 = SubscriptionCallback(
            latency_composer_mock, "/sub_node", "sub_cb", "sub_symbol", "/topic", []
        )
        latency_composer_mock.to_callback_object[sub_cb0] = sub0_cb_obj

        latency_composer_mock.to_publisher_handle[timer0_cb] = timer0_pub_handle
        callbacks = [timer0_cb, sub_cb0]
        comm = Communication(latency_composer_mock, timer0_cb, sub_cb0)
        comm.is_intra_process = True
        communications = [comm]
        variable_passings = []

        path = Path(callbacks, communications, variable_passings)
        records, column_names = path._merge_path(column_only=False)
        records_expect = Records(
            [
                Record(
                    {
                        # "callback_object": timer0_cb_obj,  # runtime info are removed
                        # "publisher_handle": timer0_pub_handle,
                        "/timer_node/timer_cb/callback_start_timestamp/0": 1,
                        "/timer_node/timer_cb/callback_end_timestamp/0": 5,
                        "/timer_node/timer_cb/rclcpp_intra_publish_timestamp/0": 4,
                        "/sub_node/sub_cb/callback_start_timestamp/0": 10,
                        "/sub_node/sub_cb/callback_end_timestamp/0": 11,
                    }
                ),
            ]
        )
        assert set(column_names) == set(records_expect.data[0].data.keys())

        assert records.equals(records_expect)
