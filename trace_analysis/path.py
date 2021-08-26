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

from typing import List, Callable, Union

from collections import UserList, UserDict
from collections import namedtuple

from trace_analysis.latency import LatencyBase
from trace_analysis.callback import CallbackBase
from trace_analysis.communication import Communication, VariablePassing, CommunicationInterface
from trace_analysis.util import Util
from trace_analysis.record.record import Records, merge, merge_sequencial

TracePointsType = namedtuple(
    "TracePointsType",
    "CALLBACK INTER_PROCESS  INTRA_PROCESS VARIABLE_PASSING",
)

CommunicationTracePointsType = namedtuple("CommunicationTracePointsType", "FROM TO")
TracePointType = namedtuple(
    "TracePointType",
    [
        "CALLBACK_START_TIMESTAMP",
        "CALLBACK_END_TIMESTAMP",
        "RCLCPP_PUBLISH_TIMESTAMP",
        "RCL_PUBLISH_TIMESTAMP",
        "DDS_WRITE_TIMESTAMP",
        "ON_DATA_AVAILABLE_TIMESTAMP",
        "RCLCPP_INTRA_PUBLISH_TIMESTAMP",
    ],
)
TRACE_POINT = TracePointType(
    "callback_start_timestamp",
    "callback_end_timestamp",
    "rclcpp_publish_timestamp",
    "rcl_publish_timestamp",
    "dds_write_timestamp",
    "on_data_available_timestamp",
    "rclcpp_intra_publish_timestamp",
)

TRACE_POINTS = TracePointsType(
    (TRACE_POINT.CALLBACK_START_TIMESTAMP, TRACE_POINT.CALLBACK_END_TIMESTAMP),
    CommunicationTracePointsType(
        (
            TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
            TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
            TRACE_POINT.DDS_WRITE_TIMESTAMP,
        ),
        (TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP, TRACE_POINT.CALLBACK_START_TIMESTAMP),
    ),
    CommunicationTracePointsType(
        (TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP,), (TRACE_POINT.CALLBACK_START_TIMESTAMP,)
    ),
    CommunicationTracePointsType(
        (TRACE_POINT.CALLBACK_END_TIMESTAMP,), (TRACE_POINT.CALLBACK_START_TIMESTAMP,)
    ),
)

LatencyComponent = Union[CallbackBase, Communication, VariablePassing]


class ColumnNameCounter(UserDict):
    def __init__(self) -> None:
        super().__init__()
        self._tracepoints_from = (
            TRACE_POINTS.INTRA_PROCESS.FROM
            + TRACE_POINTS.INTER_PROCESS.FROM
            + TRACE_POINTS.VARIABLE_PASSING.FROM
        )

    def increment_count(self, latency: LatencyComponent, tracepoint_names: List[str]) -> None:
        for tracepoint_name in tracepoint_names:
            if isinstance(latency, CallbackBase):
                key = self._to_key(latency, tracepoint_name)

            if isinstance(latency, Communication) or isinstance(latency, VariablePassing):
                if tracepoint_name in self._tracepoints_from:
                    key = self._to_key(latency.callback_from, tracepoint_name)
                else:
                    key = self._to_key(latency.callback_to, tracepoint_name)

            if key not in self.keys():
                self[key] = 0
            else:
                self[key] += 1
        return None

    def to_column_name(
        self,
        latency: LatencyComponent,
        tracepoint_name: str,
    ) -> str:
        if isinstance(latency, CallbackBase):
            return self._to_column_name(latency, tracepoint_name)
        if tracepoint_name in self._tracepoints_from:
            return self._to_column_name(latency.callback_from, tracepoint_name)
        else:
            return self._to_column_name(latency.callback_to, tracepoint_name)

    def _to_key(self, callback: CallbackBase, tracepoint_name: str) -> str:
        return f"{callback.unique_name}/{tracepoint_name}"

    def _to_column_name(self, callback: CallbackBase, tracepoint_name: str):
        key = self._to_key(callback, tracepoint_name)
        count = self.get(key, 0)
        column_name = f"{key}/{count}"
        return column_name


class PathLatencyMerger:
    def __init__(self, latency: LatencyComponent):
        self._counter = ColumnNameCounter()
        self.records = self._get_records_with_preffix(latency)
        tracepoint_names = latency.to_records(remove_runtime_info=True).columns
        self._counter.increment_count(latency, list(tracepoint_names))
        self._sort_key = list(self.records[0].keys())[
            0
        ]  # Save the first record as a key for sorting.

    def merge(self, other: LatencyComponent, join_trace_point_name: str) -> None:
        increment_keys = other.to_records(remove_runtime_info=True).columns - set(
            [join_trace_point_name]
        )
        self._counter.increment_count(other, list(increment_keys))

        records = self._get_records_with_preffix(other)

        join_key = self._counter.to_column_name(other, join_trace_point_name)
        self.records = merge(
            records=self.records,
            records_=records,
            join_key=join_key,
            how="left",
            record_sort_key=self._sort_key,
        )

    def merge_sequencial(
        self,
        other: Communication,
        trace_point_name: str,
        sub_trace_point_name: str,
    ) -> None:
        increment_keys = other.to_records(remove_runtime_info=True).columns
        self._counter.increment_count(other, list(increment_keys))

        records = self._get_records_with_preffix(other)

        record_stamp_key = self._counter.to_column_name(other.callback_from, trace_point_name)
        sub_record_stamp_key = self._counter.to_column_name(
            other.callback_from, sub_trace_point_name
        )
        self.records = merge_sequencial(
            left_records=self.records,
            right_records=records,
            right_stamp_key=record_stamp_key,
            left_stamp_key=sub_record_stamp_key,
            join_key=None,
            left_sort_key=self._sort_key,
        )

    def _get_callback_records(self, callback: CallbackBase) -> Records:
        records = callback.to_records(remove_runtime_info=True)
        renames = {}

        for key in TRACE_POINTS.CALLBACK:
            renames[key] = self._counter.to_column_name(callback, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_intra_process_records(self, communication: Communication) -> Records:
        records = communication.to_records(remove_runtime_info=True)
        renames = {}

        for key in TRACE_POINTS.INTRA_PROCESS.FROM:
            renames[key] = self._counter.to_column_name(communication, key)
        for key in TRACE_POINTS.INTRA_PROCESS.TO:
            renames[key] = self._counter.to_column_name(communication, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_inter_process_records(self, communication: Communication) -> Records:
        records = communication.to_records(remove_runtime_info=True)
        renames = {}

        for key in TRACE_POINTS.INTER_PROCESS.FROM:
            renames[key] = self._counter.to_column_name(communication, key)
        for key in TRACE_POINTS.INTER_PROCESS.TO:
            renames[key] = self._counter.to_column_name(communication, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_variable_passing_records(self, variable_passing: VariablePassing) -> Records:
        records = variable_passing.to_records(remove_runtime_info=True)
        renames = {}

        for key in TRACE_POINTS.VARIABLE_PASSING.FROM:
            renames[key] = self._counter.to_column_name(variable_passing, key)
        for key in TRACE_POINTS.VARIABLE_PASSING.TO:
            renames[key] = self._counter.to_column_name(variable_passing, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_records_with_preffix(self, latency: LatencyComponent) -> Records:
        if isinstance(latency, CallbackBase):
            return self._get_callback_records(latency)
        elif isinstance(latency, Communication):
            if latency.is_intra_process:
                return self._get_intra_process_records(latency)
            else:
                return self._get_inter_process_records(latency)
        elif isinstance(latency, VariablePassing):
            return self._get_variable_passing_records(latency)


class Path(UserList, LatencyBase):
    def __init__(
        self,
        callbacks: List[CallbackBase],
        communications: List[Communication],
        variable_passings: List[VariablePassing],
    ):
        chain: List[LatencyBase] = self._to_measurement_target_chain(
            callbacks, communications, variable_passings
        )
        super().__init__(chain)

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert len(self) > 0

        merger = PathLatencyMerger(self.data[0])

        for latency, latency_ in zip(
            self.data[:-1], self.data[1:]
        ):  # type: LatencyBase, LatencyBase
            if isinstance(latency, Communication) and isinstance(latency_, CallbackBase):
                # communication -> callback case
                # callback_start -> callback_start [merge]
                merger.merge(latency_, "callback_start_timestamp")

            elif isinstance(latency, VariablePassing) and isinstance(latency_, CallbackBase):
                # communication -> callback case
                # callback_start -> callback_start [merge]
                merger.merge(latency_, "callback_start_timestamp")

            elif isinstance(latency, CallbackBase) and isinstance(latency_, Communication):
                # callback -> communication case
                # callback_start -> publish [sequential-merge]
                if latency_.is_intra_process:
                    merger.merge_sequencial(
                        latency_, "callback_start_timestamp", "rclcpp_intra_publish_timestamp"
                    )
                else:
                    merger.merge_sequencial(
                        latency_, "callback_start_timestamp", "rclcpp_publish_timestamp"
                    )

            elif isinstance(latency, CallbackBase) and isinstance(latency_, VariablePassing):
                # callback -> variable passing case
                # callback_end -> callback_start [merge]
                merger.merge(latency_, "callback_end_timestamp")

        return merger.records

    @property
    def callbacks(self) -> List[CallbackBase]:
        return list(filter(lambda x: isinstance(x, CallbackBase), self))

    def _to_measurement_target_chain(
        self,
        callbacks: List[CallbackBase],
        communications: List[Communication],
        variable_passings: List[VariablePassing],
    ) -> List[LatencyBase]:
        chain: List[LatencyBase] = []

        chain.append(callbacks[0])
        for cb, cb_ in zip(callbacks[:-1], callbacks[1:]):
            matched: Callable[[CommunicationInterface], bool] = (
                lambda x: x.callback_from == cb and x.callback_to == cb_
            )

            communication = Util.find_one(communications, matched)
            if communication is not None:
                chain.append(communication)

            variable_passing = Util.find_one(variable_passings, matched)
            if variable_passing is not None:
                chain.append(variable_passing)

            chain.append(cb_)

        return chain

    @property
    def path_name(self):
        pass
