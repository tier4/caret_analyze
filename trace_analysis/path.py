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

from typing import List, Callable, Union, Optional, Tuple, Set

import numpy as np
import pandas as pd

from collections import UserList, UserDict
from collections import namedtuple

import itertools

from trace_analysis.latency import LatencyBase
from trace_analysis.callback import CallbackBase
from trace_analysis.communication import Communication, VariablePassing, CommunicationInterface
from trace_analysis.util import Util, UniqueList
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
    def __init__(self, latency: LatencyComponent, column_only: Optional[bool] = None):
        self._column_only = column_only or False
        self._counter = ColumnNameCounter()
        tracepoint_names = latency.to_records(remove_runtime_info=True).columns
        self._counter.increment_count(latency, list(tracepoint_names))

        self.records = self._get_records_with_preffix(latency)

        self.column_names = UniqueList()
        self.column_names += self._to_ordered_column_names(latency, self.records.columns)

    def _to_ordered_column_names(self, latency: LatencyComponent, tracepoint_names: Set[str]):
        if isinstance(latency, CallbackBase) or isinstance(latency, VariablePassing):
            ordered_names = latency.column_names
        elif latency.is_intra_process:
            ordered_names = latency.column_names_intra_process
        else:
            ordered_names = latency.column_names_inter_process

        ordered_columns_names = []
        for ordered_name, column_name in itertools.product(ordered_names, tracepoint_names):
            if ordered_name in column_name:
                ordered_columns_names.append(column_name)
        return ordered_columns_names

    def merge(self, other: LatencyComponent, join_trace_point_name: str) -> None:
        increment_keys = other.to_records(remove_runtime_info=True).columns - set(
            [join_trace_point_name]
        )
        self._counter.increment_count(other, list(increment_keys))

        records = self._get_records_with_preffix(other)
        self.column_names += self._to_ordered_column_names(other, records.columns)
        if self._column_only:
            return

        join_key = self._counter.to_column_name(other, join_trace_point_name)
        self.records = merge(
            records=self.records,
            records_=records,
            join_key=join_key,
            how="left",
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
        self.column_names += self._to_ordered_column_names(other, records.columns)

        if self._column_only:
            return

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
        self._column_names: List[str] = self._to_column_names()

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert len(self) > 0
        records, _ = self._merge_path(remove_dropped, remove_runtime_info)
        return records

    def _to_column_names(self):
        assert len(self) > 0
        _, column_names = self._merge_path(True, True, True)
        return column_names

    def _merge_path(
        self, remove_dropped: bool, remove_runtime_info: bool, column_only=False
    ) -> Tuple[Records, List[str]]:
        merger = PathLatencyMerger(self.data[0], column_only)

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

        return merger.records, merger.column_names.data

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return super().to_dataframe(remove_dropped, column_names=self._column_names)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_timeseries(remove_dropped, column_names=self._column_names)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_dataframe(binsize_ns, column_names=self._column_names)

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
