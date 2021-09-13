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

from typing import List, Callable, NamedTuple, Union, Optional, Tuple, Set

import numpy as np
import pandas as pd

from collections import UserList, UserDict

import itertools

from caret_analyze.latency import LatencyBase
from caret_analyze.callback import CallbackBase
from caret_analyze.communication import Communication, VariablePassing, CommunicationInterface
from caret_analyze.util import Util, UniqueList
from caret_analyze.record.record import RecordsInterface, merge, merge_sequencial


class TracePoint(NamedTuple):
    CALLBACK_START_TIMESTAMP: str = "callback_start_timestamp"
    CALLBACK_END_TIMESTAMP: str = "callback_end_timestamp"
    RCLCPP_PUBLISH_TIMESTAMP: str = "rclcpp_publish_timestamp"
    RCL_PUBLISH_TIMESTAMP: str = "rcl_publish_timestamp"
    DDS_WRITE_TIMESTAMP: str = "dds_write_timestamp"
    ON_DATA_AVAILABLE_TIMESTAMP: str = "on_data_available_timestamp"
    RCLCPP_INTRA_PUBLISH_TIMESTAMP: str = "rclcpp_intra_publish_timestamp"


TRACE_POINT = TracePoint()


class TracePoints(NamedTuple):
    CALLBACK: List[str] = [
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
        TRACE_POINT.CALLBACK_END_TIMESTAMP,
    ]
    INTER_PROCESS_FROM: List[str] = [
        TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
        TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
        TRACE_POINT.DDS_WRITE_TIMESTAMP,
    ]
    INTER_PROCESS_TO: List[str] = [
        TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
    ]
    INTRA_PROCESS_FROM: List[str] = [TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP]
    INTRA_PROCESS_TO: List[str] = [TRACE_POINT.CALLBACK_START_TIMESTAMP]
    VARIABLE_PASSING_FROM: List[str] = [TRACE_POINT.CALLBACK_END_TIMESTAMP]
    VARIABLE_PASSING_TO: List[str] = [TRACE_POINT.CALLBACK_START_TIMESTAMP]


TRACE_POINTS = TracePoints()


LatencyComponent = Union[CallbackBase, Communication, VariablePassing]


class ColumnNameCounter(UserDict):
    def __init__(self) -> None:
        super().__init__()
        self._tracepoints_from = (
            TRACE_POINTS.INTER_PROCESS_FROM
            + TRACE_POINTS.INTRA_PROCESS_FROM
            + TRACE_POINTS.VARIABLE_PASSING_FROM
        )

    def increment_count(self, latency: LatencyComponent, tracepoint_names: List[str]) -> None:
        for tracepoint_name in tracepoint_names:
            if isinstance(latency, CallbackBase):
                key = self._to_key(latency, tracepoint_name)

            if isinstance(latency, Communication) or isinstance(latency, VariablePassing):
                if tracepoint_name in self._tracepoints_from:
                    callback_from = latency.callback_from
                    assert callback_from is not None
                    key = self._to_key(callback_from, tracepoint_name)
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
            callback_from = latency.callback_from
            assert callback_from is not None
            return self._to_column_name(callback_from, tracepoint_name)
        else:
            return self._to_column_name(latency.callback_to, tracepoint_name)

    def _to_key(self, callback: CallbackBase, tracepoint_name: str) -> str:
        return f"{callback.unique_name}/{tracepoint_name}"

    def _to_column_name(self, callback: CallbackBase, tracepoint_name: str) -> str:
        key = self._to_key(callback, tracepoint_name)
        count = self.get(key, 0)
        column_name = f"{key}/{count}"
        return column_name


class PathLatencyMerger:
    def __init__(self, latency: LatencyComponent, column_only: Optional[bool] = None) -> None:
        self._column_only = column_only or False
        self._counter = ColumnNameCounter()
        tracepoint_names = latency.to_records().columns
        self._counter.increment_count(latency, list(tracepoint_names))

        self.records = self._get_records_with_preffix(latency)

        self.column_names = UniqueList()
        self.column_names += self._to_ordered_column_names(latency, self.records.columns)

    def _to_ordered_column_names(
        self, latency: LatencyComponent, tracepoint_names: Set[str]
    ) -> List[str]:
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
        increment_keys = other.to_records().columns - set([join_trace_point_name])
        self._counter.increment_count(other, list(increment_keys))

        records = self._get_records_with_preffix(other)
        self.column_names += self._to_ordered_column_names(other, records.columns)
        if self._column_only:
            return

        join_key = self._counter.to_column_name(other, join_trace_point_name)
        self.records = merge(
            left_records=self.records,
            right_records=records,
            join_key=join_key,
            how="left",
        )

    def merge_sequencial(
        self,
        other: Communication,
        trace_point_name: str,
        sub_trace_point_name: str,
    ) -> None:
        increment_keys = other.to_records().columns
        self._counter.increment_count(other, list(increment_keys))

        records = self._get_records_with_preffix(other)
        self.column_names += self._to_ordered_column_names(other, records.columns)

        if self._column_only:
            return
        callback_from = other.callback_from
        assert callback_from is not None
        record_stamp_key = self._counter.to_column_name(callback_from, trace_point_name)
        sub_record_stamp_key = self._counter.to_column_name(callback_from, sub_trace_point_name)
        self.records = merge_sequencial(
            left_records=self.records,
            right_records=records,
            left_stamp_key=record_stamp_key,
            right_stamp_key=sub_record_stamp_key,
            join_key=None,
            how="left",
        )

    def _get_callback_records(self, callback: CallbackBase) -> RecordsInterface:
        records = callback.to_records()
        renames = {}

        for key in TRACE_POINTS.CALLBACK:
            renames[key] = self._counter.to_column_name(callback, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_intra_process_records(self, communication: Communication) -> RecordsInterface:
        records = communication.to_records()
        renames = {}

        for key in TRACE_POINTS.INTRA_PROCESS_FROM:
            renames[key] = self._counter.to_column_name(communication, key)
        for key in TRACE_POINTS.INTRA_PROCESS_TO:
            renames[key] = self._counter.to_column_name(communication, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_inter_process_records(self, communication: Communication) -> RecordsInterface:
        records = communication.to_records()
        renames = {}

        for key in TRACE_POINTS.INTER_PROCESS_FROM:
            renames[key] = self._counter.to_column_name(communication, key)
        for key in TRACE_POINTS.INTER_PROCESS_TO:
            renames[key] = self._counter.to_column_name(communication, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_variable_passing_records(self, variable_passing: VariablePassing) -> RecordsInterface:
        records = variable_passing.to_records()
        renames = {}

        for key in TRACE_POINTS.VARIABLE_PASSING_FROM:
            renames[key] = self._counter.to_column_name(variable_passing, key)
        for key in TRACE_POINTS.VARIABLE_PASSING_TO:
            renames[key] = self._counter.to_column_name(variable_passing, key)

        records.rename_columns(renames, inplace=True)
        return records

    def _get_records_with_preffix(self, latency: LatencyComponent) -> RecordsInterface:
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
    ) -> None:
        chain: List[LatencyBase] = self._to_measurement_target_chain(
            callbacks, communications, variable_passings
        )
        super().__init__(chain)
        self._column_names: List[str] = []
        return None

    def to_records(self) -> RecordsInterface:
        assert len(self) > 0
        records, _ = self._merge_path()
        return records

    def _get_column_names(self):
        # In order to get the list of column names,
        # information on intra-process and inter-process communication is required.
        # Since this information is obtained from the actual measurement results,
        # each LatencyBase needs to have a LatencyComposer.
        # In cases such as visualization of callback graphs only,
        # the LatencyComposer (trace results) is not necessary,
        # so the list of column names is acquired when it becomes necessary.

        if len(self._column_names) == 0:
            assert len(self) > 0
            _, self._column_names = self._merge_path(column_only=True)
        return self._column_names

    def __str__(self) -> str:
        unique_names = [callback.unique_name for callback in self.callbacks]
        return "\n".join(unique_names)

    def _merge_path(self, column_only=False) -> Tuple[RecordsInterface, List[str]]:
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
                        latency_,
                        "callback_start_timestamp",
                        "rclcpp_intra_publish_timestamp",
                    )
                else:
                    merger.merge_sequencial(
                        latency_,
                        "callback_start_timestamp",
                        "rclcpp_publish_timestamp",
                    )

            elif isinstance(latency, CallbackBase) and isinstance(latency_, VariablePassing):
                # callback -> variable passing case
                # callback_end -> callback_start [merge]
                merger.merge(latency_, "callback_end_timestamp")

        column_names = merger.column_names.data
        if column_only is False:
            merger.records.sort(column_names[0], inplace=True)

        return merger.records, column_names

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        column_names  # use self._column_names instead.
        return super().to_dataframe(remove_dropped, column_names=self._get_column_names())

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        column_names  # use self._column_names instead.
        return super().to_timeseries(remove_dropped, column_names=self._get_column_names())

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        column_names  # use self._column_names instead.
        return super().to_histogram(binsize_ns, column_names=self._get_column_names())

    @property
    def callbacks(self) -> List[CallbackBase]:
        return list(filter(lambda x: isinstance(x, CallbackBase), self))

    @property
    def communications(self) -> List[Communication]:
        return list(filter(lambda x: isinstance(x, Communication), self))

    @property
    def variable_passings(self) -> List[VariablePassing]:
        return list(filter(lambda x: isinstance(x, VariablePassing), self))

    def _to_measurement_target_chain(
        self,
        callbacks: List[CallbackBase],
        communications: List[Communication],
        variable_passings: List[VariablePassing],
    ) -> List[LatencyBase]:
        chain: List[LatencyBase] = []
        if len(callbacks) == 0:
            return chain

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

    def contains(self, latency: LatencyBase):
        if isinstance(latency, CallbackBase):
            return latency in self.callbacks

        if isinstance(latency, VariablePassing):
            return latency in self.variable_passings

        if isinstance(latency, Communication):
            return latency in self.communications

        return False
