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

from __future__ import annotations

from abc import abstractmethod, ABCMeta

from typing import Tuple, Optional, List

import numpy as np
import pandas as pd
from trace_analysis.record.interface import (
    LatencyComposer,
)
from trace_analysis.callback import CallbackBase, SubscriptionCallback
from trace_analysis.latency import LatencyBase
from trace_analysis.record import Records


class CommunicationInterface(metaclass=ABCMeta):
    @property
    @abstractmethod
    def callback_from(self):
        pass

    @property
    @abstractmethod
    def callback_to(self):
        pass


class Communication(CommunicationInterface, LatencyBase):
    column_names_inter_process = [
        "rclcpp_publish_timestamp",
        "rcl_publish_timestamp",
        "dds_write_timestamp",
        "on_data_available_timestamp",
        "callback_start_timestamp",
    ]

    column_names_intra_process = [
        "rclcpp_intra_publish_timestamp",
        "callback_start_timestamp",
    ]

    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
        callback_publish: CallbackBase,
        callback_subscription: SubscriptionCallback,
    ):
        self._latency_composer = latency_composer
        self.callback_publish = callback_publish
        self.callback_subscription = callback_subscription

        self.is_intra_process: Optional[bool] = None
        if latency_composer:
            self.is_intra_process = self._is_intra_process()

        self._dds_latency: Optional[DDSLatency] = None
        self._pubsub_latency: Optional[PubSubLatency] = None
        if self.is_intra_process:
            self._dds_latency = DDSLatency(
                self._latency_composer, self.callback_publish, self.callback_subscription
            )
            self._pubsub_latency = PubSubLatency(
                self._latency_composer, self.callback_publish, self.callback_subscription
            )

    @property
    def callback_from(self):
        return self.callback_publish

    @property
    def callback_to(self):
        return self.callback_subscription

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        if self.is_intra_process:
            columns = Communication.column_names_intra_process
        else:
            columns = Communication.column_names_inter_process
        return super().to_dataframe(remove_dropped, column_names=columns)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        if self.is_intra_process:
            columns = Communication.column_names_intra_process
        else:
            columns = Communication.column_names_inter_process
        return super().to_timeseries(remove_dropped, column_names=columns)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        if self.is_intra_process:
            columns = Communication.column_names_intra_process
        else:
            columns = Communication.column_names_inter_process
        return super().to_histogram(binsize_ns, column_names=columns)

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert self._latency_composer is not None

        records: Records
        if self.is_intra_process:
            records = self._latency_composer.compose_intra_process_communication_records(
                self.callback_subscription, self.callback_publish, remove_dropped
            )
        else:
            records = self._latency_composer.compose_inter_process_communication_records(
                self.callback_subscription, self.callback_publish, remove_dropped
            )

        runtime_info_columns = ["callback_object", "publisher_handle"]
        if remove_runtime_info:
            records.drop_columns(runtime_info_columns, inplace=True)

        return records

    def _is_intra_process(self) -> bool:
        assert self._latency_composer is not None

        # Even if intra-process communication is enabled in the configuration,
        # at least one process from actual publish to callback execution is required.
        # Depending on the measurement results,
        # columns may be unintentionally generated as inter-process communication.
        intra_records = self._latency_composer.compose_intra_process_communication_records(
            self.callback_subscription, self.callback_publish, remove_dropped=True
        )

        return len(intra_records) > 0

    def to_pubsub_latency(self) -> PubSubLatency:
        assert self.is_intra_process is False, "target is intra process communication"
        return PubSubLatency(
            self._latency_composer, self.callback_publish, self.callback_subscription
        )

    def to_dds_latency(self) -> DDSLatency:
        assert self.is_intra_process is False, "target is intra process communication"
        return DDSLatency(
            self._latency_composer, self.callback_publish, self.callback_subscription
        )


class PubSubLatency(CommunicationInterface, LatencyBase):
    column_names = [
        "rclcpp_publish_timestamp",
        "rcl_publish_timestamp",
        "dds_write_timestamp",
        "on_data_available_timestamp",
        "callback_start_timestamp",
    ]

    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
        callback_publish: CallbackBase,
        callback_subscription: SubscriptionCallback,
    ):
        self._latency_composer = latency_composer
        self.callback_publish = callback_publish
        self.callback_subscription = callback_subscription

    @property
    def callback_from(self):
        return self.callback_publish

    @property
    def callback_to(self):
        return self.callback_subscription

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return super().to_dataframe(remove_dropped, column_names=PubSubLatency.column_names)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_timeseries(remove_dropped, column_names=PubSubLatency.column_names)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_histogram(binsize_ns, column_names=PubSubLatency.column_names)

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert self._latency_composer is not None

        records = self._latency_composer.compose_inter_process_communication_records(
            self.callback_subscription, self.callback_publish, remove_dropped
        )

        runtime_info_columns = ["callback_object", "publisher_handle"]
        if remove_runtime_info:
            records.drop_columns(runtime_info_columns, inplace=True)

        return records


class DDSLatency(CommunicationInterface, LatencyBase):
    column_names = [
        "dds_write_timestamp",
        "on_data_available_timestamp",
    ]

    def __init__(
        self,
        latency_composer: Optional[LatencyComposer],
        callback_publish: CallbackBase,
        callback_subscription: SubscriptionCallback,
    ):
        self._latency_composer = latency_composer
        self.callback_publish = callback_publish
        self.callback_subscription = callback_subscription

    @property
    def callback_from(self):
        return self.callback_publish

    @property
    def callback_to(self):
        return self.callback_subscription

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return super().to_dataframe(remove_dropped, column_names=DDSLatency.column_names)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_timeseries(remove_dropped, column_names=DDSLatency.column_names)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_histogram(binsize_ns, column_names=DDSLatency.column_names)

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert self._latency_composer is not None

        records = self._latency_composer.compose_inter_process_communication_records(
            self.callback_subscription, self.callback_publish, remove_dropped
        )
        rcl_layer_columns = [
            "callback_start_timestamp",
            "rcl_publish_timestamp",
            "rclcpp_publish_timestamp",
        ]
        records.drop_columns(rcl_layer_columns, inplace=True)

        runtime_info_columns = ["callback_object", "publisher_handle"]
        if remove_runtime_info:
            records.drop_columns(runtime_info_columns, inplace=True)

        return records


class VariablePassing(CommunicationInterface, LatencyBase):
    column_names = ["callback_end_timestamp", "callback_start_timestamp"]

    def __init__(
        self,
        latency_composer: LatencyComposer,
        callback_write: CallbackBase,
        callback_read: CallbackBase,
    ):
        self.callback_write = callback_write
        self.callback_read = callback_read
        self._latency_composer = latency_composer

    def to_records(self, remove_dropped=False, remove_runtime_info=False) -> Records:
        assert self._latency_composer is not None
        records: Records = self._latency_composer.compose_variable_passing_records(
            self.callback_write, self.callback_read, remove_dropped
        )
        runtime_info_columns = ["read_callback_object", "write_callback_object"]
        if remove_runtime_info:
            records.drop_columns(runtime_info_columns, inplace=True)

        return records

    def to_dataframe(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> pd.DataFrame:
        return super().to_dataframe(remove_dropped, column_names=VariablePassing.column_names)

    def to_timeseries(
        self, remove_dropped=False, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_timeseries(remove_dropped, column_names=VariablePassing.column_names)

    def to_histogram(
        self, binsize_ns: int = 1000000, *, column_names: Optional[List[str]] = None
    ) -> Tuple[np.array, np.array]:
        return super().to_histogram(binsize_ns, column_names=VariablePassing.column_names)

    @property
    def callback_from(self):
        return self.callback_write

    @property
    def callback_to(self):
        return self.callback_read
