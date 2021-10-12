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

from abc import ABCMeta
from abc import abstractmethod
from typing import List, Optional

from .callback import CallbackBase
from .callback import SubscriptionCallback
from .exceptions import TraceResultAanalyzeError
from .latency import LatencyBase
from .pub_sub import Publisher
from .pub_sub import Subscription
from .record import RecordsInterface
from .record.interface import RecordsContainer
from .record.trace_points import TRACE_POINT


class CommunicationInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def callback_from(self) -> Optional[CallbackBase]:
        pass

    @property
    @abstractmethod
    def callback_to(self) -> CallbackBase:
        pass

    @property
    @abstractmethod
    def publisher(self) -> Publisher:
        pass

    @property
    @abstractmethod
    def subscription(self) -> Subscription:
        pass


class VariablePassingInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def callback_from(self) -> CallbackBase:
        pass

    @property
    @abstractmethod
    def callback_to(self) -> CallbackBase:
        pass


class Communication(CommunicationInterface, LatencyBase):
    column_names_inter_process_dds_latency_support = [
        TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
        TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
        TRACE_POINT.DDS_WRITE_TIMESTAMP,
        TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
    ]
    column_names_inter_process = [
        TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
        TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
        TRACE_POINT.DDS_WRITE_TIMESTAMP,
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
    ]

    column_names_intra_process = [
        TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
    ]

    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        callback_publish: Optional[CallbackBase],
        callback_subscription: SubscriptionCallback,
        publisher: Publisher,
    ) -> None:
        self._records_container = records_container
        self._callback_publish: Optional[CallbackBase] = callback_publish
        self.callback_subscription = callback_subscription
        self.is_intra_process: Optional[bool] = None
        self.rmw_implementation: Optional[str] = None
        self._publisher = publisher

        if self._callback_publish is None:
            return

        if records_container:
            self.is_intra_process = self._is_intra_process(records_container)
            self.rmw_implementation = records_container.get_rmw_implementation()

        self._dds_latency: Optional[DDSLatency] = None
        if self.is_intra_process:
            self._dds_latency = DDSLatency(
                self._records_container,
                self.callback_publish,
                self.callback_subscription,
                publisher,
            )

    @property
    def callback_publish(self) -> Optional[CallbackBase]:
        return self._callback_publish

    @property
    def callback_from(self) -> Optional[CallbackBase]:
        return self.callback_publish

    @property
    def callback_to(self) -> SubscriptionCallback:
        return self.callback_subscription

    @property
    def subscription(self) -> Subscription:
        subscription = self.callback_subscription.subscription
        assert subscription is not None
        return subscription

    @property
    def publisher(self) -> Publisher:
        return self._publisher

    @property
    def topic_name(self) -> str:
        return self.callback_to.topic_name

    @property
    def column_names(self) -> List[str]:
        if self.is_intra_process:
            return Communication.column_names_intra_process
        else:
            if self.rmw_implementation == 'rmw_cyclonedds_cpp':
                return Communication.column_names_inter_process
            return Communication.column_names_inter_process_dds_latency_support

    def to_records(self) -> RecordsInterface:
        assert self._records_container is not None
        assert self.callback_publish is not None

        records: RecordsInterface
        if self.is_intra_process:
            records = self._records_container.compose_intra_process_communication_records(
                self.callback_subscription, self.callback_publish
            )
            records.sort(
                Communication.column_names_intra_process[0], inplace=True)
        else:
            records = self._records_container.compose_inter_process_communication_records(
                self.callback_subscription, self.callback_publish
            )
            records.sort(
                Communication.column_names_inter_process[0], inplace=True)

        return records

    def _is_intra_process(self, records_container: RecordsContainer) -> Optional[bool]:
        assert self.callback_publish is not None

        # Even if intra-process communication is enabled in the configuration,
        # at least one process from actual publish to callback execution is required.
        # Depending on the measurement results,
        # columns may be unintentionally generated as inter-process communication.
        try:
            intra_records = records_container.compose_intra_process_communication_records(
                self.callback_subscription, self.callback_publish
            )

            return len(intra_records.data) > 0
        except TraceResultAanalyzeError as e:
            print(e)  # TODO: use logger
            return None

    def to_dds_latency(self) -> DDSLatency:
        assert self.is_intra_process is False, 'target is intra process communication'
        assert self.rmw_implementation == 'rmw_fastrtps_cpp', \
            'dds latency is rmw_fastrtps_cpp only.'
        return DDSLatency(
            self._records_container,
            self.callback_publish,
            self.callback_subscription,
            self._publisher,
        )


class DDSLatency(CommunicationInterface, LatencyBase):
    _column_names = [
        TRACE_POINT.DDS_WRITE_TIMESTAMP,
        TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
    ]

    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        callback_publish: Optional[CallbackBase],
        callback_subscription: SubscriptionCallback,
        publisher: Publisher,
    ) -> None:
        self._records_container = records_container
        self._callback_publish = callback_publish
        self.callback_subscription = callback_subscription
        self._publisher = publisher

    @property
    def callback_publish(self) -> CallbackBase:
        assert self._callback_publish is not None
        return self._callback_publish

    @property
    def callback_from(self) -> CallbackBase:
        assert self.callback_publish is not None
        return self.callback_publish

    @property
    def callback_to(self) -> CallbackBase:
        return self.callback_subscription

    @property
    def subscription(self) -> Subscription:
        subscription = self.callback_subscription.subscription
        assert subscription is not None
        return subscription

    @property
    def publisher(self) -> Publisher:
        return self._publisher

    @property
    def column_names(self) -> List[str]:
        return DDSLatency._column_names

    def to_records(self) -> RecordsInterface:
        assert self._records_container is not None
        assert self.callback_publish is not None

        records = self._records_container.compose_inter_process_communication_records(
            self.callback_subscription, self.callback_publish
        )
        rcl_layer_columns = [
            TRACE_POINT.CALLBACK_START_TIMESTAMP,
            TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
            TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        records.drop_columns(rcl_layer_columns, inplace=True)
        records.sort(self.column_names[0], inplace=True)

        return records


class VariablePassing(VariablePassingInterface, LatencyBase):
    _column_names = [
        TRACE_POINT.CALLBACK_END_TIMESTAMP,
        TRACE_POINT.CALLBACK_START_TIMESTAMP,
        ]

    def __init__(
        self,
        records_container: Optional[RecordsContainer],
        callback_write: CallbackBase,
        callback_read: CallbackBase,
    ) -> None:
        self.callback_write = callback_write
        self.callback_read = callback_read
        self._records_container = records_container

    def to_records(self) -> RecordsInterface:
        assert self._records_container is not None
        records: RecordsInterface = self._records_container.compose_variable_passing_records(
            self.callback_write, self.callback_read
        )
        records.sort(self.column_names[0], inplace=True)
        return records

    @property
    def column_names(self) -> List[str]:
        return VariablePassing._column_names

    @property
    def callback_from(self) -> CallbackBase:
        return self.callback_write

    @property
    def callback_to(self) -> CallbackBase:
        return self.callback_read
