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

from abc import ABCMeta, abstractmethod
from typing import Optional, Union

from caret_analyze.value_objects.subscription import IntraProcessBufferStructValue

from ..common import ClockConverter
from ..record import RecordsInterface
from ..value_objects import (
    CallbackStructValue,
    CommunicationStructValue,
    NodePathStructValue,
    PublisherStructValue,
    Qos,
    SubscriptionStructValue,
    TimerStructValue,
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    TransformValue,
    VariablePassingStructValue,
)


class RecordsProvider(metaclass=ABCMeta):

    # callback_start
    # callback_end
    @abstractmethod
    def callback_records(
        self,
        callback_info: CallbackStructValue
    ) -> RecordsInterface:
        """
        Compose callback records.

        Parameters
        ----------
        callback_info : CallbackStructInfo
            [description]

        Returns
        -------
        RecordsInterface
            [description]

        """
        pass

    # callback_end
    # any
    # callback_start
    @abstractmethod
    def variable_passing_records(
        self,
        variable_passing_info: VariablePassingStructValue
    ) -> RecordsInterface:
        """
        Compose variable passing records.

        Parameters
        ----------
        variable_passing_info : VariablePassingStructInfo

        Returns
        -------
        RecordsInterface

        """
        pass

    @abstractmethod
    def ipc_buffer_records(
        self,
        ipc_buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        pass

    # callback_start
    # any
    # rclcpp_publish
    @abstractmethod
    def node_records(
        self,
        node_path_info: NodePathStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def communication_records(
        self,
        communication_info: CommunicationStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def tf_broadcast_records(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def tf_lookup_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def tf_communication_records(
        self,
        communication: TransformCommunicationStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def tf_set_records(
        self,
        buffer: TransformFrameBufferStructValue,
        transform: TransformValue,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def tf_set_lookup_records(
        self,
        buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def timer_records(
        self,
        timer: TimerStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def get_sim_time_converter(
        self,
    ) -> ClockConverter:
        pass

    @abstractmethod
    def verify_communication(
        self,
        communication: CommunicationStructValue
    ) -> bool:
        pass


class RuntimeDataProvider(RecordsProvider):

    @abstractmethod
    def get_rmw_implementation(
        self
    ) -> Optional[str]:
        pass

    @abstractmethod
    def is_intra_process_communication(
        self,
        communication_info: CommunicationStructValue
    ) -> Optional[bool]:
        pass

    @abstractmethod
    def get_qos(
        self,
        info: Union[PublisherStructValue, SubscriptionStructValue]
    ) -> Optional[Qos]:
        pass
