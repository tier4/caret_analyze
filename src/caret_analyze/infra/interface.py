# Copyright 2021 TIER IV, Inc.
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

from ..common import ClockConverter
from ..record import RecordsInterface
from ..value_objects import (CallbackStructValue, CommunicationStructValue,
                             NodePathStructValue, PublisherStructValue, Qos,
                             SubscriptionCallbackStructValue,
                             SubscriptionStructValue,
                             TimerStructValue,
                             VariablePassingStructValue)


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
        callback_info : CallbackStructValue
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
        variable_passing_info : VariablePassingStructValue
            variable passing information

        Returns
        -------
        RecordsInterface

        """
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

    # callback_publish
    # any
    # callback_start
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
    def timer_records(
        self,
        timer: TimerStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def get_sim_time_converter(
        self,
        min_ns: float,
        max_ns: float
    ) -> ClockConverter:
        pass

    @abstractmethod
    def verify_communication(
        self,
        communication: CommunicationStructValue
    ) -> bool:
        pass

    @abstractmethod
    def path_beginning_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def path_end_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        pass


class RuntimeDataProvider(RecordsProvider):

    @abstractmethod
    def get_rmw_implementation(
        self
    ) -> str | None:
        pass

    @abstractmethod
    def is_intra_process_communication(
        self,
        communication_info: CommunicationStructValue
    ) -> bool | None:
        pass

    @abstractmethod
    def get_qos(
        self,
        info: PublisherStructValue | SubscriptionStructValue
    ) -> Qos | None:
        pass
