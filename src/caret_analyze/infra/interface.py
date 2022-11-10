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

from ..common import ClockConverter
from ..record.interface import RecordsInterface
from ..value_objects import (CallbackStructValue, CommunicationStructValue,
                             NodePathStructValue, PublisherStructValue, Qos,
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
            Callback info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

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
            VariablePassingInfo info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

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
        """
        Compose node path records.

        Parameters
        ----------
        node_path_info : NodePathStructValue
            NodePath info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

        """
        pass

    # callback_publish
    # any
    # callback_start
    @abstractmethod
    def communication_records(
        self,
        communication_info: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Compose communication records.

        Parameters
        ----------
        communication_info : CommunicationStructValue
            Communication info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

        """
        pass

    @abstractmethod
    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Compose communication records.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Subscription info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

        """
        pass

    @abstractmethod
    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Compose communication records.

        Parameters
        ----------
        publisher : PublisherStructValue
            Publisher info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

        """
        pass

    @abstractmethod
    def timer_records(
        self,
        timer: TimerStructValue
    ) -> RecordsInterface:
        """
        Compose timer records.

        Parameters
        ----------
        timer : TimerStructValue
            Timer info to get.

        Returns
        -------
        RecordsInterface
            Composed records instance.

        """
        pass

    @abstractmethod
    def get_sim_time_converter(
        self,
    ) -> ClockConverter:
        """
        Get ClockConverter instance.

        Returns
        -------
        ClockConverter
            Converter to convert from system time to sim time.

        """
        pass

    @abstractmethod
    def verify_communication(
        self,
        communication: CommunicationStructValue
    ) -> bool:
        """
        Check whether the environment is capable of calculating communication records correctly.

        Parameters
        ----------
        communication : CommunicationStructValue
            Communication info to check.

        Returns
        -------
        bool
            True if communication records can be calculated correctly, false otherwise.

        """
        pass


class RuntimeDataProvider(RecordsProvider):

    @abstractmethod
    def get_rmw_implementation(
        self
    ) -> Optional[str]:
        """
        Get rmw implementation name.

        Returns
        -------
        Optional[str]
            Rmw implementation name.

        Note
        ----
            It is better to return a ValueObject than to return a string.

        """
        pass

    @abstractmethod
    def is_intra_process_communication(
        self,
        communication_info: CommunicationStructValue
    ) -> Optional[bool]:
        """
        Check whether given communication is intra process communication.

        Parameters
        ----------
        communication_info : CommunicationStructValue
            Communication info to check.

        Returns
        -------
        Optional[bool]
            True if given communication is intra process communication, false othewise.

        """

        pass

    @abstractmethod
    def get_qos(
        self,
        info: Union[PublisherStructValue, SubscriptionStructValue]
    ) -> Optional[Qos]:
        """
        Get QoS info.

        Parameters
        ----------
        info : Union[PublisherStructValue, SubscriptionStructValue]
            info to get QoS.

        Returns
        -------
        Optional[Qos]
            QoS information for the specified Publisher/Subscription.
            None if the DataProvider implementation does not support it.

        """
        pass
