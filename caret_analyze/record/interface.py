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

from .record import RecordsInterface


class PublisherInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def topic_name(self) -> str:
        pass

    @property
    @abstractmethod
    def callback_name(self) -> Optional[str]:
        pass


class SubscriptionInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def topic_name(self) -> str:
        pass

    @property
    @abstractmethod
    def callback_name(self) -> str:
        pass


class CallbackInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def symbol(self) -> str:
        pass

    @property
    @abstractmethod
    def callback_name(self) -> str:
        pass

    @property
    @abstractmethod
    def subscription(self) -> Optional[SubscriptionInterface]:
        pass


class TimerCallbackInterface(CallbackInterface):
    TYPE_NAME = 'timer_callback'

    @classmethod
    def to_callback_name(cls, i: int) -> str:
        return f'{cls.TYPE_NAME}_{i}'

    @property
    @abstractmethod
    def period_ns(self) -> int:
        pass


class SubscriptionCallbackInterface(CallbackInterface):
    TYPE_NAME = 'subscription_callback'

    @classmethod
    def to_callback_name(cls, index: int) -> str:
        return f'{cls.TYPE_NAME}_{index}'

    @property
    @abstractmethod
    def topic_name(self) -> str:
        pass


class RecordsContainer(metaclass=ABCMeta):
    # callback_end_timestamp
    # callback_object
    # callback_start_timestamp
    @abstractmethod
    def compose_callback_records(self, callback_attr: CallbackInterface) -> RecordsInterface:
        pass

    # subsequent_callback_object
    # callback_start_timestamp
    # dds_write_timestamp
    # on_data_available_timestamp
    # rcl_publish_timestamp
    # rclcpp_publish_timestamp
    @abstractmethod
    def compose_inter_process_communication_records(
        self,
        subscription_callback_attr: SubscriptionCallbackInterface,
        publish_callback_attr: CallbackInterface,
    ) -> RecordsInterface:
        pass

    # subsequent_callback_object
    # callback_start_timestamp
    # rclcpp_intra_publish_timestamp
    @abstractmethod
    def compose_intra_process_communication_records(
        self,
        subscription_callback_attr: SubscriptionCallbackInterface,
        publish_callback_attr: CallbackInterface,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def compose_variable_passing_records(
        self,
        callback_write_attr: CallbackInterface,
        callback_read_attr: CallbackInterface,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def get_rmw_implementation(self) -> str:
        pass


class ArchitectureInfoContainer(metaclass=ABCMeta):

    @abstractmethod
    def get_node_names(self) -> List[str]:
        pass

    @abstractmethod
    def get_timer_callbacks(
        self,
        node_name: Optional[str] = None,
        period_ns: Optional[int] = None,
    ) -> List[TimerCallbackInterface]:
        pass

    @abstractmethod
    def get_subscription_callbacks(
        self,
        node_name: Optional[str] = None,
        topic_name: Optional[str] = None,
    ) -> List[SubscriptionCallbackInterface]:
        pass

    @abstractmethod
    def get_publishers(
        self, node_name: Optional[str] = None, topic_name: Optional[str] = None
    ) -> List[PublisherInterface]:
        pass
