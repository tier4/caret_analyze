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

from .record import RecordsInterface
from ..value_objects.callback_info import CallbackInfo, SubscriptionCallbackInfo


class RecordsContainer(metaclass=ABCMeta):
    # callback_end_timestamp
    # callback_object
    # callback_start_timestamp
    @abstractmethod
    def compose_callback_records(self, callback_attr: CallbackInfo) -> RecordsInterface:
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
        subscription_callback_attr: SubscriptionCallbackInfo,
        publish_callback_attr: CallbackInfo,
    ) -> RecordsInterface:
        pass

    # subsequent_callback_object
    # callback_start_timestamp
    # rclcpp_intra_publish_timestamp
    @abstractmethod
    def compose_intra_process_communication_records(
        self,
        subscription_callback_attr: SubscriptionCallbackInfo,
        publish_callback_attr: CallbackInfo,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def compose_variable_passing_records(
        self,
        callback_write_attr: CallbackInfo,
        callback_read_attr: CallbackInfo,
    ) -> RecordsInterface:
        pass

    @abstractmethod
    def get_rmw_implementation(self) -> str:
        pass
