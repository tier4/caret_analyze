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

from ..value_objects import CallbackStructValue, PublisherStructValue, SubscriptionStructValue


class InfraHelper():
    @staticmethod
    def cb_to_column(
        callback: CallbackStructValue,
        trace_point_name: str
    ) -> str:
        cb_name = callback.callback_name

        return f'{cb_name}/{trace_point_name}'

    @staticmethod
    def sub_to_column(
        subscription: SubscriptionStructValue,
        trace_point_name: str
    ) -> str:
        topic_name = subscription.topic_name

        return f'{topic_name}/{trace_point_name}'

    @staticmethod
    def pub_to_column(
        publisher: PublisherStructValue,
        trace_point_name: str
    ) -> str:
        topic_name = publisher.topic_name

        return f'{topic_name}/{trace_point_name}'
