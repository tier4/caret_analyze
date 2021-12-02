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

from typing import Optional

from ..value_objects import SubscriptionStructValue
from ..infra.interface import RuntimeDataProvider
from ..common import CustomDict

class Subscription:

    def __init__(
        self,
        val: SubscriptionStructValue,
        data_provider: Optional[RuntimeDataProvider],
    ) -> None:
        self._val = val
        self._provider = data_provider

    @property
    def node_name(self) -> str:
        return self._val.node_name

    @property
    def summary(self) -> CustomDict:
        return CustomDict({
            'topic_name': self._val.topic_name,
            'callback': self._val.callback_name
        })

    @property
    def topic_name(self) -> str:
        return self._val.topic_name

    @property
    def callback_name(self) -> Optional[str]:
        return self._val.callback_name
