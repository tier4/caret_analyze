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

from typing import Optional, Union

from .path_base import PathBase
from ..common import Summarizable, Summary
from ..infra.interface import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface
from ..value_objects import Qos, SubscriptionStructValue


class Subscription(PathBase, Summarizable):

    def __init__(
        self,
        val: SubscriptionStructValue,
        data_provider: Union[RecordsProvider, RuntimeDataProvider],
    ) -> None:
        super().__init__()
        self._val = val
        self._provider = data_provider

    @property
    def node_name(self) -> str:
        return self._val.node_name

    @property
    def summary(self) -> Summary:
        return self._val.summary

    @property
    def topic_name(self) -> str:
        return self._val.topic_name

    @property
    def callback_name(self) -> Optional[str]:
        return self._val.callback_name

    @property
    def qos(self) -> Optional[Qos]:
        if isinstance(self._provider, RuntimeDataProvider):
            return self._provider.get_qos(self._val)
        return None

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.subscribe_records(self._val)
        return records
