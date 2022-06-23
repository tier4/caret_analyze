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

from caret_analyze.value_objects.timer import TimerStructValue

from .path_base import PathBase
from ..common import Summarizable, Summary
from ..infra.interface import RecordsProvider, RuntimeDataProvider
from ..record import RecordsInterface


class Timer(PathBase, Summarizable):

    def __init__(
        self,
        val: TimerStructValue,
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
    def period_ns(self) -> int:
        return self._val.period_ns

    @property
    def callback_name(self) -> Optional[str]:
        return self._val.callback_name

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.timer_records(self._val)
        return records
