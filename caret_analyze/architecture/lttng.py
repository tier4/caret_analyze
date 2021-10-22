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

from typing import List

from .interface import ArchitectureReader
from ..record.lttng import Lttng
from ..value_objects.callback_info import SubscriptionCallbackInfo, TimerCallbackInfo
from ..value_objects.path_alias import PathAlias
from ..value_objects.publisher import Publisher
from ..value_objects.variable_passing_info import VariablePassingInfo


class ArchitectureLttng(ArchitectureReader):

    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        self._lttng = lttng

    def get_node_names(self) -> List[str]:
        return self._lttng.get_node_names()

    def get_timer_callbacks(
        self,
        node_name: str
    ) -> List[TimerCallbackInfo]:
        return self._lttng.get_timer_callbacks(node_name)

    def get_path_aliases(self) -> List[PathAlias]:
        return []

    def get_variable_passings(self, node_name: str) -> List[VariablePassingInfo]:
        node_name
        return []

    def get_subscription_callbacks(
        self,
        node_name: str,
    ) -> List[SubscriptionCallbackInfo]:
        return self._lttng.get_subscription_callbacks(node_name)

    def get_publishers(
        self,
        node_name: str,
    ) -> List[Publisher]:
        return self._lttng.get_publishers(node_name)
