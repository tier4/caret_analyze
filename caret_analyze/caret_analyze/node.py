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

from typing import List, Optional

from .callback import CallbackBase
from .communication import VariablePassing
from .path import Path
from .pub_sub import Publisher
from .pub_sub import Subscription
from .record.interface import LatencyComposer


class Node:

    def __init__(self, node_name: str) -> None:
        super().__init__()
        self.node_name: str = node_name
        self.callbacks: List[CallbackBase] = []
        self._unlinked_publishes: List[Publisher] = []
        self.composer: Optional[LatencyComposer] = None
        self.variable_passings: List[VariablePassing] = []
        self.paths: List[Path] = []

    @property
    def publishes(self) -> List[Publisher]:
        publishes = self.unlinked_publishes
        for cb in self.callbacks:
            publishes = publishes + cb.publishes
        return publishes

    @property
    def subscriptions(self) -> List[Subscription]:
        subs: List[Subscription] = []
        for cb in self.callbacks:
            if cb.subscription is not None:
                subs.append(cb.subscription)
        return subs

    @property
    def unlinked_publishes(self) -> List[Publisher]:
        return self._unlinked_publishes
