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

from .message_flow_plot import MessageFlowPlot
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import InvalidArgumentError
from ...runtime import Path


class MessageFlowPlotFactory:
    """Factory class to create an instance of MessageFlowPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        target_path: Path,
        visualize_lib: VisualizeLibInterface,
        granularity: str | None = None,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> MessageFlowPlot:
        granularity = granularity or 'raw'
        if granularity not in ['raw', 'node']:
            raise InvalidArgumentError('granularity must be [ raw / node ]')

        return MessageFlowPlot(
            target_path, visualize_lib, granularity, treat_drop_as_delay, lstrip_s, rstrip_s
        )
