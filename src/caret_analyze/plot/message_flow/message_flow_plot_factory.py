# Copyright 2021 TIER IV, Inc.
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
from ...exceptions import InvalidArgumentError
from ...runtime import Path


class MessageFlowPlotFactory:
    """Factory class to create an instance of MessageFlowPlot."""

    @staticmethod
    def create_instance(
        target_path: Path,
        visualize_lib: VisualizeLibInterface,
        granularity: str | None = None,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> MessageFlowPlot:
        """
        Create instance.

        Parameters
        ----------
        target_path : Path
            Target path.
        visualize_lib : VisualizeLibInterface
            Instance of VisualizeLibInterface used for visualization.
        granularity : str | None
            Granularity.
        treat_drop_as_delay : bool
            Treat drop as delay.
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        MessageFlowPlot
            Created instance of MessageFlowPlot.

        Raises
        ------
        InvalidArgumentError
            Argument granularity is not "raw" or "node".

        """
        granularity = granularity or 'raw'
        if granularity not in ['raw', 'node']:
            raise InvalidArgumentError('granularity must be [ raw / node ]')

        return MessageFlowPlot(
            target_path, visualize_lib, granularity, treat_drop_as_delay, lstrip_s, rstrip_s
        )
