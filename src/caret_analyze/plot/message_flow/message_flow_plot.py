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

from logging import getLogger

from bokeh.plotting import figure as Figure

import pandas as pd

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...runtime import Path

logger = getLogger(__name__)


class MessageFlowPlot(PlotBase):
    """Class that provides API for message flow plot."""

    def __init__(
        self,
        target_path: Path,
        visualize_lib: VisualizeLibInterface,
        granularity: str,
        treat_drop_as_delay: bool,
        lstrip_s: float,
        rstrip_s: float,
    ) -> None:
        self._target_path = target_path
        self._visualize_lib = visualize_lib
        self._granularity = granularity
        self._treat_drop_as_delay = treat_drop_as_delay
        self._lstrip_s = lstrip_s
        self._rstrip_s = rstrip_s

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        logger.warning("'to_dataframe' method is not implemented in MessageFlowPlot.")
        return pd.DataFrame()

    def figure(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None  # FIXME: not used in message flow
    ) -> Figure:
        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else True

        return self._visualize_lib.message_flow(
            self._target_path, xaxis_type, ywheel_zoom,
            self._granularity, self._treat_drop_as_delay, self._lstrip_s, self._rstrip_s
        )
