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

from collections.abc import Sequence
from logging import getLogger

from bokeh.plotting import figure as Figure

import pandas as pd

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...runtime import Path

logger = getLogger(__name__)


# TODO: Migrate drawing process to visualize_lib
class ResponseTimeHistPlot(PlotBase):

    def __init__(
        self,
        visualize_lib: VisualizeLibInterface,
        target: Sequence[Path],
        case: str,
        binsize_ns: int
    ) -> None:
        self._visualize_lib = visualize_lib
        self._target = list(target)
        self._case = case
        self._binsize_ns = binsize_ns

    def to_dataframe(self, xaxis_type: str) -> pd.DataFrame:
        logger.warning("'to_dataframe' method is not implemented in ResponseTimeHistPlot.")
        return pd.DataFrame()

    def figure(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None
    ) -> Figure:
        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else True
        full_legends = full_legends if full_legends is not None else True

        return self._visualize_lib.response_time_hist(
            self._target, self._case, self._binsize_ns,
            xaxis_type, ywheel_zoom, full_legends
        )
