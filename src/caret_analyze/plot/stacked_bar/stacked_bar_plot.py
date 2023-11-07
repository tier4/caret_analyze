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

from bokeh.plotting import figure as Figure

import pandas as pd

from .latency_stacked_bar import LatencyStackedBar
from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface


class StackedBarPlot(PlotBase):
    """Class that provides API of Stacked Bar graph."""

    def __init__(
        self,
        metrics: LatencyStackedBar,
        visualize_lib: VisualizeLibInterface,
        case: str = 'all',
    ) -> None:
        self._metrics = metrics
        self._visualize_lib = visualize_lib
        self._case = case

    def figure(
        self,
        xaxis_type: str | None = 'system_time',
        ywheel_zoom: bool | None = True,
        full_legends: bool | None = False,
    ) -> Figure:

        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else True
        full_legends = full_legends if full_legends is not None else False

        return self._visualize_lib.stacked_bar(
            self._metrics,
            xaxis_type,
            ywheel_zoom,
            full_legends,
            self._case,
        )

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        # return super().to_dataframe(xaxis_type)
        return self._metrics.to_dataframe(xaxis_type)
