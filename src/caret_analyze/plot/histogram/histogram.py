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

from logging import getLogger
from typing import Optional, Sequence

from bokeh.plotting import Figure, figure

import pandas as pd

from caret_analyze.record import ResponseTime

from ..plot_base import PlotBase
from ..plot_util import PlotColorSelector
from ..visualize_lib import VisualizeLibInterface
from ...runtime import Path

logger = getLogger(__name__)


# TODO: Migrate drawing process to visualize_lib
class ResponseTimeHistPlot(PlotBase):

    def __init__(
        self,
        visualize_lib: VisualizeLibInterface,
        target: Sequence[Path],
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000
    ) -> None:
        self._visulize_lib = visualize_lib
        self._target = list(target)
        self._case = case
        self._binsize_ns = binsize_ns

    def to_dataframe(self, xaxis_type: str) -> pd.DataFrame:
        logger.warning("'to_dataframe' method is not implemented in ResponseTimeHistPlot.")
        return pd.DataFrame()

    def figure(
        self,
        xaxis_type: Optional[str] = None,
        ywheel_zoom: Optional[bool] = None,
        full_legends: Optional[bool] = None
    ) -> Figure:
        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else True

        p = figure(plot_width=600,
                   plot_height=400,
                   active_scroll='wheel_zoom',
                   x_axis_label='Response Time [ms]',
                   y_axis_label='Probability')
        color_selector = PlotColorSelector()
        for _, path in enumerate(self._target):
            records = path.to_records()
            response = ResponseTime(records)

            if self._case == 'best-to-worst':
                hist, bins = response.to_histogram(self._binsize_ns)
            elif self._case == 'best':
                hist, bins = response.to_best_case_histogram(self._binsize_ns)
            elif self._case == 'worst':
                hist, bins = response.to_worst_case_histogram(self._binsize_ns)

            hist = hist / sum(hist)

            bins = bins*10**-6
            color = color_selector.get_color(path.path_name)
            p.quad(top=hist, bottom=0, left=bins[:-1], right=bins[1:],
                   color=color, alpha=0.5, legend_label=f'{path.path_name}')

        return p
