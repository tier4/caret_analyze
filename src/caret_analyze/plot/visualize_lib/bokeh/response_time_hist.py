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

from bokeh.plotting import figure as Figure

from caret_analyze.record import ResponseTime

from .util import ColorSelectorFactory, init_figure
from ....runtime import Path


class BokehResponseTimeHist:

    def __init__(
        self,
        target_paths: Sequence[Path],
        case: str,
        binsize_ns: int,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
    ) -> None:
        self._target_paths = target_paths
        self._case = case
        self._binsize_ns = binsize_ns
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._full_legends = full_legends

    def create_figure(self) -> Figure:
        fig = init_figure('Histogram of response time [ms]', self._ywheel_zoom,
                          self._xaxis_type, 'Probability', 'Response Time [ms]')

        color_selector = ColorSelectorFactory.create_instance('unique')
        for path in self._target_paths:
            response = ResponseTime(path.to_records())

            if self._case == 'best-to-worst':
                hist, bins = response.to_histogram(self._binsize_ns)
            elif self._case == 'best':
                hist, bins = response.to_best_case_histogram(self._binsize_ns)
            elif self._case == 'worst':
                hist, bins = response.to_worst_case_histogram(self._binsize_ns)

            hist = hist / sum(hist)
            bins = bins*10**-6
            fig.quad(
                top=hist, bottom=0, left=bins[:-1], right=bins[1:],
                color=color_selector.get_color(), alpha=0.5, legend_label=f'{path.path_name}'
            )

        return fig
