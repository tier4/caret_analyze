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

from bokeh.plotting import figure, show

from caret_analyze.exceptions import UnsupportedTypeError

from caret_analyze.record import ResponseTime

from .histogram_interface import HistPlot

from .plot_util import PlotColorSelector


class ResponseTimePlot(HistPlot):

    def __init__(
        self,
        target,
        case='best-to-worst',
        binsize_ns=10000000
    ):
        if case not in ['best-to-worst', 'best', 'worst']:
            raise UnsupportedTypeError(
                f'Unsupported "case". case = {case}.'
                'supported "case": [best-to-worst/best/worst]'
            )
        super().__init__(target)
        self._case = case
        self._binsize_ns = binsize_ns

    def _show_core(self):
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
        show(p)
        return p
