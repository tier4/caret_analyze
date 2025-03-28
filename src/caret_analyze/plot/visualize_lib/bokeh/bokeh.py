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

from collections.abc import Sequence
from logging import getLogger

from bokeh import __version__ as bokeh_version
from bokeh.models import HoverTool
from bokeh.models.renderers import GlyphRenderer

from bokeh.plotting import figure as Figure

import numpy as np
from packaging import version

from .callback_scheduling import BokehCallbackSched
from .message_flow import BokehMessageFlow
from .stacked_bar import BokehStackedBar
from .timeseries import BokehTimeSeries
from .util import ColorSelectorFactory, LegendManager
from ..visualize_lib_interface import VisualizeLibInterface
from ...metrics_base import MetricsBase
from ....runtime import CallbackBase, CallbackGroup, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path
HistTypes = CallbackBase | Communication | Path | Publisher | Subscription


logger = getLogger(__name__)


class Bokeh(VisualizeLibInterface):
    """Class that visualizes data using Bokeh library."""

    def __init__(self) -> None:
        self._legend_items: list[tuple[str, list[GlyphRenderer]]] = []

    def message_flow(
        self,
        target_path: Path,
        xaxis_type: str,
        ywheel_zoom: bool,
        granularity: str,
        treat_drop_as_delay: bool,
        lstrip_s: float,
        rstrip_s: float
    ) -> Figure:
        """
        Get message flow figure.

        Parameters
        ----------
        target_path : Path
            The target path.
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        granularity : str
            Granularity.
        treat_drop_as_delay : bool
            Treat drop as delay.
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of message flow.

        """
        message_flow = BokehMessageFlow(
            target_path, xaxis_type, ywheel_zoom, granularity,
            treat_drop_as_delay, lstrip_s, rstrip_s,
        )
        return message_flow.create_figure()

    def stacked_bar(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,  # all, best, worst or worst-with-external-latency
    ) -> Figure:
        """
        Get a stacked bar figure.

        Parameters
        ----------
        metrics : MetricsBase
            Metrics to be y-axis in visualization.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        case : str
            Parameter specifying all, best, worst, or worst-with-external-latency.
            Use to create Response time timeseries graph.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of stacked bar.

        """
        stacked_bar = BokehStackedBar(metrics, xaxis_type, ywheel_zoom, full_legends, case)
        return stacked_bar.create_figure()

    def callback_scheduling(
        self,
        callback_groups: Sequence[CallbackGroup],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        coloring_rule: str,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> Figure:
        """
        Get callback scheduling figure.

        Parameters
        ----------
        callback_groups : Sequence[CallbackGroup]
            The target callback groups.
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool, optional
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        coloring_rule : str, optional
            The unit of color change
            There are there rules which are [callback/callback_group/node], by default 'callback'
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of callback scheduling.

        """
        callback_scheduling = BokehCallbackSched(
            callback_groups, xaxis_type, ywheel_zoom,
            full_legends, coloring_rule, lstrip_s, rstrip_s
        )
        return callback_scheduling.create_figure()

    def timeseries(
        self,
        metrics: MetricsBase,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str
    ) -> Figure:
        """
        Get a timeseries figure.

        Parameters
        ----------
        metrics : MetricsBase
            Metrics to be y-axis in visualization.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        case : str
            Parameter specifying all, best, worst, or worst-with-external-latency.
            Use to create Response time timeseries graph.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of timeseries.

        """
        timeseries = BokehTimeSeries(metrics, xaxis_type, ywheel_zoom, full_legends, case)
        return timeseries.create_figure()

    def histogram(
        self,
        hist_list: list[list[int]],
        bins: list[float],
        target_objects: Sequence[HistTypes],
        metrics_name: str,
        case: str | None = None
    ) -> Figure:
        """
        Get a histogram figure.

        Parameters
        ----------
        hist_list : list[list[int]]
            Data array of histogram to be visualized.
        bins : list[float]
            Data array of bins of histogram.
        target_objects : list[CallbackBase | Communication | Path]
            Object array to be visualized.
        metrics_name : str
            Name of metrics.
            "frequency", "latency", "period" or "response_time" can be specified.
        case : str
            Parameter specifying all, best, worst, or worst-with-external-latency.
            Use to create Response time histogram graph.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of histogram.

        Raises
        ------
        NotImplementedError
            Argument metrics_name is not "frequency", "period", "latency", or "response_time".

        """
        legend_manager = LegendManager()
        if metrics_name == 'frequency':
            x_label = metrics_name + ' [Hz]'
        elif metrics_name in ['period', 'latency', 'response_time']:
            x_label = metrics_name + ' [ms]'
        else:
            raise NotImplementedError()

        plot: Figure = Figure(
            title=f'Histogram of {metrics_name}'
            if case is None else f'Histogram of {metrics_name} --- {case} case ---',
            x_axis_label=x_label, y_axis_label='The number of samples', width=800
            )

        hists_t = np.array(hist_list).T

        color_selector = ColorSelectorFactory.create_instance('unique')
        colors = [color_selector.get_color() for _ in target_objects]

        quad_dicts: dict = {t: [] for t in target_objects}
        for i, h in enumerate(hists_t):
            data = list(zip(h, colors, target_objects))
            data.sort(key=lambda x: x[0], reverse=True)
            for top, color, target_object in data:
                if top == 0:
                    continue
                quad = plot.quad(
                    top=top, bottom=0, left=bins[i], right=bins[i+1],
                    color=color, alpha=1, line_color='white'
                    )
                if version.parse(bokeh_version) >= version.parse('3.4.0'):
                    hover = HoverTool(
                        tooltips=[(x_label, f'{bins[i]}'), ('The number of samples', f'{top}')],
                        renderers=[quad],
                        visible=False
                        )
                else:
                    hover = HoverTool(
                        tooltips=[(x_label, f'{bins[i]}'), ('The number of samples', f'{top}')],
                        renderers=[quad],
                        toggleable=False
                    )
                plot.add_tools(hover)
                quad_dicts[target_object] = quad_dicts[target_object] + [quad]

        for target_object_key, quad_value in quad_dicts.items():
            legend_manager.add_legend(target_object_key, quad_value)

        legends = legend_manager.create_legends(20, False, location='top_right', separate=20)
        for legend in legends:
            plot.add_layout(legend, 'right')
            plot.legend.click_policy = 'hide'
        return plot
