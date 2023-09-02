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

from bokeh.plotting import Figure

from .callback_scheduling import BokehCallbackSched
from .message_flow import BokehMessageFlow
from .response_time_hist import BokehResponseTimeHist
from .stacked_bar import BokehStackedBar
from .timeseries import BokehTimeSeries
from ..visualize_lib_interface import VisualizeLibInterface
from ...metrics_base import MetricsBase
from ....runtime import CallbackBase, CallbackGroup, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path

logger = getLogger(__name__)


class Bokeh(VisualizeLibInterface):
    """Class that visualizes data using Bokeh library."""

    def __init__(self) -> None:
        pass

    def response_time_hist(
        self,
        target_paths: Sequence[Path],
        case: str,
        binsize_ns: int,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
    ) -> Figure:
        response_time_hist = BokehResponseTimeHist(
            target_paths, case, binsize_ns, xaxis_type, ywheel_zoom, full_legends
        )
        return response_time_hist.create_figure()

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
        case: str,  # best or worst
    ) -> Figure:
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
            Parameter specifying best, worst or all. Use to create Responsetime timeseries graph.


        Returns
        -------
        bokeh.plotting.Figure
            Figure of timeseries.

        """
        timeseries = BokehTimeSeries(metrics, xaxis_type, ywheel_zoom, full_legends, case)
        return timeseries.create_figure()
