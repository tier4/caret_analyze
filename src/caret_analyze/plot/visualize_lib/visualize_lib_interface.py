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

from abc import ABCMeta, abstractmethod
from collections.abc import Sequence

from bokeh.plotting import figure as Figure

from ..metrics_base import MetricsBase
from ...record import Frequency, Latency, Period, ResponseTime
from ...runtime import CallbackBase, CallbackGroup, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription)
MetricsTypes = Frequency | Latency | Period | ResponseTime
HistTypes = CallbackBase | Communication | Path | Publisher | Subscription


class VisualizeLibInterface(metaclass=ABCMeta):
    """Interface class for VisualizeLib."""

    @abstractmethod
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
        Get message flow.

        Parameters
        ----------
        target_path : Path
            Target path.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        granularity : str
            Granularity.
        treat_drop_as_delay : bool
            Treat drop as delay.
        lstrip_s : float, optional
            Start time of cropping range.
        rstrip_s: float, optional
            End point of cropping range.

        Returns
        -------
        Figure
            Figure of message flow.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    @abstractmethod
    def callback_scheduling(
        self,
        callback_groups: Sequence[CallbackGroup],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        coloring_rule: str,
        lstrip_s: float,
        rstrip_s: float
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
            Start time of cropping range.
        rstrip_s: float, optional
            End point of cropping range.

        Returns
        -------
        Figure
            Figure of callback scheduling.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    @abstractmethod
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
        Figure
            Figure of timeseries.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    @abstractmethod
    def stacked_bar(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,
    ) -> Figure:
        """
        Get stacked bar.

        Parameters
        ----------
        metrics : MetricsBase
            Metrics.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold, by default False.
        case : str
            Parameter specifying all, best, worst, or worst-with-external-latency.
            Use to create Response time timeseries graph.

        Returns
        -------
        Figure
            Figure of stacked bar.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

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
        Figure
            Figure of histogram.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()
