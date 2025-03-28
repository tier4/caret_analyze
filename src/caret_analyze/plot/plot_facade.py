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

from .callback_scheduling import CallbackSchedulingPlot, CallbackSchedulingPlotFactory
from .histogram import HistogramPlotFactory
from .message_flow import MessageFlowPlot, MessageFlowPlotFactory
from .plot_base import PlotBase
from .stacked_bar import StackedBarPlot, StackedBarPlotFactory
from .timeseries import TimeSeriesPlotFactory
from .visualize_lib import VisualizeLibFactory
from ..common import type_check_decorator
from ..runtime import (Application, CallbackBase, CallbackGroup, Communication, Executor, Node,
                       Path, Publisher, Subscription)

logger = getLogger(__name__)


CallbackGroupTypes = (Application | Executor | Path | Node |
                      CallbackGroup | Sequence[CallbackGroup])


class Plot:
    """Facade class for plot."""

    @staticmethod
    @type_check_decorator
    def create_response_time_stacked_bar_plot(
        target_object: Path,
        metrics: str = 'latency',
        case: str = 'all'
    ) -> StackedBarPlot:
        """
        Get StackedBarPlot instance.

        Parameters
        ----------
        target_object : Path
            Target path
        metrics : str
            Metrics for stacked bar graph.
            supported metrics: [latency]
        case : str, optional
            Response time calculation method, all by default.
            supported case: [all/best/worst/worst-with-external-latency].

        Returns
        -------
        StackedBarPlot
            Created instance of StackedBarPlot.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = StackedBarPlotFactory.create_instance(
            target_object,
            visualize_lib,
            metrics,
            case,
        )
        return plot

    @staticmethod
    @type_check_decorator
    def create_period_timeseries_plot(
        *target_objects: CallbackBase | Communication | Publisher | Subscription
    ) -> PlotBase:
        """
        Get period timeseries plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of period PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(target_objects, 'period', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_frequency_timeseries_plot(
        *target_objects: CallbackBase | Communication | Publisher | Subscription
    ) -> PlotBase:
        """
        Get frequency timeseries plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of frequency PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(target_objects, 'frequency', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_latency_timeseries_plot(
        *target_objects: CallbackBase | Communication
    ) -> PlotBase:
        """
        Get latency timeseries plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of latency PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(target_objects, 'latency', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_response_time_timeseries_plot(
        *target_objects: Path,
        case: str = 'all'
    ) -> PlotBase:
        """
        Get response time timeseries plot instance.

        Parameters
        ----------
        *target_objects : Path
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.
        case: str, optional
            Response time calculation method, all by default.
            supported case: [all/best/worst/worst-with-external-latency].

        Returns
        -------
        PlotBase
            Created instance of response_time PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            target_objects, 'response_time', visualize_lib, case
        )
        return plot

    @staticmethod
    @type_check_decorator
    def create_callback_scheduling_plot(
        target_objects: CallbackGroupTypes,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> CallbackSchedulingPlot:
        """
        Get CallbackSchedulingPlot instance.

        Parameters
        ----------
        target_objects : CallbackGroupTypes
            CallbackGroupTypes = (Application | Executor | Path | Node |
                                  CallbackGroup | Sequence[CallbackGroup])
            Instances that are the sources of the plotting.
        lstrip_s : float, optional
            Start time of cropping range, 0 by default.
        rstrip_s: float, optional
            End point of cropping range, 0 by default.

        Returns
        -------
        CallbackSchedulingPlot
            Created instance of CallbackSchedulingPlot.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        if isinstance(target_objects, (tuple, set)):
            target_objects = list(target_objects)
        plot = CallbackSchedulingPlotFactory.create_instance(
            target_objects, visualize_lib, lstrip_s, rstrip_s
        )
        return plot

    @staticmethod
    @type_check_decorator
    def create_message_flow_plot(
        target_path: Path,
        granularity: str | None = None,
        treat_drop_as_delay: bool = False,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> MessageFlowPlot:
        """
        Get MessageFlowPlot instance.

        Parameters
        ----------
        target_path : Path
            Target path to be plotted.
        granularity : str | None, optional
            Granularity of chain with two value; [raw/node], None by default.
        treat_drop_as_delay : bool, optional
            If there is a drop, the flow is drawn by connecting it to the next one,
            False by default.
        lstrip_s : float, optional
            Start time of cropping range, 0 by default.
        rstrip_s: float, optional
            End point of cropping range, 0 by default.

        Returns
        -------
        MessageFlowPlot
            Created instance of MessageFlowPlot.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = MessageFlowPlotFactory.create_instance(
            target_path, visualize_lib, granularity, treat_drop_as_delay, lstrip_s, rstrip_s
        )
        return plot

    @staticmethod
    @type_check_decorator
    def create_frequency_histogram_plot(
        *target_objects: CallbackBase | Communication | Publisher | Subscription
    ) -> PlotBase:
        """
        Get frequency histogram plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of frequency PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(target_objects, 'frequency', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_latency_histogram_plot(
        *target_objects: CallbackBase | Communication
    ) -> PlotBase:
        """
        Get latency histogram plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of latency PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(target_objects, 'latency', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_period_histogram_plot(
        *target_objects: CallbackBase | Communication | Publisher | Subscription
    ) -> PlotBase:
        """
        Get period histogram plot instance.

        Parameters
        ----------
        *target_objects : CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase
            Created instance of period PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(target_objects, 'period', visualize_lib)
        return plot

    @staticmethod
    @type_check_decorator
    def create_response_time_histogram_plot(
        *target_objects: Path,
        case: str = 'all',
    ) -> PlotBase:
        """
        Get response time histogram plot instance.

        Parameters
        ----------
        *target_objects : Path
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.
        case: str, optional
            Response time calculation method, all by default.
            supported case: [all/best/worst/worst-with-external-latency].

        Returns
        -------
        PlotBase
            Created instance of response_time PlotBase.

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(
            target_objects, 'response_time', visualize_lib, case
        )
        return plot
