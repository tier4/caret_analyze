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
from typing import Collection, Optional, Union

from multimethod import multimethod as singledispatchmethod

from .callback_scheduling import CallbackSchedulingPlot, CallbackSchedulingPlotFactory
from .histogram import ResponseTimeHistPlot
from .message_flow import MessageFlowPlot, MessageFlowPlotFactory
from .plot_base import PlotBase
from .stacked_bar import StackedBarPlotFactory
from .timeseries import TimeSeriesPlotFactory
from .visualize_lib import VisualizeLibFactory
from ..runtime import (Application, CallbackBase, CallbackGroup, Communication, Executor, Node,
                       Path, Publisher, Subscription)

logger = getLogger(__name__)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]
CallbackSchedTypes = Union[Application, Executor, Path,
                           Node, CallbackGroup, Collection[CallbackGroup]]


class Plot:
    """Facade class for plot."""

    @singledispatchmethod
    def create_period_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotBase:
        """
        Get period timeseries plot instance.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'period', visualize_lib
        )
        return plot

    @staticmethod
    def create_response_time_stacked_bar_plot(
        target_object: Path,
        metrics: str = 'latency',
        case: str = 'worst'
    ):
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = StackedBarPlotFactory.create_instance(
            target_object,
            visualize_lib,
            metrics,
            case,
        )
        return plot

    @staticmethod
    @create_period_timeseries_plot.register
    def _create_period_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotBase:
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'period', visualize_lib
        )
        return plot

    @singledispatchmethod
    def create_frequency_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotBase:
        """
        Get frequency timeseries plot instance.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'frequency', visualize_lib
        )
        return plot

    @staticmethod
    @create_frequency_timeseries_plot.register
    def _create_frequency_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotBase:
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'frequency', visualize_lib
        )
        return plot

    @singledispatchmethod
    def create_latency_timeseries_plot(
        target_objects: Collection[Union[CallbackBase, Communication]]
    ) -> PlotBase:
        """
        Get latency timeseries plot instance.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'latency', visualize_lib
        )
        return plot

    @staticmethod
    @create_latency_timeseries_plot.register
    def _create_latency_timeseries_plot_tuple(
        *target_objects: Union[CallbackBase, Communication]
    ) -> PlotBase:
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'latency', visualize_lib
        )
        return plot

    @singledispatchmethod
    def create_response_time_histogram_plot(
        paths: Collection[Path],
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000
    ) -> ResponseTimeHistPlot:
        """
        Get ResponseTimePlot instance.

        Parameters
        ----------
        path : Collection[Path]
            Target path.
            This also accepts multiple path inputs by unpacking.
        case : str, optional
            response time calculation method, by default best-to-worst.
            supported case: [best-to-worst/best/worst].
        binsize_ns : int, optional
            binsize [ns], by default 1000000.

        Returns
        -------
        ResponseTimePlot

        """
        return ResponseTimeHistPlot(list(paths), case, int(binsize_ns))

    @staticmethod
    @create_response_time_histogram_plot.register
    def _create_response_time_histogram_plot_tuple(
        *paths: Path,
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000
    ) -> ResponseTimeHistPlot:
        return ResponseTimeHistPlot(list(paths), case, int(binsize_ns))

    @singledispatchmethod
    def create_callback_scheduling_plot(  # type: ignore
        target_objects: CallbackSchedTypes,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> CallbackSchedulingPlot:
        """
        Get CallbackSchedulingPlot instance.

        Parameters
        ----------
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        CallbackSchedulingPlot

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        if isinstance(target_objects, (tuple, set)):
            target_objects = list(target_objects)
        plot = CallbackSchedulingPlotFactory.create_instance(
            target_objects, visualize_lib, lstrip_s, rstrip_s
        )
        return plot

    @staticmethod
    @create_callback_scheduling_plot.register
    def _create_callback_scheduling_plot_tuple(
        *target_objects: CallbackGroup,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> PlotBase:
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = CallbackSchedulingPlotFactory.create_instance(
            list(target_objects), visualize_lib, lstrip_s, rstrip_s
        )
        return plot

    @staticmethod
    def create_message_flow_plot(
        target_path: Path,
        granularity: Optional[str] = None,
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
        granularity : Optional[str], optional
            Granularity of chain with two value; [raw/node], by default None.
        treat_drop_as_delay : bool, optional
            If there is a drop, the flow is drawn by connecting it to the next one,
            by default False.
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        MessageFlowPlot

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = MessageFlowPlotFactory.create_instance(
            target_path, visualize_lib, granularity, treat_drop_as_delay, lstrip_s, rstrip_s
        )
        return plot
