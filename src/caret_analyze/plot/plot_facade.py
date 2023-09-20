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

from collections.abc import Collection
from logging import getLogger

from .callback_scheduling import CallbackSchedulingPlot, CallbackSchedulingPlotFactory
from .histogram import HistogramPlotFactory, ResponseTimeHistPlot, ResponseTimeHistPlotFactory
from .message_flow import MessageFlowPlot, MessageFlowPlotFactory
from .plot_base import PlotBase
from .stacked_bar import StackedBarPlot, StackedBarPlotFactory
from .timeseries import TimeSeriesPlotFactory
from .visualize_lib import VisualizeLibFactory
from ..runtime import (Application, CallbackBase, CallbackGroup, Communication, Executor, Node,
                       Path, Publisher, Subscription)

logger = getLogger(__name__)

TimeSeriesTypes = CallbackBase | Communication | Publisher | Subscription | Path
HistTypes = CallbackBase | Communication
CallbackSchedTypes = (Application | Executor | Path |
                      Node | CallbackGroup | Collection[CallbackGroup])


def parse_collection_or_unpack(
    target_arg: tuple[Collection[TimeSeriesTypes]] | tuple[TimeSeriesTypes, ...]
) -> list[TimeSeriesTypes]:
    """
    Parse target argument.

    To address both cases where the target argument is passed in collection type
    or unpacked, this function converts them to the same list format.

    Parameters
    ----------
    target_arg : tuple[Collection[TimeSeriesTypes]] | tuple[TimeSeriesTypes, ...]
        Target objects.

    Returns
    -------
    list[TimeSeriesTypes]

    """
    parsed_target_objects: list[TimeSeriesTypes]
    if isinstance(target_arg[0], Collection):
        assert len(target_arg) == 1
        parsed_target_objects = list(target_arg[0])
    else:  # Unpacked case
        parsed_target_objects = list(target_arg)  # type: ignore

    return parsed_target_objects


def parse_collection_or_unpack_for_hist(
    target_arg: tuple[Collection[HistTypes]] | tuple[HistTypes, ...]
) -> list[HistTypes]:
    """
    Parse target argument.

    To address both cases where the target argument is passed in collection type
    or unpacked, this function converts them to the same list format.

    Parameters
    ----------
    target_arg : tuple[Collection[HistTypes]] | tuple[HistTypes, ...]
        Target objects.

    Returns
    -------
    list[HistTypes]

    """
    parsed_target_objects: list[HistTypes]
    if isinstance(target_arg[0], Collection):
        assert len(target_arg) == 1
        parsed_target_objects = list(target_arg[0])
    else:  # Unpacked case
        parsed_target_objects = list(target_arg)  # type: ignore

    return parsed_target_objects


class Plot:
    """Facade class for plot."""

    @staticmethod
    def create_response_time_stacked_bar_plot(
        target_object: Path,
        metrics: str = 'latency',
        case: str = 'worst'
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
            Response time calculation method, worst by default.
            supported case: [best/worst].

        Returns
        -------
        StackedBarPlot

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
    def create_period_timeseries_plot(
        *target_objects: TimeSeriesTypes
    ) -> PlotBase:
        """
        Get period timeseries plot instance.

        Parameters
        ----------
        target_objects : Collection[TimeSeriesTypes]
            TimeSeriesTypes = CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            parse_collection_or_unpack(target_objects), 'period', visualize_lib
        )
        return plot

    @staticmethod
    def create_frequency_timeseries_plot(
        *target_objects: TimeSeriesTypes
    ) -> PlotBase:
        """
        Get frequency timeseries plot instance.

        Parameters
        ----------
        target_objects : Collection[TimeSeriesTypes]
            TimeSeriesTypes = CallbackBase | Communication | Publisher | Subscription
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            parse_collection_or_unpack(target_objects), 'frequency', visualize_lib
        )
        return plot

    @staticmethod
    def create_latency_timeseries_plot(
        *target_objects: CallbackBase | Communication
    ) -> PlotBase:
        """
        Get latency timeseries plot instance.

        Parameters
        ----------
        target_objects : Collection[CallbackBase | Communication]
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            parse_collection_or_unpack(target_objects), 'latency', visualize_lib
        )
        return plot

    def create_response_time_timeseries_plot(
        *target_objects: Path,
        case: str = 'best'
    ) -> PlotBase:
        """
        Get response time timeseries plot instance.

        Parameters
        ----------
        target_objects : Collection[Path]
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.
        case: str, optional
            Response time calculation method, best by default.
            supported case: [best/worst/all].

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            parse_collection_or_unpack(target_objects), 'response_time', visualize_lib, case
        )
        return plot

    @staticmethod
    def create_response_time_histogram_plot(
        *paths: Path,
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000
    ) -> ResponseTimeHistPlot:
        """
        Get ResponseTimePlot instance.

        Parameters
        ----------
        paths : Collection[Path]
            Target path.
            This also accepts multiple path inputs by unpacking.
        case : str, optional
            Response time calculation method, best-to-worst by default.
            supported case: [best-to-worst/best/worst].
        binsize_ns : int, optional
            binsize [ns], 1000000 by default.

        Returns
        -------
        ResponseTimePlot

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = ResponseTimeHistPlotFactory.create_instance(
            visualize_lib, list(paths), case, int(binsize_ns)
        )
        return plot

    @staticmethod
    def create_callback_scheduling_plot(
        target_objects: CallbackSchedTypes,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> CallbackSchedulingPlot:
        """
        Get CallbackSchedulingPlot instance.

        Parameters
        ----------
        target_objects : CallbackSchedTypes
            CallbackSchedTypes = (Application | Executor | Path | Node |
                                  CallbackGroup | Collection[CallbackGroup])
            Instances that are the sources of the plotting.
        lstrip_s : float, optional
            Start time of cropping range, 0 by default.
        rstrip_s: float, optional
            End point of cropping range, 0 by default.

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

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = MessageFlowPlotFactory.create_instance(
            target_path, visualize_lib, granularity, treat_drop_as_delay, lstrip_s, rstrip_s
        )
        return plot

    @staticmethod
    def create_frequency_histogram_plot(
        *target_objects: HistTypes
    ) -> PlotBase:
        """
        Get frequency histogram plot instance.

        Parameters
        ----------
        target_objects : Collection[HistTypes]
            HistTypes = CallbackBase | Communication
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(
            parse_collection_or_unpack_for_hist(target_objects),
            'frequency', visualize_lib
        )
        return plot

    @staticmethod
    def create_latency_histogram_plot(
        *target_objects: HistTypes
    ) -> PlotBase:
        """
        Get latency histogram plot instance.

        Parameters
        ----------
        target_objects : Collection[HistTypes]
            HistTypes = CallbackBase | Communication
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(
            parse_collection_or_unpack_for_hist(target_objects),
            'latency', visualize_lib
        )
        return plot

    @staticmethod
    def create_period_histogram_plot(
        *target_objects: HistTypes
    ) -> PlotBase:
        """
        Get period histogram plot instance.

        Parameters
        ----------
        target_objects : Collection[HistTypes]
            HistTypes = CallbackBase | Communication
            Instances that are the sources of the plotting.
            This also accepts multiple inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = HistogramPlotFactory.create_instance(
            parse_collection_or_unpack_for_hist(target_objects),
            'period', visualize_lib
        )
        return plot
