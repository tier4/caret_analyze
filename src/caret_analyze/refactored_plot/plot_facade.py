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
from typing import Collection, Union

from multimethod import multimethod as singledispatchmethod

from .histogram import ResponseTimePlot
from .plot_base import PlotBase
from .timeseries import TimeSeriesPlotFactory
from .visualize_lib import VisualizeLibFactory
from ..runtime import CallbackBase, Communication, Path, Publisher, Subscription

logger = getLogger(__name__)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class Plot:

    @singledispatchmethod
    def create_period_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotBase:
        visualize_lib = VisualizeLibFactory.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'period', visualize_lib
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
    ) -> ResponseTimePlot:
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
        return ResponseTimePlot(list(paths), case, int(binsize_ns))

    @staticmethod
    @create_response_time_histogram_plot.register
    def _create_response_time_histogram_plot_tuple(
        *paths: Path,
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000
    ) -> ResponseTimePlot:
        return ResponseTimePlot(list(paths), case, int(binsize_ns))

    # ---------- Previous Interface ----------
    @singledispatchmethod
    def create_callback_frequency_plot(
        callbacks: Collection[CallbackBase]
    ) -> PlotBase:
        """
        Get callback frequency plot.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        return Plot.create_frequency_timeseries_plot(callbacks)

    @staticmethod
    @create_callback_frequency_plot.register
    def _create_callback_frequency_plot_tuple(
        *callbacks: CallbackBase
    ) -> PlotBase:
        return Plot.create_frequency_timeseries_plot(callbacks)

    @singledispatchmethod
    def create_callback_period_plot(
        callbacks: Collection[CallbackBase]
    ) -> PlotBase:
        """
        Get callback period plot.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        return Plot.create_period_timeseries_plot(callbacks)

    @staticmethod
    @create_callback_period_plot.register
    def _create_callback_period_plot_tuple(
        *callbacks: CallbackBase
    ) -> PlotBase:
        return Plot.create_period_timeseries_plot(callbacks)

    @singledispatchmethod
    def create_callback_jitter_plot(
        callbacks: Collection[CallbackBase]
    ) -> PlotBase:
        logger.warning('create_callback_jitter_plot is deprecated.'
                       'Use create_callback_period_plot')
        return Plot.create_callback_period_plot(callbacks)

    @staticmethod
    @create_callback_jitter_plot.register
    def _create_callback_jitter_plot_tuple(
        *callbacks: CallbackBase
    ) -> PlotBase:
        logger.warning('create_callback_jitter_plot is deprecated.'
                       'Use create_callback_period_plot')
        return Plot.create_callback_period_plot(callbacks)

    @singledispatchmethod
    def create_callback_latency_plot(
        callbacks: Collection[CallbackBase]
    ) -> PlotBase:
        """
        Get callback latency plot.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        PlotBase

        """
        return Plot.create_latency_timeseries_plot(callbacks)

    @staticmethod
    @create_callback_latency_plot.register
    def _create_callback_latency_plot_tuple(
        *callbacks: CallbackBase
    ) -> PlotBase:
        return Plot.create_latency_timeseries_plot(callbacks)

    @singledispatchmethod
    def create_publish_subscription_period_plot(
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> PlotBase:
        return Plot.create_period_timeseries_plot(pub_subs)

    @staticmethod
    @create_publish_subscription_period_plot.register
    def _create_publish_subscription_period_plot_tuple(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PlotBase:
        return Plot.create_period_timeseries_plot(pub_subs)

    @singledispatchmethod
    def create_publish_subscription_frequency_plot(
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> PlotBase:
        return Plot.create_frequency_timeseries_plot(pub_subs)

    @staticmethod
    @create_publish_subscription_frequency_plot.register
    def _create_publish_subscription_frequency_plot_tuple(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PlotBase:
        return Plot.create_frequency_timeseries_plot(pub_subs)

    @singledispatchmethod
    def create_communication_latency_plot(
        communications: Collection[Communication]
    ) -> PlotBase:
        return Plot.create_latency_timeseries_plot(communications)

    @staticmethod
    @create_communication_latency_plot.register
    def _create_communication_latency_plot_tuple(
        *communications: Communication
    ) -> PlotBase:
        return Plot.create_latency_timeseries_plot(communications)

    @singledispatchmethod
    def create_communication_frequency_plot(
        communications: Collection[Communication]
    ) -> PlotBase:
        return Plot.create_frequency_timeseries_plot(communications)

    @staticmethod
    @create_communication_frequency_plot.register
    def _create_communication_frequency_plot_tuple(
        *communications: Communication
    ) -> PlotBase:
        return Plot.create_frequency_timeseries_plot(communications)

    @singledispatchmethod
    def create_communication_period_plot(
        communications: Collection[Communication]
    ) -> PlotBase:
        return Plot.create_period_timeseries_plot(communications)

    @staticmethod
    @create_communication_period_plot.register
    def _create_communication_period_plot_tuple(
        *communications: Communication
    ) -> PlotBase:
        return Plot.create_period_timeseries_plot(communications)
