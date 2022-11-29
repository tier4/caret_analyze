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

from .plot_interface import PlotInterface
from .timeseries import TimeSeriesPlotFactory
from .visualize_lib import VisualizeLib
from ..common import type_check_decorator
from ..runtime import CallbackBase, Communication, Publisher, Subscription

logger = getLogger(__name__)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class Plot:

    @singledispatchmethod
    @type_check_decorator
    def create_period_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'period', visualize_lib
        )
        return plot

    @staticmethod
    @create_period_timeseries_plot.register
    @type_check_decorator
    def _create_period_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'period', visualize_lib
        )
        return plot

    @singledispatchmethod
    @type_check_decorator
    def create_frequency_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'frequency', visualize_lib
        )
        return plot

    @staticmethod
    @create_frequency_timeseries_plot.register
    @type_check_decorator
    def _create_frequency_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'frequency', visualize_lib
        )
        return plot

    @singledispatchmethod
    @type_check_decorator
    def create_latency_timeseries_plot(
        target_objects: Collection[Union[CallbackBase, Communication]]
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'latency', visualize_lib
        )
        return plot

    @staticmethod
    @create_latency_timeseries_plot.register
    @type_check_decorator
    def _create_latency_timeseries_plot_tuple(
        *target_objects: Union[CallbackBase, Communication]
    ) -> PlotInterface:
        visualize_lib = VisualizeLib.create_instance()
        plot = TimeSeriesPlotFactory.create_instance(
            list(target_objects), 'latency', visualize_lib
        )
        return plot
