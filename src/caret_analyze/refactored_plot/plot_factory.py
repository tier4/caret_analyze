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

from typing import Collection, Union

from multimethod import multimethod as singledispatchmethod

from .frequency_timeseries_plot import FrequencyTimeSeriesPlot
from .plot_interface import PlotInterface
from ..exceptions import InvalidArgumentError
from ..runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class Plot:

    @singledispatchmethod
    def create_period_timeseries_plot(arg) -> PlotInterface:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_period_timeseries_plot.register
    def _create_period_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotInterface:
        return PeriodTimeSeriesPlot(target_objects)

    @staticmethod
    @create_period_timeseries_plot.register
    def _create_period_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotInterface:
        return PeriodTimeSeriesPlot(target_objects)

    @singledispatchmethod
    def create_frequency_timeseries_plot(arg) -> PlotInterface:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_frequency_timeseries_plot.register
    def _create_frequency_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotInterface:
        return FrequencyTimeSeriesPlot(target_objects)

    @staticmethod
    @create_frequency_timeseries_plot.register
    def _create_frequency_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotInterface:
        return FrequencyTimeSeriesPlot(target_objects)

    @singledispatchmethod
    def create_latency_timeseries_plot(arg) -> PlotInterface:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_latency_timeseries_plot.register
    def _create_latency_timeseries_plot(
        target_objects: Collection[TimeSeriesTypes]
    ) -> PlotInterface:
        return LatencyTimeSeriesPlot(target_objects)

    @staticmethod
    @create_latency_timeseries_plot.register
    def _create_latency_timeseries_plot_tuple(
        *target_objects: TimeSeriesTypes
    ) -> PlotInterface:
        return LatencyTimeSeriesPlot(target_objects)
