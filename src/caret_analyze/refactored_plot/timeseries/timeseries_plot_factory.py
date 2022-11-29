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

from .frequency_timeseries_plot import FrequencyTimeSeriesPlot
from .latency_timeseries_plot import LatencyTimeSeriesPlot
from .period_timeseries_plot import PeriodTimeSeriesPlot
from .timeseries_plot_interface import TimeSeriesInterface
from ..visualize_lib import VisualizeInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesPlotTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class TimeSeriesPlotFactory:

    @staticmethod
    def create_instance(
        target_objects: Collection[TimeSeriesPlotTypes],
        metrics: str,
        visualize_lib: VisualizeInterface
    ) -> TimeSeriesInterface:
        if metrics == 'frequency':
            return FrequencyTimeSeriesPlot(list(target_objects), visualize_lib)
        elif metrics == 'latency':
            return LatencyTimeSeriesPlot(list(target_objects), visualize_lib)
        elif metrics == 'period':
            return PeriodTimeSeriesPlot(list(target_objects), visualize_lib)
        else:
            raise NotImplementedError()
