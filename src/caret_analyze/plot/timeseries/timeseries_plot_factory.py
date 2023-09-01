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

from .frequency_timeseries import FrequencyTimeSeries
from .latency_timeseries import LatencyTimeSeries
from .period_timeseries import PeriodTimeSeries
from .response_time_timeseries import ResponsetimeTimeSeries
from .timeseries_plot import TimeSeriesPlot
from ..metrics_base import MetricsBase
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Path, Publisher, Subscription

TimeSeriesPlotTypes = CallbackBase | Communication | (Publisher | Subscription) | Path


class TimeSeriesPlotFactory:
    """Factory class to create an instance of TimeSeriesPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        target_objects: Sequence[TimeSeriesPlotTypes],
        metrics: str,
        visualize_lib: VisualizeLibInterface,
        case: str
    ) -> TimeSeriesPlot:
        """
        Create an instance of TimeSeriesPlot.

        Parameters
        ----------
        target_objects : Sequence[TimeSeriesPlotTypes]
            TimeSeriesPlotTypes = CallbackBase | Communication | (Publisher | Subscription)
        metrics : str
            Metrics for timeseries data.
            supported metrics: [frequency/latency/period]
        visualize_lib : VisualizeLibInterface
            Instance of VisualizeLibInterface used for visualization.
        case : str
            Parameter specifying best, worst or all. Use to create Responsetime timeseries graph.

        Returns
        -------
        TimeSeriesPlot

        Raises
        ------
        UnsupportedTypeError
            Argument metrics is not "frequency", "latency", or "period".

        """
        metrics_: MetricsBase
        if metrics == 'frequency':
            metrics_ = FrequencyTimeSeries(list(target_objects))
            return TimeSeriesPlot(metrics_, visualize_lib)
        elif metrics == 'latency':
            # Ignore the mypy type check because type_check_decorator is applied.
            metrics_ = LatencyTimeSeries(list(target_objects))  # type: ignore
            return TimeSeriesPlot(metrics_, visualize_lib)
        elif metrics == 'period':
            metrics_ = PeriodTimeSeries(list(target_objects))
            return TimeSeriesPlot(metrics_, visualize_lib)
        elif metrics == 'response_time':
            metrics_ = ResponsetimeTimeSeries(list(target_objects), case)
            return TimeSeriesPlot(metrics_, visualize_lib, case)
        else:
            raise UnsupportedTypeError(
                'Unsupported metrics specified. '
                'Supported metrics: [frequency/latency/period]'
            )
