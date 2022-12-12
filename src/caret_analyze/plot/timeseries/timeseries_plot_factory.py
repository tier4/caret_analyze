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

from typing import Sequence, Union

from .frequency_timeseries import FrequencyTimeSeries
from .latency_timeseries import LatencyTimeSeries
from .period_timeseries import PeriodTimeSeries
from .timeseries_plot import TimeSeriesPlot
from ..metrics_base import MetricsBase
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesPlotTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class TimeSeriesPlotFactory:
    """Factory class to create an instance of TimeSeriesPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        target_objects: Sequence[TimeSeriesPlotTypes],
        metrics: str,
        visualize_lib: VisualizeLibInterface
    ) -> TimeSeriesPlot:
        """
        Create an instance of TimeSeriesPlot.

        Parameters
        ----------
        target_objects : Sequence[TimeSeriesPlotTypes]
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
        metrics : str
            Metrics for timeseries data.
            supported metrics: [frequency/latency/period]
        visualize_lib : VisualizeLibInterface
            Instance of VisualizeLibInterface used for visualization.

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
        else:
            raise UnsupportedTypeError(
                'Unsupported metrics specified. '
                'Supported metrics: [frequency/latency/period]'
            )
