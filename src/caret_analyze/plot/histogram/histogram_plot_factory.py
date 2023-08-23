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

from caret_analyze.record import Frequency, Latency, Period

from .histogram_plot import HistogramPlot
from ..visualize_lib import VisualizeLibInterface
# from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication

HistogramPlotTypes = CallbackBase | Communication


class HistogramPlotFactory:
    """Factory class to create an instance of TimeSeriesPlot."""

    @staticmethod
    # @type_check_decorator
    def create_instance(
        # target_objects: HistogramPlotTypes,
        target_objects: Sequence[HistogramPlotTypes],
        metrics: str,
        visualize_lib: VisualizeLibInterface
    ) -> HistogramPlot:
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

        Returns
        -------
        TimeSeriesPlot

        Raises
        ------
        UnsupportedTypeError
            Argument metrics is not "frequency", "latency", or "period".

        """
        metrics_: list[Frequency | Latency | Period] = []
        callback_names = [obj.callback_name for obj in target_objects]

        if metrics == 'frequency':
            for target_object in target_objects:
                temp = Frequency(target_object.to_records())
                metrics_.append(temp)
            return HistogramPlot(metrics_, visualize_lib, callback_names, metrics)
        elif metrics == 'latency':
            # Ignore the mypy type check because type_check_decorator is applied.
            for target_object in target_objects:
                temp = Latency(target_object.to_records())
                metrics_.append(temp)
            return HistogramPlot(metrics_, visualize_lib, callback_names, metrics)
        elif metrics == 'period':
            for target_object in target_objects:
                temp = Period(target_object.to_records())
                metrics_.append(temp)
            return HistogramPlot(metrics_, visualize_lib, callback_names, metrics)
        else:
            raise UnsupportedTypeError(
                'Unsupported metrics specified. '
                'Supported metrics: [frequency/latency/period]'
            )
