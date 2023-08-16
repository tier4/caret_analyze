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

from .histogram_plot import HistogramPlot
from ..metrics_base import MetricsBase
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication
from caret_analyze.record import Frequency, Latency, Period

# TimeSeriesPlotTypes = CallbackBase | Communication | (Publisher | Subscription)
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
        callback_name = target_objects[0].callback_name
        # target_objects = target_objects[0]
            # Ignore the mypy type check because type_check_decorator is applied.
        # metrics_ = Latency(target_objects.to_records())  # type: ignore
        # return HistogramPlot(metrics_, visualize_lib)


        #mertix_ をターゲットオブジェクトが複数ある場合を想定して、metrix_配列を複数作る
        #histgram_plotはmetrix_配列を受け取る
        #
        #引数をそれぞれ変更する
    
        if metrics == 'frequency':
            metrics_ = Frequency(target_objects.to_records())
            return HistogramPlot(metrics_, visualize_lib, callback_name, metrics)
        elif metrics == 'latency':
            # Ignore the mypy type check because type_check_decorator is applied.
            # metrics_ = Latency(target_objects.to_records())  # type: ignore
            for target_object in target_objects:
                temp = Latency(target_object.to_records())
                metrics_.append(temp)
            return HistogramPlot(metrics_, visualize_lib, callback_name, metrics)
        elif metrics == 'period':
            metrics_ = Period(target_objects.to_records())
            return HistogramPlot(metrics_, visualize_lib, callback_name, metrics)
        else:
            raise UnsupportedTypeError(
                'Unsupported metrics specified. '
                'Supported metrics: [frequency/latency/period]'
            )
