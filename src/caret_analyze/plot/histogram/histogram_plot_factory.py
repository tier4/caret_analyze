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

from caret_analyze.record import Frequency, Latency, Period, ResponseTime

from .histogram_plot import HistogramPlot
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Path

MetricsType = Frequency | Latency | Period | ResponseTime
HistTypes = CallbackBase | Communication | Path


class HistogramPlotFactory:
    """Factory class to create an instance of HistogramPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        target_objects: Sequence[HistTypes],
        metrics_name: str,
        visualize_lib: VisualizeLibInterface,
        case: str | None = None
    ) -> HistogramPlot:
        """
        Create an instance of HistogramPlot.

        Parameters
        ----------
        target_objects : Sequence[HistTypes]
            HistTypes = CallbackBase | Communication | Path
        metrics_name : str
            Metrics for HistogramPlot data.
            supported metrics: [frequency/latency/period/response_time]
        case : str, optional
            Response time calculation method, used only for response time.
            supported case: [all/best/worst/worst-with-external-latency].
        visualize_lib : VisualizeLibInterface
            Instance of VisualizeLibInterface used for visualization.

        Returns
        -------
        HistogramPlot

        Raises
        ------
        UnsupportedTypeError
            Argument metrics is not "frequency", "latency", or "period".

        """
        if metrics_name == 'frequency':
            metrics_frequency: list[MetricsType] =\
                  [Frequency(target_object.to_records()) for target_object in target_objects]
            return HistogramPlot(
                metrics_frequency,
                visualize_lib,
                target_objects,
                metrics_name
                )
        elif metrics_name == 'latency':
            metrics_latency: list[MetricsType] =\
                  [Latency(target_object.to_records()) for target_object in target_objects]
            return HistogramPlot(
                metrics_latency,
                visualize_lib,
                target_objects,
                metrics_name
                )
        elif metrics_name == 'period':
            metrics_period: list[MetricsType] =\
                  [Period(target_object.to_records()) for target_object in target_objects]
            return HistogramPlot(
                metrics_period,
                visualize_lib,
                target_objects,
                metrics_name
                )
        elif metrics_name == 'response_time':
            metrics_response_time: list[MetricsType] =\
                  [ResponseTime(target_object.to_records()) for target_object in target_objects]
            return HistogramPlot(
                metrics_response_time,
                visualize_lib,
                target_objects,
                metrics_name,
                case
                )
        else:
            raise UnsupportedTypeError(
                'Unsupported metrics specified. '
                'Supported metrics: [frequency/latency/period]'
            )
