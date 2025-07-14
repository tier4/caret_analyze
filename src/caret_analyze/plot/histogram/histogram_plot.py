# Copyright 2021 TIER IV, Inc.
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

from bokeh.plotting import figure as Figure

from caret_analyze.record import Frequency, Latency, Period, RecordsInterface, ResponseTime

from numpy import histogram
import pandas as pd

from ..plot_base import PlotBase
from ..util import get_clock_converter
from ..visualize_lib import VisualizeLibInterface
from ...common import ClockConverter
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Path, Publisher, Subscription

MetricsTypes = Frequency | Latency | Period | ResponseTime
HistTypes = CallbackBase | Communication | Path | Publisher | Subscription


class HistogramPlot(PlotBase):
    """Class that provides API for histogram data."""

    def __init__(
        self,
        metrics: list[MetricsTypes],
        visualize_lib: VisualizeLibInterface,
        target_objects: Sequence[HistTypes],
        metrics_name: str,
        case: str | None = None
    ) -> None:
        self._metrics = metrics
        self._visualize_lib = visualize_lib
        self._target_objects = target_objects
        self._metrics_name = metrics_name
        self._case = case

    def to_dataframe(
        self,
        xaxis_type: str
    ) -> pd.DataFrame:
        """
        Get data in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.

        Returns
        -------
        pd.DataFrame
            Histogram dataFrame.

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    def figure(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None
    ) -> Figure:
        """
        Get a histogram graph for each object using the bokeh library.

        Parameters
        ----------
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified, by default "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel, by default True.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold, by default False.

        Returns
        -------
        Figure
            bokeh.plotting.Figure

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else True
        full_legends = full_legends if full_legends is not None else False

        # Validate
        self._validate_xaxis_type(xaxis_type)

        # get converter
        converter: ClockConverter | None = None
        if xaxis_type == 'sim_time':
            converter = get_clock_converter(self._target_objects)

        hist_list, bins = self._histogram_data(converter)

        return self._visualize_lib.histogram(
            hist_list,
            bins,
            self._target_objects,
            self._metrics_name,
            self._case
            )

    def _histogram_data(self, converter: ClockConverter | None
                        ) -> tuple[list[list[int]], list[float]]:

        # wrapper function of to_records()/to_xxx_records()
        def to_records(metrics: MetricsTypes,
                       converter: ClockConverter | None) -> RecordsInterface:
            if isinstance(metrics, ResponseTime):
                if self._case == 'all':
                    return metrics.to_all_records(converter=converter)
                elif self._case == 'best':
                    return metrics.to_best_case_records(converter=converter)
                elif self._case == 'worst':
                    return metrics.to_worst_case_records(converter=converter)
                elif self._case == 'worst-with-external-latency':
                    return metrics.to_worst_with_external_latency_case_records(converter=converter)
                else:
                    raise ValueError('optional argument "case" must be following: \
                                     "all", "best", "worst", "worst-with-external-latency".')
            else:
                return metrics.to_records(converter=converter)

        data_list: list[list[int | float]] = [
            [
                _ for _ in to_records(m, converter).get_column_series(self._metrics_name)
                if _ is not None
            ]
            for m in self._metrics
        ]

        if self._metrics_name in ['period', 'latency', 'response_time']:
            data_list = [[_ *10**(-6) for _ in data] for data in data_list]

        filled_data_list = [data for data in data_list if len(data)]
        if len(filled_data_list) != 0:
            max_value = max(max(filled_data_list, key=lambda x: max(x)))
            min_value = min(min(filled_data_list, key=lambda x: min(x)))
            data_range = (min_value, max_value)
        else:
            data_range = None

        hists: list[list[int]] = []
        bins: list[float] = []
        for hist_type in data_list:
            hist, tmp_bins = histogram(hist_type, 20, data_range, density=False)
            bins = list(tmp_bins)
            hists.append(list(hist))

        return hists, bins

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
