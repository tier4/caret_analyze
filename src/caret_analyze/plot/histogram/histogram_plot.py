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

from bokeh.plotting import figure as Figure

from caret_analyze.record import Frequency, Latency, Period, ResponseTime

import pandas as pd

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Path

MetricsTypes = Frequency | Latency | Period | ResponseTime
HistTypes = CallbackBase | Communication | Path


class HistogramPlot(PlotBase):
    """Class that provides API for histogram data."""

    def __init__(
        self,
        metrics: list[MetricsTypes],
        visualize_lib: VisualizeLibInterface,
        target_objects: Sequence[HistTypes],
        data_type: str,
        case: str | None = None
    ) -> None:
        self._metrics = metrics
        self._visualize_lib = visualize_lib
        self._target_objects = target_objects
        self._data_type = data_type
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

        return self._visualize_lib.histogram(
            self._metrics,
            self._target_objects,
            self._data_type,
            self._case
            )

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
