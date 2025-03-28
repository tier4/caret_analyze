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

from bokeh.plotting import figure as Figure

import pandas as pd

from ..metrics_base import MetricsBase
from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path


class TimeSeriesPlot(PlotBase):
    """Class that provides API for timeseries data."""

    def __init__(
        self,
        metrics: MetricsBase,
        visualize_lib: VisualizeLibInterface,
        case: str = 'best'  # case is only used for response time timeseries.
    ) -> None:
        self._metrics = metrics
        self._visualize_lib = visualize_lib
        self._case = case

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get time series data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified, by default "system_time".

        Returns
        -------
        pd.DataFrame
            Time series plot DataFrame.

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        self._validate_xaxis_type(xaxis_type)
        return self._metrics.to_dataframe(xaxis_type)

    def figure(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None
    ) -> Figure:
        """
        Get a timeseries graph for each object using the bokeh library.

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

        return self._visualize_lib.timeseries(
            self._metrics,
            xaxis_type,
            ywheel_zoom,
            full_legends,
            self._case
        )

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
