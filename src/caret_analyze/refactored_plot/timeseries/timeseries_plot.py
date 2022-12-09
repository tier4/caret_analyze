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

from typing import Union

from bokeh.plotting import Figure

import pandas as pd

from ..metrics_base import MetricsBase
from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...exceptions import UnsupportedTypeError
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class TimeSeriesPlot(PlotBase):

    def __init__(
        self,
        metrics: MetricsBase,
        visualize_lib: VisualizeLibInterface
    ) -> None:
        self._metrics = metrics
        self._visualize_lib = visualize_lib

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get time series data for each object in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        return self._metrics.to_dataframe(xaxis_type)

    def figure(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> Figure:
        """
        Draw a timeseries graph for each object using the bokeh library.

        Parameters
        ----------
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.

        Returns
        -------
        bokeh.plotting.Figure

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        self._validate_xaxis_type(xaxis_type)

        return self._visualize_lib.timeseries(
            self._metrics,
            xaxis_type,
            ywheel_zoom,
            full_legends
        )

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
