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

from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Optional

from bokeh.plotting import Figure, save, show
from bokeh.resources import CDN

import pandas as pd

logger = getLogger(__name__)


class PlotBase(metaclass=ABCMeta):
    """Plot base class."""

    @abstractmethod
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

    @abstractmethod
    def figure(
        self,
        xaxis_type: Optional[str],
        ywheel_zoom: Optional[bool],
        full_legends: Optional[bool]
    ) -> Figure:
        """
        Get bokeh.plotting.Figure object.

        Parameters
        ----------
        xaxis_type : Optional[str]
            Type of time for timestamp.
        ywheel_zoom : Optional[bool]
            If True, the drawn graph can be expanded in the y-axis direction.
        full_legends : Optional[bool]
            If True, all legends are drawn even if the number of legends exceeds the threshold.

        Returns
        -------
        bokeh.plotting.Figure

        """
        raise NotImplementedError()

    def show(
        self,
        xaxis_type: Optional[str] = None,
        ywheel_zoom: Optional[bool] = None,
        full_legends: Optional[bool] = None,
        # TODO: add interactive option
    ) -> None:
        """
        Draw a graph using the bokeh library.

        Parameters
        ----------
        xaxis_type : str
            Type of x-axis of the graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        p = self.figure(xaxis_type, ywheel_zoom, full_legends)
        show(p)

    def save(
        self,
        export_path: str,
        title: str = '',
        xaxis_type: Optional[str] = None,
        ywheel_zoom: Optional[bool] = None,
        full_legends: Optional[bool] = None
    ) -> None:
        """
        Export a graph using the bokeh library.

        Parameters
        ----------
        export_path : str
            The graph will be saved as a file.
        title: str, optional
            Title of the graph, by default ''.
        xaxis_type : str
            Type of x-axis of the graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        p = self.figure(xaxis_type, ywheel_zoom, full_legends)
        save(p, export_path, title=title, resources=CDN)
