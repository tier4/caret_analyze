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
        raise NotImplementedError()

    @abstractmethod
    def figure(
        self,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        raise NotImplementedError()

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None,
        # TODO: add interactive option
    ) -> Figure:
        """
        Draw a graph using the bokeh library.

        Parameters
        ----------
        xaxis_type : str
            Type of x-axis of the graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        export_path : str, optional
            The graph will be saved as a file, by default None.
            This option is deprecated, please use save method.

        Returns
        -------
        bokeh.plotting.Figure

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        p = self.figure(xaxis_type, ywheel_zoom, full_legends)
        if export_path:
            logger.warning("The 'export_path' option is deprecated, please use 'save' method.")
            self.save(export_path=export_path, xaxis_type=xaxis_type,
                      ywheel_zoom=ywheel_zoom, full_legends=full_legends)
        else:
            show(p)

        return p

    def save(
        self,
        export_path: str,
        title: str = '',
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
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
            The default is "system_time".
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
