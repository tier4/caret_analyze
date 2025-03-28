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
from logging import getLogger

from bokeh.plotting import figure as Figure, save, show
from bokeh.resources import CDN

import pandas as pd

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...common import UniqueList
from ...exceptions import UnsupportedTypeError
from ...runtime import Application, CallbackGroup, Executor, Node, Path

CallbackGroupTypes = (Application | Executor | Path | Node |
                      CallbackGroup | Sequence[CallbackGroup])

logger = getLogger(__name__)


class CallbackSchedulingPlot(PlotBase):
    """Class that provides API for callback scheduling plot."""

    def __init__(
        self,
        target_objects: CallbackGroupTypes,
        visualize_lib: VisualizeLibInterface,
        lstrip_s: float,
        rstrip_s: float
    ) -> None:
        self._visualize_lib = visualize_lib
        self._callback_groups = self._get_callback_groups(target_objects)
        self._lstrip_s = lstrip_s
        self._rstrip_s = rstrip_s

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        """
        Get data in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.

        Returns
        -------
        pd.DataFrame
            Callback scheduling plot DataFrame.

        """
        logger.warning("'to_dataframe' method is not implemented in CallbackSchedulingPlot.")
        return pd.DataFrame()

    def figure(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None,
        coloring_rule: str | None = None,
    ) -> Figure:
        """
        Get a callback scheduling plot using the bokeh library.

        Parameters
        ----------
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified, by default "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel, by default False.
        full_legends : bool, optional
            If True, all legends are drawn
            even if the number of legends exceeds the threshold, by default False.
        coloring_rule : str, optional
            The unit of color change
            There are there rules which are [callback/callback_group/node], by default 'callback'

        Returns
        -------
        bokeh.plotting.Figure

        Raises
        ------
        UnsupportedTypeError
            - Argument xaxis_type is not "system_time", "index", or "sim_time".
            - Argument coloring_rule is not 'callback', 'callback_group', or 'node'.

        """
        # Set default value
        xaxis_type = xaxis_type or 'system_time'
        ywheel_zoom = ywheel_zoom if ywheel_zoom is not None else False
        full_legends = full_legends if full_legends is not None else False
        coloring_rule = coloring_rule or 'callback'

        # Validate
        self._validate_xaxis_type(xaxis_type)
        if coloring_rule not in ['callback', 'callback_group', 'node']:
            raise UnsupportedTypeError(
                f'Unsupported coloring_rule. coloring_rule = {coloring_rule}. '
                'supported coloring_rule: [callback/callback_group/node]'
            )

        return self._visualize_lib.callback_scheduling(
            self._callback_groups, xaxis_type, ywheel_zoom, full_legends,
            coloring_rule, self._lstrip_s, self._rstrip_s
        )

    def show(
        self,
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None,
        coloring_rule: str | None = None,
    ) -> None:
        """
        Show a callback scheduling plot.

        Parameters
        ----------
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified, by default "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel, by default False.
        full_legends : bool, optional
            If True, all legends are drawn
            even if the number of legends exceeds the threshold, by default False.
        coloring_rule : str, optional
            The unit of color change
            There are there rules which are [callback/callback_group/node], by default 'callback'

        """
        p = self.figure(xaxis_type, ywheel_zoom, full_legends, coloring_rule)
        show(p)

    def save(
        self,
        export_path: str,
        title: str = '',
        xaxis_type: str | None = None,
        ywheel_zoom: bool | None = None,
        full_legends: bool | None = None,
        coloring_rule: str | None = None
    ) -> None:
        """
        Save a callback scheduling plot.

        Parameters
        ----------
        export_path : str
            The graph will be saved as a file.
        title: str, optional
            Title of the graph, by default ''.
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified, by default "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel, by default False.
        full_legends : bool, optional
            If True, all legends are drawn
            even if the number of legends exceeds the threshold, by default False.
        coloring_rule : str, optional
            The unit of color change
            There are there rules which are [callback/callback_group/node], by default 'callback'

        """
        p = self.figure(xaxis_type, ywheel_zoom, full_legends, coloring_rule)
        save(p, export_path, title=title, resources=CDN)

    @staticmethod
    def _get_callback_groups(target_objects: CallbackGroupTypes) -> list[CallbackGroup]:
        callback_groups: list[CallbackGroup]

        if isinstance(target_objects, (Application, Executor, Node)):
            if target_objects.callback_groups is None:
                logger.warning('target.callback_groups is None')
                callback_groups = []
            else:
                callback_groups = target_objects.callback_groups

        elif isinstance(target_objects, Path):
            _callback_groups = UniqueList()
            for comm in target_objects.communications:
                for cbg in comm.publish_node.callback_groups or []:
                    _callback_groups.append(cbg)
            for cbg in target_objects.communications[-1].subscribe_node.callback_groups or []:
                _callback_groups.append(cbg)
            if len(_callback_groups) == 0:
                logger.warning('target.callback_groups is None')
                callback_groups = []
            else:
                callback_groups = _callback_groups.as_list()

        elif isinstance(target_objects, CallbackGroup):
            callback_groups = [target_objects]

        else:  # Sequence[CallbackGroup]
            callback_groups = list(target_objects)

        return callback_groups

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time]'
            )
