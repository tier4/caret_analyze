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

from logging import getLogger
from typing import List, Sequence, Tuple, Union

from bokeh.colors import Color, RGB
from bokeh.models import LinearAxis, Range1d, SingleIntervalTicker
from bokeh.plotting import Figure, figure

import colorcet as cc
import numpy as np

from .bokeh_source import LegendManager, LineSource
from .color_selector import ColorSelectorFactory
from ..visualize_lib_interface import VisualizeLibInterface
from ...metrics_base import MetricsBase
from ....runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class Bokeh(VisualizeLibInterface):
    """Class that visualizes data using Bokeh library."""

    def __init__(self) -> None:
        pass

    def timeseries(
        self,
        metrics: MetricsBase,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        """
        Get a timeseries figure.

        Parameters
        ----------
        metrics : MetricsBase
            Metrics to be y-axis in visualization.
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
            Figure of timeseries.

        """
        target_objects = metrics.target_objects
        timeseries_records_list = metrics.to_timeseries_records_list(xaxis_type)

        # Initialize figure
        y_axis_label = timeseries_records_list[0].columns[1]
        fig_args = {'frame_height': 270,
                    'frame_width': 800,
                    'y_axis_label': y_axis_label}

        if xaxis_type == 'system_time':
            fig_args['x_axis_label'] = 'system time [s]'
        elif xaxis_type == 'sim_time':
            fig_args['x_axis_label'] = 'simulation time [s]'
        else:
            fig_args['x_axis_label'] = xaxis_type

        if ywheel_zoom:
            fig_args['active_scroll'] = 'wheel_zoom'
        else:
            fig_args['tools'] = ['xwheel_zoom', 'xpan', 'save', 'reset']
            fig_args['active_scroll'] = 'xwheel_zoom'

        if isinstance(target_objects[0], CallbackBase):
            fig_args['title'] = f'Time-line of callbacks {y_axis_label}'
        elif isinstance(target_objects[0], Communication):
            fig_args['title'] = f'Time-line of communications {y_axis_label}'
        else:
            fig_args['title'] = f'Time-line of publishes/subscribes {y_axis_label}'

        p = figure(**fig_args)

        # Apply xaxis offset
        frame_min, frame_max = self._get_range(target_objects)
        if xaxis_type == 'system_time':
            self._apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)

        # Draw lines
        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        legend_manager = LegendManager()
        line_source = LineSource(frame_min, xaxis_type, legend_manager)
        p.add_tools(line_source.create_hover(target_objects[0]))
        for to, timeseries in zip(target_objects, timeseries_records_list):
            renderer = p.line(
                'x', 'y',
                source=line_source.generate(to, timeseries),
                color=color_selector.get_color()
            )
            legend_manager.add_legend(to, renderer)

        # Draw legends
        num_legend_threshold = 20
        """
        In Autoware, the number of callbacks in a node is less than 20.
        Here, num_legend_threshold is set to 20 as the maximum value.
        """
        legend_manager.draw_legends(p, num_legend_threshold, full_legends)

        return p

    @staticmethod
    def _create_color_palette() -> Sequence[Color]:
        def from_rgb(r: float, g: float, b: float) -> RGB:
            r_ = int(r*255)
            g_ = int(g*255)
            b_ = int(b*255)

            return RGB(r_, g_, b_)

        return [from_rgb(*rgb) for rgb in cc.glasbey_bw_minc_20]

    @staticmethod
    def _get_range(target_objects: List[TimeSeriesTypes]) -> Tuple[int, int]:
        has_valid_data = False

        try:
            # NOTE:
            # If remove_dropped=True,
            # data in DataFrame may be lost due to drops in columns other than the first column.
            to_dfs = [to.to_dataframe(remove_dropped=False) for to in target_objects]
            to_dfs_valid = [to_df for to_df in to_dfs if len(to_df) > 0]

            # NOTE:
            # The first column is system time for now.
            # The other columns could be other than system time.
            # Only the system time is picked out here.
            base_series = [df.iloc[:, 0] for df in to_dfs_valid]
            min_series = [series.min() for series in base_series]
            max_series = [series.max() for series in base_series]
            valid_min_series = [value for value in min_series if not np.isnan(value)]
            valid_max_series = [value for value in max_series if not np.isnan(value)]

            has_valid_data = len(valid_max_series) > 0 or len(valid_min_series) > 0
            to_min = min(valid_min_series)
            to_max = max(valid_max_series)
            return to_min, to_max
        except Exception:
            msg = 'Failed to calculate interval of measurement time.'
            if not has_valid_data:
                msg += ' No valid measurement data.'
            logger.warning(msg)
            return 0, 1

    @staticmethod
    def _apply_x_axis_offset(
        fig: Figure,
        x_range_name: str,
        min_ns: float,
        max_ns: float
    ) -> None:
        offset_s = min_ns*1.0e-9
        end_s = (max_ns-min_ns)*1.0e-9

        fig.extra_x_ranges = {x_range_name: Range1d(start=min_ns, end=max_ns)}

        xaxis = LinearAxis(x_range_name=x_range_name)
        xaxis.visible = False  # type: ignore

        ticker = SingleIntervalTicker(interval=1, num_minor_ticks=10)
        fig.xaxis.ticker = ticker
        fig.add_layout(xaxis, 'below')

        fig.x_range = Range1d(start=0, end=end_s)

        fig.xgrid.minor_grid_line_color = 'black'
        fig.xgrid.minor_grid_line_alpha = 0.1

        fig.xaxis.major_label_overrides = {0: f'0+{offset_s}'}
