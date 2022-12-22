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

from collections import defaultdict
from logging import getLogger
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

from bokeh.colors import Color, RGB
from bokeh.models import (GlyphRenderer, HoverTool, Legend,
                          LinearAxis, Range1d, SingleIntervalTicker)
from bokeh.plotting import ColumnDataSource, Figure, figure

import colorcet as cc
import numpy as np

from .visualize_lib_interface import VisualizeLibInterface
from ..metrics_base import MetricsBase
from ...record import RecordsInterface
from ...runtime import (CallbackBase, Communication, Publisher,
                        Subscription, SubscriptionCallback, TimerCallback)

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
        def get_color_generator() -> Generator:
            color_palette = self._create_color_palette()
            color_idx = 0
            while True:
                yield color_palette[color_idx]
                color_idx = (color_idx + 1) % len(color_palette)

        line_source = LineSource(frame_min, xaxis_type)
        p.add_tools(line_source.create_hover(target_objects[0]))
        color_generator = get_color_generator()
        legend_manager = LegendManager()
        for to, timeseries in zip(target_objects, timeseries_records_list):
            renderer = p.line(
                'x', 'y',
                source=line_source.generate(to, timeseries),
                color=next(color_generator)
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
            to_dfs = [to.to_dataframe(remove_dropped=True) for to in target_objects]
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


class LineSource:
    """Class that generate lines of timeseries data."""

    def __init__(
        self,
        frame_min,
        xaxis_type: str
    ) -> None:
        self._frame_min = frame_min
        self._xaxis_type = xaxis_type

    def create_hover(self, target_object: TimeSeriesTypes) -> HoverTool:
        """
        Create HoverTool for timeseries figure.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]

        Returns
        -------
        bokeh.models.HoverTool
            This contains information display when hovering the mouse over a drawn line.

        """
        source_keys = self._get_source_keys(target_object)
        tips_str = '<div style="width:400px; word-wrap: break-word;">'
        for k in source_keys:
            tips_str += f'@{k} <br>'
        tips_str += '</div>'

        return HoverTool(tooltips=tips_str, point_policy='follow_mouse')

    def generate(
        self,
        target_object: TimeSeriesTypes,
        timeseries_records: RecordsInterface,
    ) -> ColumnDataSource:
        """
        Generate a line source for timeseries figure.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
        timeseries_records : RecordsInterface
            Records containing timeseries data.

        Returns
        -------
        bokeh.plotting.ColumnDataSource
            Line source for timeseries figure.

        """
        line_source = ColumnDataSource(data={
            k: [] for k in (['x', 'y'] + self._get_source_keys(target_object))
        })
        data_dict = self._get_data_dict(target_object)
        x_item, y_item = self._get_x_y(timeseries_records)
        for x, y in zip(x_item, y_item):
            line_source.stream({**{'x': [x], 'y': [y]}, **data_dict})  # type: ignore

        return line_source

    def _get_x_y(
        self,
        timeseries_records: RecordsInterface
    ) -> Tuple[List[Union[int, float]], List[Union[int, float]]]:
        def ensure_not_none(
            target_seq: Sequence[Optional[Union[int, float]]]
        ) -> List[Union[int, float]]:
            """
            Ensure the inputted list does not include None.

            Notes
            -----
            The timeseries_records is implemented not to include None,
            so if None is included, an AssertionError is output.

            """
            not_none_list = [_ for _ in target_seq if _ is not None]
            assert len(target_seq) == len(not_none_list)

            return not_none_list

        ts_column = timeseries_records.columns[0]
        value_column = timeseries_records.columns[1]
        timestamps = ensure_not_none(timeseries_records.get_column_series(ts_column))
        values = ensure_not_none(timeseries_records.get_column_series(value_column))
        if 'latency' in value_column.lower() or 'period' in value_column.lower():
            values = [v*10**(-6) for v in values]  # [ns] -> [ms]

        x_item: List[Union[int, float]]
        y_item: List[Union[int, float]] = values
        if self._xaxis_type == 'system_time':
            x_item = [(ts-self._frame_min)*10**(-9) for ts in timestamps]
        elif self._xaxis_type == 'index':
            x_item = list(range(0, len(values)))
        elif self._xaxis_type == 'sim_time':
            x_item = timestamps

        return x_item, y_item

    def _get_source_keys(
        self,
        target_object: TimeSeriesTypes
    ) -> List[str]:
        source_keys: List[str]
        if isinstance(target_object, CallbackBase):
            source_keys = ['node_name', 'callback_name', 'callback_type',
                           'callback_param', 'symbol']
        elif isinstance(target_object, Communication):
            source_keys = ['topic_name', 'publish_node_name', 'subscribe_node_name']
        elif isinstance(target_object, (Publisher, Subscription)):
            source_keys = ['node_name', 'topic_name']
        else:
            raise NotImplementedError()

        return source_keys

    def _get_description(
        self,
        key: str,
        target_object: TimeSeriesTypes
    ) -> str:
        if key == 'callback_param':
            if isinstance(target_object, TimerCallback):
                description = f'period_ns = {target_object.period_ns}'
            elif isinstance(target_object, SubscriptionCallback):
                description = f'subscribe_topic_name = {target_object.subscribe_topic_name}'
        else:
            raise NotImplementedError()

        return description

    def _get_data_dict(
        self,
        target_object: TimeSeriesTypes
    ) -> Dict[str, Any]:
        data_dict: Dict[str, Any] = {}
        for k in self._get_source_keys(target_object):
            try:
                data_dict[k] = [f'{k} = {getattr(target_object, k)}']
            except AttributeError:
                data_dict[k] = [self._get_description(k, target_object)]

        return data_dict


class LegendManager:
    """Class that manages legend in Bokeh figure."""

    def __init__(self) -> None:
        self._legend_count_map: Dict[str, int] = defaultdict(int)
        self._legend_items: List[Tuple[str, List[GlyphRenderer]]] = []

    def add_legend(
        self,
        target_object: Any,
        renderer: GlyphRenderer
    ) -> None:
        """
        Store a legend of the input object internally.

        Parameters
        ----------
        target_object : Any
            Instance of any class.
        renderer : bokeh.models.GlyphRenderer
            Instance of renderer.

        """
        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._legend_count_map[class_name]}'
        self._legend_count_map[class_name] += 1
        self._legend_items.append((label, [renderer]))

    def draw_legends(
        self,
        figure: Figure,
        max_legends: int = 20,
        full_legends: bool = False
    ) -> None:
        """
        Add legends to the input figure.

        Parameters
        ----------
        figure : Figure
            Target figure.
        max_legends : int, optional
            Maximum number of legends to display, by default 20.
        full_legends : bool, optional
            Display all legends even if they exceed max_legends, by default False.

        """
        for i in range(0, len(self._legend_items)+10, 10):
            if not full_legends and i >= max_legends:
                logger.warning(
                    f'The maximum number of legends drawn by default is {max_legends}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            figure.add_layout(Legend(items=self._legend_items[i:i+10]), 'right')

        figure.legend.click_policy = 'hide'
