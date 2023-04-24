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

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, Figure

from .util import (apply_x_axis_offset, ColorSelectorFactory, get_callback_param_desc,
                   HoverKeysFactory, HoverSource, init_figure, LegendManager)
from ...metrics_base import MetricsBase
from ....record import Range, RecordsInterface
from ....runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class BokehTimeSeries:

    def __init__(
        self,
        metrics: MetricsBase,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> None:
        self._metrics = metrics
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._full_legends = full_legends

    def create_figure(self) -> Figure:
        target_objects = self._metrics.target_objects
        timeseries_records_list = self._metrics.to_timeseries_records_list(self._xaxis_type)

        # Initialize figure
        y_axis_label = timeseries_records_list[0].columns[1]
        if y_axis_label == 'frequency':
            y_axis_label = y_axis_label + ' [Hz]'
        elif y_axis_label in ['period', 'latency']:
            y_axis_label = y_axis_label + ' [ms]'
        else:
            raise NotImplementedError()
        if isinstance(target_objects[0], CallbackBase):
            title = f'Time-line of callbacks {y_axis_label}'
        elif isinstance(target_objects[0], Communication):
            title = f'Time-line of communications {y_axis_label}'
        else:
            title = f'Time-line of publishes/subscribes {y_axis_label}'
        fig = init_figure(title, self._ywheel_zoom, self._xaxis_type, y_axis_label)

        # Apply xaxis offset
        records_range = Range([to.to_records() for to in target_objects])
        frame_min, frame_max = records_range.get_range()
        if self._xaxis_type == 'system_time':
            apply_x_axis_offset(fig, frame_min, frame_max)

        # Draw lines
        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        legend_manager = LegendManager()
        line_source = LineSource(legend_manager, target_objects[0], frame_min, self._xaxis_type)
        fig.add_tools(line_source.create_hover())
        for to, timeseries in zip(target_objects, timeseries_records_list):
            renderer = fig.line(
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
        legends = legend_manager.create_legends(num_legend_threshold, self._full_legends)
        for legend in legends:
            fig.add_layout(legend, 'right')
        fig.legend.click_policy = 'hide'

        return fig


class LineSource:
    """Class to generate timeseries line sources."""

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: TimeSeriesTypes,
        frame_min,
        xaxis_type: str,
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance('timeseries', target_object)
        self._hover_source = HoverSource(self._hover_keys)
        self._legend_manager = legend_manager
        self._frame_min = frame_min
        self._xaxis_type = xaxis_type

    def create_hover(self, options: Dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
        return self._hover_keys.create_hover(options)

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
            k: [] for k in (['x', 'y'] + self._hover_keys.to_list())
        })
        additional_hover_dict = {
            'legend_label': f'legend_label = {self._legend_manager.get_label(target_object)}'}
        if isinstance(target_object, CallbackBase):
            additional_hover_dict['callback_param'] = get_callback_param_desc(target_object)
        hover_source = self._hover_source.generate(target_object, additional_hover_dict)
        x_item, y_item = self._get_x_y(timeseries_records)
        for x, y in zip(x_item, y_item):
            line_source.stream({**{'x': [x], 'y': [y]}, **hover_source})  # type: ignore

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
