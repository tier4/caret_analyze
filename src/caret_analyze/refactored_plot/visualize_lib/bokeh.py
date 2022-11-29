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
from typing import Any, Dict, Generator, List, Sequence, Tuple, Union

from bokeh.colors import Color, RGB
from bokeh.models import HoverTool, Legend, LinearAxis, Range1d, SingleIntervalTicker
from bokeh.plotting import ColumnDataSource, Figure, figure

import colorcet as cc

from .visualize_lib_interface import VisualizeLibInterface
from ...record import RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class Bokeh(VisualizeLibInterface):

    def __init__(self) -> None:
        pass

    def timeseries(
        self,
        target_objects: List[TimeSeriesTypes],
        timeseries_records_list: List[RecordsInterface],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        helper = BokehTimeSeriesHelper()

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
        p.add_tools(helper.get_hover(target_objects[0]))

        # Apply xaxis offset
        frame_min, frame_max = self._get_range(target_objects)
        if xaxis_type == 'system_time':
            self._apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)

        # Draw lines
        color_generator = helper.get_color_generator()
        legend_items = []
        for to, timeseries in zip(target_objects, timeseries_records_list):
            line_source = helper.get_line_source(to, timeseries, frame_min, xaxis_type)
            renderer = p.line('x', 'y', source=line_source, color=next(color_generator))
            legend_items.append((helper.get_legend_label(to), [renderer]))

        # Add legends by ten
        num_legend_threshold = 20
        """
        In Autoware, the number of callbacks in a node is less than 20.
        Here, num_legend_threshold is set to 20 as the maximum value.
        """
        for i in range(0, len(legend_items)+10, 10):
            if not full_legends and i >= num_legend_threshold:
                logger.warning(
                    f'The maximum number of legends drawn by default is {num_legend_threshold}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            p.add_layout(Legend(items=legend_items[i:i+10]), 'right')
        p.legend.click_policy = 'hide'

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
        to_valid = [to for to in target_objects if len(to.to_records()) > 0]
        if len(to_valid) == 0:
            logger.warning('Failed to found measurement results.')
            return 0, 1

        to_dfs = [to.to_dataframe(remove_dropped=True) for to in target_objects]
        to_dfs_valid = [to_df for to_df in to_dfs if len(to_df) > 0]

        # NOTE:
        # The first column is system time for now.
        # The other columns could be other than system time.
        # Only the system time is picked out here.
        base_series = [df.iloc[:, 0] for df in to_dfs_valid]
        to_min = min(series.min() for series in base_series)
        to_max = max(series.max() for series in base_series)

        return to_min, to_max

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
        xaxis.visible = False

        ticker = SingleIntervalTicker(interval=1, num_minor_ticks=10)
        fig.xaxis.ticker = ticker
        fig.add_layout(xaxis, 'below')

        fig.x_range = Range1d(start=0, end=end_s)

        fig.xgrid.minor_grid_line_color = 'black'
        fig.xgrid.minor_grid_line_alpha = 0.1

        fig.xaxis.major_label_overrides = {0: f'0+{offset_s}'}


class BokehTimeSeriesHelper:

    _source_property_dict = {
        'TimerCallback':
            ['node_name', 'callback_name', 'callback_type', 'period_ns', 'symbol'],
        'SubscriptionCallback':
            ['node_name', 'callback_name', 'callback_type', 'subscribe_topic_name', 'symbol'],
        'Communication':
            ['topic_name', 'publish_node_name', 'subscribe_node_name'],
        'Publisher':
            ['node_name', 'topic_name'],
        'Subscription':
            ['node_name', 'topic_name']
    }

    def __init__(self) -> None:
        self._color_palette = Bokeh._create_color_palette()
        self._legend_count_map: Dict[str, int] = defaultdict(int)

    def get_line_source(
        self,
        target_object: TimeSeriesTypes,
        timeseries_records: RecordsInterface,
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        # Get x_item and y_item
        ts_column = timeseries_records.columns[0]
        value_column = timeseries_records.columns[1]
        timestamps = timeseries_records.get_column_series(ts_column)
        values = timeseries_records.get_column_series(value_column)
        if 'latency' in value_column.lower() or 'period' in value_column.lower():
            values = [v*10**(-6) for v in values]

        if xaxis_type == 'system_time':
            x_item = [(ts-frame_min)*10**(-9) for ts in timestamps]
            y_item = values
        elif xaxis_type == 'index':
            x_item = list(range(0, len(values)))
            y_item = values
        elif xaxis_type == 'sim_time':
            x_item = timestamps
            y_item = values

        # create line_source
        source_properties = self._source_property_dict[type(target_object).__name__]
        data: Dict[str, Sequence[Any]] = {'x': [], 'y': []}
        data.update({p: [] for p in source_properties})
        line_source = ColumnDataSource(data=data)

        for x, y in zip(x_item, y_item):
            new_data: Dict[str, Sequence[Any]] = {'x': [x], 'y': [y]}
            new_data.update({p: [str(getattr(target_object, p))] for p in source_properties})
            line_source.stream(new_data)

        return line_source

    def get_hover(self, target_object: TimeSeriesTypes) -> HoverTool:
        if isinstance(target_object, CallbackBase):
            source_properties = sorted(set(
                self._source_property_dict['TimerCallback'] +
                self._source_property_dict['SubscriptionCallback'])
            )
        else:
            source_properties = self._source_property_dict[type(target_object).__name__]

        tips_str = '<div style="width:400px; word-wrap: break-word;"><br>'
        for k in source_properties:
            tips_str += f'{k} = @{k} <br>'
        tips_str += '</div>'

        return HoverTool(tooltips=tips_str, point_policy='follow_mouse')

    def get_legend_label(self, target_object: TimeSeriesTypes) -> str:
        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._legend_count_map[class_name]}'
        self._legend_count_map[class_name] += 1

        return label

    def get_color_generator(self) -> Generator:
        color_idx = 0
        while True:
            yield self._color_palette[color_idx]
            color_idx = (color_idx + 1) % len(self._color_palette)
