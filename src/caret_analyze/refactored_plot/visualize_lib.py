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
from typing import Dict, List, Sequence, Tuple, Union

from bokeh.colors import Color, RGB
from bokeh.models import (HoverTool, Legend, LinearAxis, Range1d,
                          SingleIntervalTicker)
from bokeh.plotting import ColumnDataSource, Figure, figure

import colorcet as cc

from ..exceptions import UnsupportedTypeError
from ..record import RecordsInterface
from ..runtime import (CallbackBase, Communication, Publisher, Subscription,
                       SubscriptionCallback, TimerCallback)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class VisualizeLib:

    @staticmethod
    def create_timeseries_plot(
        target_objects: List[TimeSeriesTypes],
        records_list: List[RecordsInterface],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        return Bokeh.create_timeseries_plot(
            target_objects,
            records_list,
            xaxis_type,
            ywheel_zoom,
            full_legends
        )


class Bokeh:

    @staticmethod
    def create_timeseries_plot(
        target_objects: List[TimeSeriesTypes],
        records_list: List[RecordsInterface],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        # Preprocess
        p = Bokeh._create_empty_timeseries_plot(
            target_object=target_objects[0],
            xaxis_type=xaxis_type,
            y_axis_label=records_list[0].columns[1],
            ywheel_zoom=ywheel_zoom
        )
        p.add_tools(Bokeh._get_hover(target_objects[0]))
        frame_min, frame_max = Bokeh._get_range(target_objects)
        if xaxis_type == 'system_time':
            Bokeh._apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)

        # Draw lines
        color_selector = PlotColorSelector()
        label_generator = LegendLabelGenerator()
        legend_items = []
        for obj, records in zip(target_objects, records_list):
            line_source = Bokeh._get_line_source(obj, records, frame_min, xaxis_type)
            renderer = p.line('x', 'y',
                              source=line_source,
                              color=color_selector.get_color(str(obj)))
            legend_items.append((label_generator.get_label(obj), [renderer]))

        # Add legends by ten
        num_legend_threshold = 20
        """
        In Autoware, the number of callbacks in a node is less than 20.
        Here, num_legend_threshold is set to 20 as the maximum value.
        """
        for i in range(0, len(legend_items)+10, 10):
            if not full_legends and i >= num_legend_threshold:
                logger.warning(
                    'The maximum number of legends drawn '
                    f'by default is {num_legend_threshold}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            p.add_layout(Legend(items=legend_items[i:i+10]), 'right')
        p.legend.click_policy = 'hide'

        return p

    @staticmethod
    def _get_line_source(
        target_object: TimeSeriesTypes,
        records: RecordsInterface,
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        timestamps = records.get_column_series(records.columns[0])
        values = records.get_column_series(records.columns[1])
        if xaxis_type == 'system_time':
            x_item = [(ts-frame_min)*10**(-9) for ts in timestamps]
            y_item = values
        elif xaxis_type == 'index':
            x_item = list(range(0, len(values)))
            y_item = values
        elif xaxis_type == 'sim_time':
            x_item = timestamps
            y_item = values

        if isinstance(target_object, CallbackBase):
            line_source = Bokeh._get_line_source_callback(x_item, y_item, target_object)
        elif isinstance(target_object, Communication):
            line_source = Bokeh._get_line_source_communication(x_item, y_item, target_object)
        elif isinstance(target_object, (Publisher, Subscription)):
            line_source = Bokeh._get_line_source_pub_sub(x_item, y_item, target_object)

        return line_source

    @staticmethod
    def _get_line_source_pub_sub(
        x_item, y_item,
        pub_sub: Union[Publisher, Subscription]
    ) -> ColumnDataSource:
        line_source = ColumnDataSource(
            data={
                'x': [],
                'y': [],
                'node_name': [],
                'topic_name': [],
            }
        )
        for x, y in zip(x_item, y_item):
            new_data = {
                'x': [x],
                'y': [y],
                'node_name': [pub_sub.node_name],
                'topic_name': [pub_sub.topic_name],
            }
            line_source.stream(new_data)

        return line_source

    @staticmethod
    def _get_line_source_communication(
        x_item, y_item,
        comm: Communication
    ) -> ColumnDataSource:
        line_source = ColumnDataSource(
            data={
                'x': [],
                'y': [],
                'topic_name': [],
                'publish_node': [],
                'subscribe_node': []
            }
        )
        for x, y in zip(x_item, y_item):
            new_data = {
                'x': [x],
                'y': [y],
                'topic_name': [comm.topic_name],
                'publish_node': [comm.publish_node_name],
                'subscribe_node': [comm.subscribe_node_name]
            }
            line_source.stream(new_data)

        return line_source

    @staticmethod
    def _get_line_source_callback(
        x_item, y_item,
        callback: CallbackBase
    ) -> ColumnDataSource:
        line_source = ColumnDataSource(
            data={
                'x': [],
                'y': [],
                'node_name': [],
                'callback_name': [],
                'callback_type': [],
                'callback_param': [],
                'symbol': []
            }
        )
        callback_param = Bokeh._get_callback_param_desc(callback)
        for x, y in zip(x_item, y_item):
            new_data = {
                'x': [x],
                'y': [y],
                'node_name': [callback.node_name],
                'callback_name': [callback.callback_name],
                'symbol': [callback.symbol],
                'callback_param': [callback_param],
                'callback_type': [f'{callback.callback_type}']
            }
            line_source.stream(new_data)

        return line_source

    @staticmethod
    def _get_callback_param_desc(callback: CallbackBase):
        if isinstance(callback, TimerCallback):
            return f'period_ns: {callback.period_ns}'

        if isinstance(callback, SubscriptionCallback):
            return f'topic_name: {callback.subscribe_topic_name}'

        raise UnsupportedTypeError('callback type must be '
                                   '[ TimerCallback/ SubscriptionCallback]')

    @staticmethod
    def _create_empty_timeseries_plot(
        target_object: TimeSeriesTypes,
        xaxis_type: str,
        y_axis_label: str,
        ywheel_zoom: bool
    ) -> Figure:
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

        if isinstance(target_object, CallbackBase):
            fig_args['title'] = f'Time-line of callbacks {y_axis_label}'
        elif isinstance(target_object, Communication):
            fig_args['title'] = f'Time-line of communications {y_axis_label}'
        else:
            fig_args['title'] = f'Time-line of publishes/subscribes {y_axis_label}'

        return figure(**fig_args)

    @staticmethod
    def _get_hover(target_object: TimeSeriesTypes) -> HoverTool:
        if isinstance(target_object, CallbackBase):
            hover = HoverTool(
                        tooltips="""
                        <div style="width:400px; word-wrap: break-word;">
                        <br>
                        node_name = @node_name <br>
                        callback_name = @callback_name <br>
                        callback_type = @callback_type <br>
                        @callback_param <br>
                        symbol = @symbol
                        </div>
                        """,
                        point_policy='follow_mouse'
                    )
        elif isinstance(target_object, Communication):
            hover = HoverTool(
                        tooltips="""
                        <div style="width:400px; word-wrap: break-word;">
                        <br>
                        topic_name = @topic_name <br>
                        publish_node = @publish_node <br>
                        subscribe_node = @subscribe_node <br>
                        </div>
                        """,
                        point_policy='follow_mouse'
                    )
        elif isinstance(target_object, (Publisher, Subscription)):
            hover = HoverTool(
                        tooltips="""
                        <div style="width:400px; word-wrap: break-word;">
                        <br>
                        node_name = @node_name <br>
                        topic_name = @topic_name <br>
                        </div>
                        """,
                        point_policy='follow_mouse'
                    )

        return hover

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


class LegendLabelGenerator:

    def __init__(self) -> None:
        self._count_map: Dict[str, int] = defaultdict(int)

    def get_label(self, target_object: TimeSeriesTypes) -> str:
        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._count_map[class_name]}'
        self._count_map[class_name] += 1

        return label


class PlotColorSelector:

    def __init__(self) -> None:
        self._palette: Sequence[Color] = \
            [self._from_rgb(*rgb) for rgb in cc.glasbey_bw_minc_20]
        self._color_map: Dict[str, Color] = {}

    def get_color(
        self,
        plot_object_name: str
    ) -> Color:
        if plot_object_name not in self._color_map:
            color_index = len(self._color_map) % len(self._palette)
            self._color_map[plot_object_name] = self._palette[color_index]

        return self._color_map[plot_object_name]

    @staticmethod
    def _from_rgb(r: float, g: float, b: float) -> RGB:
        r_ = int(r*255)
        g_ = int(g*255)
        b_ = int(b*255)

        return RGB(r_, g_, b_)
