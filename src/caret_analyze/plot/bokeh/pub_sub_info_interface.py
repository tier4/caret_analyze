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
from logging import getLogger, Logger
from typing import Collection, Dict, Optional, Union

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, Figure, figure, save, show
from bokeh.resources import CDN

import pandas as pd

from .plot_util import get_fig_args, PlotColorSelector, validate_xaxis_type
from .util import apply_x_axis_offset, get_range
from ...common import ClockConverter
from ...runtime import Publisher, Subscription

logger = getLogger(__name__)


class PubSubTimeSeriesPlot(metaclass=ABCMeta):

    def __init__(
        self,
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> None:
        self._pub_subs = list(pub_subs)

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None,
        # interactive: bool = False
    ) -> Figure:
        validate_xaxis_type(xaxis_type)

        return self._show_core('All', xaxis_type, ywheel_zoom, full_legends, export_path)

        # # not interactive
        # if self._last_export_path:
        #     self._show_core('All')
        #     return None
        # if len(all_topic_names) == 1:
        #     return [self._show_core('All')]
        # elif not interactive:
        #     return [self._show_core(topic_name) for
        #             topic_name in ['All'] + all_topic_names]
        # # interactive
        # else:
        #     topic_dropdown = Dropdown(description='Topic:',
        #                               options=(['All'] + all_topic_names))
        #     interact(self._show_core,
        #              topic_name=topic_dropdown)

    def _show_core(
        self,
        topic_name: str,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        export_path: Optional[str]
    ) -> Figure:
        source_df = self._to_dataframe_core(xaxis_type)
        source_df_by_topic = self._get_source_df_by_topic(source_df)

        Hover = HoverTool(
                    tooltips="""
                    <div style="width:400px; word-wrap: break-word;">
                    <br>
                    node_name = @node_name <br>
                    topic_name = @topic_name <br>
                    </div>
                    """,
                    point_policy='follow_mouse'
                )
        frame_min, frame_max = get_range(self._pub_subs)
        y_axis_label = source_df.columns.get_level_values(1).to_list()[1]
        fig_args = get_fig_args(
            xaxis_type=xaxis_type,
            title=f'Time-line of publishes/subscribes {y_axis_label}',
            y_axis_label=y_axis_label,
            ywheel_zoom=ywheel_zoom
        )
        p = figure(**fig_args)
        if xaxis_type == 'system_time':
            apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        p.add_tools(Hover)

        # Draw lines
        color_selector = PlotColorSelector()
        legend_items = []
        pub_count = 0
        sub_count = 0
        if topic_name == 'All':
            drawn_pub_sub = self._pub_subs
        else:
            drawn_pub_sub = [pub_sub for pub_sub in self._pub_subs
                             if pub_sub.topic_name == topic_name]
        for i, pub_sub in enumerate(drawn_pub_sub):
            color = color_selector.get_color(str(i))
            # TODO:
            # remove source_df_by_topic argument from _get_pub_sub_lines.
            # source_df_by_topic is a redundant argument since pub_sub has its data
            line_source = self._get_pub_sub_lines(
                pub_sub,
                source_df_by_topic,
                frame_min,
                xaxis_type
            )
            if isinstance(pub_sub, Publisher):
                legend_label = f'publisher{pub_count}'
                pub_count += 1
            else:
                legend_label = f'subscription{sub_count}'
                sub_count += 1
            renderer = p.line('x', 'y',
                              source=line_source,
                              color=color)
            legend_items.append((legend_label, [renderer]))

        # Add legends by ten
        num_legend_threshold = 20
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

        # Output
        if export_path is None:
            show(p)
        else:
            save(p, export_path,
                 title='PubSub time-line', resources=CDN)

        return p

    def _get_pub_sub_lines(
        self,
        pub_sub: Union[Publisher, Subscription],
        source_df_by_topic: Dict[str, pd.DataFrame],
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        source_df = source_df_by_topic[pub_sub.topic_name].dropna()
        ts_column_idx = source_df.columns.to_list().index(
            self._get_ts_column_name(pub_sub))
        single_pub_sub_df = source_df.iloc[:, ts_column_idx:ts_column_idx+2]

        if xaxis_type == 'system_time':
            x_item = ((single_pub_sub_df.iloc[:, 0] - frame_min)
                      * 10**(-9)).to_list()
            y_item = single_pub_sub_df.iloc[:, 1].to_list()
        elif xaxis_type == 'index':
            x_item = single_pub_sub_df.index
            y_item = single_pub_sub_df.iloc[:, 1].to_list()
        elif xaxis_type == 'sim_time':
            x_item = single_pub_sub_df.iloc[:, 0].to_list()
            y_item = single_pub_sub_df.iloc[:, 1].to_list()

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

    def to_dataframe(
        self,
        xaxis_type: str = 'system_time'
    ) -> pd.DataFrame:
        validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass

    def _get_source_df_by_topic(
        self,
        source_df: pd.DataFrame
    ) -> Dict:
        source_df_by_topic = {}
        topic_names = set(source_df.columns.get_level_values(0).to_list())
        for topic_name in topic_names:
            topic_df = source_df.loc[:, pd.IndexSlice[topic_name, :]]
            topic_df.columns = topic_df.columns.droplevel(0)
            source_df_by_topic[topic_name] = topic_df

        return source_df_by_topic

    @staticmethod
    def _get_ts_column_name(
        pub_sub: Union[Publisher, Subscription]
    ) -> str:
        if isinstance(pub_sub, Publisher):
            if pub_sub.callback_names:
                ts_column_name = (f'{pub_sub.callback_names[0]}'
                                  '/rclcpp_publish_timestamp [ns]')
            else:
                ts_column_name = 'rclcpp_publish_timestamp [ns]'
        else:
            ts_column_name = f'{pub_sub.column_names[0]} [ns]'

        return ts_column_name

    def _get_converter(
        self
    ) -> ClockConverter:
        converter = self._pub_subs[0]._provider.get_sim_time_converter()

        return converter

    def _output_table_size_zero_warn(
        self,
        logger: Logger,
        metrics: str,
        pub_sub: Union[Publisher, Subscription]
    ) -> None:
        logger.warning(
            'Since no timestamp is recorded, '
            f'the {metrics} cannot be calculated. '
            f'pub_sub_summary: {pub_sub.summary}'
        )
