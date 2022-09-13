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
from typing import Dict, List, Optional, Union

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, Figure, figure, save, show
from bokeh.resources import CDN

from ipywidgets import Dropdown, interact

import pandas as pd

from .plot_util import get_fig_args, PlotColorSelector, validate_xaxis_type
from .util import apply_x_axis_offset, get_range
from ...common import ClockConverter
from ...runtime import Publisher, Subscription

logger = getLogger(__name__)


class PubSubTimeSeriesPlot(metaclass=ABCMeta):

    _last_xaxis_type: Optional[str] = None
    _last_ywheel_zoom: bool = True
    _last_full_legends: bool = False
    _last_export_path: Optional[str] = None

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        self._pub_subs = pub_subs

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None,
        interactive: bool = False
    ) -> Union[List[Figure], None]:
        validate_xaxis_type(xaxis_type)
        self._last_xaxis_type = xaxis_type
        self._last_ywheel_zoom = ywheel_zoom
        self._last_full_legends = full_legends
        self._last_export_path = export_path

        all_topic_names = sorted({ps.topic_name for ps in self._pub_subs})
        # not interactive
        if self._last_export_path:
            self._show_core('All')
            return None
        if len(all_topic_names) == 1:
            return [self._show_core('All')]
        elif not interactive:
            return [self._show_core(topic_name) for
                    topic_name in ['All'] + all_topic_names]
        # interactive
        else:
            topic_dropdown = Dropdown(description='Topic:',
                                      options=(['All'] + all_topic_names))
            interact(self._show_core,
                     topic_name=topic_dropdown)

    def _show_core(
        self,
        topic_name: str
    ) -> Figure:
        source_df = self._to_dataframe_core(self._last_xaxis_type)
        source_df_by_topic = self._get_source_df_by_topic(source_df)
        if topic_name == 'All':
            source_df.columns = source_df.columns.droplevel(0)
        else:
            source_df = source_df_by_topic[topic_name]

        Hover = HoverTool(
                    tooltips="""
                    <div style="width:400px; word-wrap: break-word;">
                    <br>
                    node_name = @node_name <br>
                    callback_name = @callback_name <br>
                    </div>
                    """,
                    point_policy='follow_mouse'
                )
        frame_min, frame_max = get_range(self._pub_subs)
        y_axis_label = source_df.columns.to_list()[1]
        fig_args = get_fig_args(
            xaxis_type=self._last_xaxis_type,
            title=f'Time-line of publishes/subscribes {y_axis_label}',
            y_axis_label=y_axis_label,
            ywheel_zoom=self._last_ywheel_zoom
        )
        p = figure(**fig_args)
        if self._last_xaxis_type == 'system_time':
            apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        p.add_tools(Hover)

        # Draw lines
        color_selector = PlotColorSelector()
        legend_items = []
        pub_count = 0
        sub_count = 0
        for i in range(0, len(source_df.columns), 2):
            line_source_df = source_df.iloc[:, i:i+2].dropna()
            pub_sub_name = line_source_df.columns.to_list()[0]
            color = color_selector.get_color(pub_sub_name)
            line_source = self._get_pub_sub_lines(
                line_source_df,
                frame_min,
                self._last_xaxis_type
            )

            if 'rclcpp_publish_timestamp' in pub_sub_name:
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
            if not self._last_full_legends and i >= num_legend_threshold:
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
        if self._last_export_path is None:
            show(p)
        else:
            save(p, self._last_export_path,
                 title='PubSub time-line', resources=CDN)

        return p

    def _get_pub_sub_lines(
        self,
        line_source_df: pd.DataFrame,
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        def get_node_name(
            line_source_df: pd.DataFrame
        ) -> str:
            return line_source_df.columns.to_list()[0].split('/')[1]

        def get_callback_name(
            line_source_df: pd.DataFrame
        ) -> str:
            return line_source_df.columns.to_list()[0].split('/')[2]

        if xaxis_type == 'system_time':
            x_item = ((line_source_df.iloc[:, 0] - frame_min)
                      * 10**(-9)).to_list()
            y_item = line_source_df.iloc[:, 1].to_list()
        elif xaxis_type == 'index':
            x_item = line_source_df.index
            y_item = line_source_df.iloc[:, 1].to_list()
        elif xaxis_type == 'sim_time':
            x_item = line_source_df.iloc[:, 0].to_list()
            y_item = line_source_df.iloc[:, 1].to_list()

        line_source = ColumnDataSource(
            data={
                'x': [],
                'y': [],
                'node_name': [],
                'callback_name': [],
            }
        )
        for x, y in zip(x_item, y_item):
            new_data = {
                'x': [x],
                'y': [y],
                'node_name': [get_node_name(line_source_df)],
                'callback_name': [get_callback_name(line_source_df)],
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
                ts_column_name = '/rclcpp_publish_timestamp [ns]'
        else:
            ts_column_name = f'{pub_sub.column_names[0]} [ns]'

        return ts_column_name

    def _get_converter(
        self
    ) -> ClockConverter:
        converter = self._pub_subs[0]._provider.get_sim_time_converter()

        return converter
