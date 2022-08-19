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
from collections import defaultdict
from logging import getLogger
from typing import Dict, Optional

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, figure, save, show
from bokeh.resources import CDN

import pandas as pd

from ipywidgets import interact, Dropdown

from caret_analyze.runtime.publisher import Publisher

from .util import apply_x_axis_offset, ColorSelector, get_range
from ...exceptions import UnsupportedTypeError

logger = getLogger(__name__)


class PubSubTimeSeriesPlot(metaclass=ABCMeta):

    _last_xaxis_type: Optional[str] = None
    _last_ywheel_zoom: bool = True
    _last_full_legends: bool = False
    _last_export_path: Optional[str] = None

    def show(
        self,
        xaxis_type: Optional[str] = None,
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None,
    ) -> None:
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)
        self._last_xaxis_type = xaxis_type
        self._last_ywheel_zoom = ywheel_zoom
        self._last_full_legends = full_legends
        self._last_export_path = export_path

        topic_dropdown = Dropdown(description='Topic:',
                                  options=['All', '/topic2', '/drive'])
        interact(self._show_core,
                 topic_name=topic_dropdown)

    def _show_core(
        self,
        topic_name: str
    ) -> None:
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
        fig_args = self._get_fig_args(self._last_xaxis_type,
                                      source_df.columns.to_list()[1],
                                      self._last_ywheel_zoom)
        p = figure(**fig_args)
        if self._last_xaxis_type == 'system_time':
            apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        p.add_tools(Hover)

        # Draw lines
        color_selector = \
            ColorSelector.create_instance(coloring_rule='publish_subscribe')
        legend_items = []
        pub_count = 0
        sub_count = 0
        for i in range(0, len(source_df.columns), 2):
            line_source_df = source_df.iloc[:, i:i+2].dropna()
            pub_sub_name = line_source_df.columns.to_list()[0]
            color = color_selector.get_color(
                None, None, None,
                pub_sub_name=pub_sub_name
            )
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
            renderer = p.line('x',
                              'y',
                              source=line_source,
                              color=color)
            legend_items.append((legend_label, [renderer]))

        # Add legends
        num_legend_threshold = 20  # TODO(atsushi)
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

        if self._last_export_path is None:
            show(p)
        else:
            save(p, self._last_export_path,
                 title='PubSub time-line', resources=CDN)

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

    def _get_fig_args(
        self,
        xaxis_type: str,
        y_axis_label: str,
        ywheel_zoom: bool
    ) -> dict:
        fig_args = {'height': 500,
                    'frame_height': 270,
                    'frame_width': 800,
                    'y_axis_label': y_axis_label,
                    'title': f'Time-line of communications {y_axis_label}'}

        if xaxis_type == 'system_time':
            fig_args['x_axis_label'] = 'system time [s]'
        elif xaxis_type == 'sim_time':
            fig_args['x_axis_label'] = 'simulation time [s]'
        else:
            fig_args['x_axis_label'] = xaxis_type

        if(ywheel_zoom):
            fig_args['active_scroll'] = 'wheel_zoom'
        else:
            fig_args['tools'] = ['xwheel_zoom', 'xpan', 'save', 'reset']
            fig_args['active_scroll'] = 'xwheel_zoom'

        return fig_args

    def to_dataframe(
        self,
        xaxis_type: Optional[str] = None
    ) -> pd.DataFrame:
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    def _validate_xaxis_type(self, xaxis_type: Optional[str]):
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass

    def _get_pub_name(
        self,
        pub: Publisher
    ) -> str:
        assert len(pub.callback_names) == 1  # HACK

        return (f'{pub.node_name}{pub.callback_names[0]}'
                '/rclcpp_publish_timestamp')

    def _create_pub_sub_df_dict(
        self
    ) -> Dict:
        pub_sub_df_dict = defaultdict(pd.DataFrame)
        for pub_sub in self._pub_subs:
            pub_sub_series = pub_sub.to_dataframe().iloc[:, 0]
            if isinstance(pub_sub, Publisher):
                pub_sub_series.rename(self._get_pub_name(pub_sub),
                                      inplace=True)
            pub_sub_df_dict[pub_sub.topic_name] = pd.concat(
                [pub_sub_df_dict[pub_sub.topic_name],
                 pub_sub_series],
                axis=1
            )

        return pub_sub_df_dict
    
    def _df_convert_to_sim_time(
        self,
        latency_table: pd.DataFrame
    ) -> None:
        # TODO(hsgwa): refactor
        converter = self._pub_subs[0]._provider.get_sim_time_converter()
        for c in range(len(latency_table.columns)):
            for i in range(len(latency_table)):
                latency_table.iat[i, c] = converter.convert(
                        latency_table.iat[i, c])
