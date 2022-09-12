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

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, Figure, figure, save, show
from bokeh.resources import CDN

import pandas as pd

from .plot_util import get_fig_args, PlotColorSelector, validate_xaxis_type
from .util import apply_x_axis_offset, get_range
from ...common import ClockConverter
from ...runtime import Communication

logger = getLogger(__name__)


class CommunicationTimeSeriesPlot(metaclass=ABCMeta):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        self._communications = communications

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None
    ) -> Figure:
        validate_xaxis_type(xaxis_type)
        Hover = HoverTool(
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
        frame_min, frame_max = get_range(self._communications)
        source_df = self._to_dataframe_core(xaxis_type)
        y_axis_label = source_df.columns.get_level_values(1).to_list()[1]
        fig_args = get_fig_args(
            xaxis_type=xaxis_type,
            title=f'Time-line of communications {y_axis_label}',
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
        for i, comm in enumerate(self._communications):
            color = color_selector.get_color(self._get_comm_name(comm))
            line_source = self._get_comm_lines(
                comm,
                source_df,
                frame_min,
                xaxis_type
            )
            renderer = p.line('x', 'y',
                              source=line_source,
                              color=color)
            legend_label = f'communication{i}'
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
                 title='communication time-line', resources=CDN)

        return p

    def _get_comm_lines(
        self,
        comm: Communication,
        source_df: pd.DataFrame,
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        comm_name = self._get_comm_name(comm)
        single_comm_df = source_df.loc[:, (comm_name,)].dropna()
        if xaxis_type == 'system_time':
            x_item = ((single_comm_df.iloc[:, 0] - frame_min)
                      * 10**(-9)).to_list()
            y_item = single_comm_df.iloc[:, 1].to_list()
        elif xaxis_type == 'index':
            x_item = single_comm_df.index
            y_item = single_comm_df.iloc[:, 1].to_list()
        elif xaxis_type == 'sim_time':
            x_item = single_comm_df.iloc[:, 0].to_list()
            y_item = single_comm_df.iloc[:, 1].to_list()

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

    def to_dataframe(
        self,
        xaxis_type: str = 'system_time'
    ) -> pd.DataFrame:
        validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass

    def _get_converter(
        self
    ) -> ClockConverter:
        converter_cb = \
            self._communications[0]._callback_subscription[0]
        converter = converter_cb._provider.get_sim_time_converter()

        return converter

    def _get_comm_name(
        self,
        comm: Communication
    ) -> str:
        return (f'{comm.summary["publish_node"]}|'
                f'{comm.summary["topic_name"]}|'
                f'{comm.summary["subscribe_node"]}')
