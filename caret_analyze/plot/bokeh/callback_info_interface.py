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
from typing import List, Optional, Union

from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource, figure, show

import pandas as pd

from .callback_sched import ColorSelector, get_range
from .util import apply_x_axis_offset, get_callback_param_desc
from ...exceptions import UnsupportedTypeError
from ...runtime import Application, CallbackBase, CallbackGroup, Executor, Node

CallbacksType = Union[Application, Executor,
                      Node, CallbackGroup, List[CallbackBase]]


class TimeSeriesPlot(metaclass=ABCMeta):
    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__()
        self._callbacks: List[CallbackBase] = []
        if(isinstance(target, (Application, Executor, Node, CallbackGroup))):
            self._callbacks = target.callbacks
        else:
            self._callbacks = target

    def show(self, xaxis_type: Optional[str] = None, ywheel_zoom: bool = True):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)
        Hover = HoverTool(
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
        frame_min, frame_max = get_range(self._callbacks)
        if(xaxis_type == 'system_time'):
            source_df = self._to_dataframe_core('system_time')
            l1_columns = source_df.columns.get_level_values(1).to_list()
            fig_args = self._get_fig_args('system time [s]', l1_columns[1], ywheel_zoom)
            p = figure(**fig_args)
            apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        elif(xaxis_type == 'sim_time'):
            source_df = self._to_dataframe_core('sim_time')
            l1_columns = source_df.columns.get_level_values(1).to_list()
            fig_args = self._get_fig_args('simulation time [s]', l1_columns[1], ywheel_zoom)
            p = figure(**fig_args)
        elif(xaxis_type == 'index'):
            source_df = self._to_dataframe_core('index')
            l1_columns = source_df.columns.get_level_values(1).to_list()
            fig_args = self._get_fig_args('index', l1_columns[1], ywheel_zoom)
            p = figure(**fig_args)
        p.add_tools(Hover)
        coloring_rule = 'callback'
        color_selector = ColorSelector.create_instance(coloring_rule)
        for i, callback in enumerate(self._callbacks):
            color = color_selector.get_color(
                callback.node_name,
                None,
                callback.callback_name)
            line_source = get_callback_lines(callback,
                                             source_df,
                                             l1_columns,
                                             frame_min,
                                             xaxis_type)
            p.line('x',
                   'y',
                   source=line_source,
                   legend_label=f'callback{i}',
                   color=color)
        p.add_layout(p.legend[0], 'right')
        show(p)

    def to_dataframe(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str):
        pass

    def _validate_xaxis_type(self, xaxis_type: Optional[str]):
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )

    def _concate_cb_latency_table(self) -> pd.DataFrame:
        callbacks_latency_table = pd.DataFrame()
        for callback in self._callbacks:
            callback_latency_table = callback.to_dataframe()
            callbacks_latency_table = pd.concat(
                    [callbacks_latency_table, callback_latency_table],
                    axis=1)

        return callbacks_latency_table

    def _df_convert_to_sim_time(self, latency_table: pd.DataFrame) -> None:
        converter = self._callbacks[0]._provider.get_sim_time_converter()
        for c in range(len(latency_table.columns)):
            for i in range(len(latency_table)):
                latency_table.iat[i, c] = converter.convert(latency_table.iat[i, c])

    def _get_fig_args(self, x_axis_label: str, y_axis_label: str, ywheel_zoom: bool) -> dict:
        fig_args = {'height': 300,
                    'width': 1000,
                    'x_axis_label': x_axis_label,
                    'y_axis_label': y_axis_label,
                    'title': f'Time-line of callbacks {y_axis_label}'}
        if(ywheel_zoom):
            fig_args['active_scroll'] = 'wheel_zoom'
        else:
            fig_args['tools'] = ['xwheel_zoom', 'xpan', 'save', 'reset']
            fig_args['active_scroll'] = 'xwheel_zoom'

        return fig_args


def get_callback_lines(callback: CallbackBase,
                       source_df,
                       l1_columns,
                       frame_min,
                       type_name) -> ColumnDataSource:
    single_cb_df = source_df.loc[:, (callback.callback_name,)].dropna()
    if type_name == 'system_time':
        x_item = ((single_cb_df.loc[:, l1_columns[0]]-frame_min)*10**(-9)).to_list()
        y_item = single_cb_df.loc[:, l1_columns[1]].to_list()
    elif type_name == 'index':
        x_item = single_cb_df.index
        y_item = single_cb_df.loc[:, l1_columns[1]].to_list()
    elif type_name == 'sim_time':
        x_item = single_cb_df.loc[:, l1_columns[0]].to_list()
        y_item = single_cb_df.loc[:, l1_columns[1]].to_list()
    line_source = ColumnDataSource(data={
                                       'x': [],
                                       'y': [],
                                       'node_name': [],
                                       'callback_name': [],
                                       'callback_type': [],
                                       'callback_param': [],
                                       'symbol': []
                                            })
    callback_param = get_callback_param_desc(callback)
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
