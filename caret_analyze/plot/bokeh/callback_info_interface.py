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

from bokeh.palettes import d3
from bokeh.plotting import figure, show

import pandas as pd

from .callback_sched import get_range
from .util import apply_x_axis_offset
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
        self._callbacks = []
        if(isinstance(target, (Application, Executor, Node, CallbackGroup))):
            self._callbacks = target.callbacks
        else:
            self._callbacks = target

    def show(self, xaxis_type: Optional[str] = None, y_axis_scroll: bool = True):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        if(xaxis_type == 'system_time'):
            self._show_with_system_time(y_axis_scroll)
        elif(xaxis_type == 'sim_time'):
            self._show_with_sim_time(y_axis_scroll)
        elif(xaxis_type == 'index'):
            self._show_with_index(y_axis_scroll)

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
                latency_table[c][i] = converter.convert(latency_table[c][i])

    def _get_fig_args(self, x_axis_label: str, y_axis_label: str, y_axis_scroll: bool) -> dict:
        fig_args = {'height': 300,
                    'width': 1000,
                    'x_axis_label': x_axis_label,
                    'y_axis_label': y_axis_label,
                    'title': f'Time-line of callbacks {y_axis_label}'}
        if(y_axis_scroll):
            fig_args['active_scroll'] = 'wheel_zoom'
        else:
            fig_args['tools'] = ['xwheel_zoom', 'xpan', 'save', 'reset']
            fig_args['active_scroll'] = 'xwheel_zoom'

        return fig_args

    def _show_with_index(self, y_axis_scroll: bool):
        source_df = self._to_dataframe_core('index')
        l1_columns = source_df.columns.get_level_values(1).to_list()
        colors = d3['Category20'][20]
        fig_args = self._get_fig_args('index', l1_columns[1], y_axis_scroll)
        p = figure(**fig_args)

        for i, callback_name in enumerate(source_df.columns.get_level_values(0).to_list()):
            single_cb_df = source_df.loc[:, (callback_name,)].dropna()
            p.line(x=single_cb_df.index,
                   y=single_cb_df.loc[:, l1_columns[1]].to_list(),
                   line_color=colors[i],
                   legend_label=callback_name)

        p.add_layout(p.legend[0], 'right')
        show(p)

    def _show_with_sim_time(self, y_axis_scroll: bool):
        source_df = self._to_dataframe_core('sim_time')
        l1_columns = source_df.columns.get_level_values(1).to_list()
        colors = d3['Category20'][20]
        fig_args = self._get_fig_args('simulation time [s]', l1_columns[1], y_axis_scroll)
        p = figure(**fig_args)

        for i, callback_name in enumerate(source_df.columns.get_level_values(0).to_list()):
            p.line(l1_columns[0],
                   l1_columns[1],
                   source=source_df.loc[:, (callback_name,)].dropna(),
                   line_color=colors[i],
                   legend_label=callback_name)

        p.add_layout(p.legend[0], 'right')
        show(p)

    def _show_with_system_time(self, y_axis_scroll: bool):
        source_df = self._to_dataframe_core('system_time')
        l1_columns = source_df.columns.get_level_values(1).to_list()
        colors = d3['Category20'][20]
        fig_args = self._get_fig_args('system time [s]', l1_columns[1], y_axis_scroll)
        p = figure(**fig_args)

        frame_min, frame_max = get_range(self._callbacks)
        apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        for i, callback_name in enumerate(source_df.columns.get_level_values(0).to_list()):
            single_cb_df = source_df.loc[:, (callback_name,)].dropna()
            p.line(x=((single_cb_df.loc[:, l1_columns[0]]-frame_min)*10**(-9)).to_list(),
                   y=single_cb_df.loc[:, l1_columns[1]].to_list(),
                   line_color=colors[i],
                   legend_label=callback_name)

        p.add_layout(p.legend[0], 'right')
        show(p)
