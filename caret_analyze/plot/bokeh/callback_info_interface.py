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

    def show(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        self._show_core(xaxis_type)

    def to_dataframe(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str):
        pass

    @abstractmethod
    def _show_core(self, xaxis_type: str):
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

    def _show_from_multi_index_df(
        self,
        source_df: pd.DataFrame,
        xaxis_type: str,
        title=''
    ) -> None:
        l1_columns = source_df.columns.get_level_values(1).to_list()
        colors = d3['Category20'][20]
        p = figure(height=300,
                   width=1000,
                   x_axis_label=f'{xaxis_type}',
                   y_axis_label=f'{l1_columns[1]}',
                   title=title,
                   tools=['xwheel_zoom', 'xpan', 'save', 'reset'],
                   active_scroll='xwheel_zoom')

        source_df = self._to_dataframe_core(xaxis_type)
        if(xaxis_type == 'index'):
            for i, callback_name in enumerate(source_df.columns.get_level_values(0).to_list()):
                single_cb_df = source_df.loc[:, (callback_name,)].dropna()
                p.line(x=single_cb_df.index,
                       y=single_cb_df.loc[:, l1_columns[1]].to_list(),
                       line_color=colors[i],
                       legend_label=callback_name)
        else:
            for i, callback_name in enumerate(source_df.columns.get_level_values(0).to_list()):
                p.line(l1_columns[0],
                       l1_columns[1],
                       source=source_df.loc[:, (callback_name,)].dropna(),
                       line_color=colors[i],
                       legend_label=callback_name)

        p.add_layout(p.legend[0], 'right')
        show(p)
