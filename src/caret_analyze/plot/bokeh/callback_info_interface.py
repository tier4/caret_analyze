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
from typing import List, Optional, Union

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, Figure, figure, save, show
from bokeh.resources import CDN


import pandas as pd

from .plot_util import get_fig_args, validate_xaxis_type
from .util import (apply_x_axis_offset, ColorSelector,
                   get_callback_param_desc, get_range)
from ...common import ClockConverter
from ...runtime import (Application, CallbackBase, CallbackGroup,
                        Executor, Node, Path)

logger = getLogger(__name__)

CallbacksType = Union[Application, Path, Executor, Node,
                      CallbackGroup, CallbackBase, List[CallbackBase]]


class TimeSeriesPlot(metaclass=ABCMeta):
    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__()
        self._callbacks: List[CallbackBase] = []
        if(isinstance(target, (Application, Executor, Node, CallbackGroup))):
            self._callbacks = target.callbacks
        elif(isinstance(target, Path)):
            for comm in target.communications:
                self._callbacks += comm.publish_node.callbacks
            self._callbacks += \
                target.communications[-1].subscribe_node.callbacks
        elif(isinstance(target, CallbackBase)):
            self._callbacks = [target]
        else:
            self._callbacks = target

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None
    ) -> Figure:
        """
        Draw a line graph for each callback using the bokeh library.

        Parameters
        ----------
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        export_path : Optional[str]
            If you give path, the drawn graph will be saved as a file.

        Returns
        -------
        bokeh.plotting.Figure

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        """
        validate_xaxis_type(xaxis_type)
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
        source_df = self._to_dataframe_core(xaxis_type)
        y_axis_label = source_df.columns.get_level_values(1).to_list()[1]
        fig_args = get_fig_args(
            xaxis_type=xaxis_type,
            title=f'Time-line of callbacks {y_axis_label}',
            y_axis_label=y_axis_label,
            ywheel_zoom=ywheel_zoom
        )
        p = figure(**fig_args)
        if xaxis_type == 'system_time':
            apply_x_axis_offset(p, 'x_axis_plot', frame_min, frame_max)
        p.add_tools(Hover)

        # Draw lines
        color_selector = \
            ColorSelector.create_instance(coloring_rule='callback')
        legend_items = []
        for i, callback in enumerate(self._callbacks):
            color = color_selector.get_color(
                callback.node_name,
                None,
                callback.callback_name
            )
            line_source = self._get_callback_lines(callback,
                                                   source_df,
                                                   frame_min,
                                                   xaxis_type)
            renderer = p.line('x', 'y',
                              source=line_source,
                              color=color)
            legend_label = f'callback{i}'
            legend_items.append((legend_label, [renderer]))

        # Add legends by ten
        num_legend_threshold = 20
        # In Autoware, the number of callbacks in a node is less than 20.
        # Here, num_legend_threshold is set to 20 as the maximum value.
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
            save(p, export_path, title='callback time-line', resources=CDN)

    def _get_callback_lines(
        self,
        callback: CallbackBase,
        source_df: pd.DataFrame,
        frame_min: int,
        xaxis_type: str
    ) -> ColumnDataSource:
        single_cb_df = source_df.loc[:, (callback.callback_name,)].dropna()
        if xaxis_type == 'system_time':
            x_item = ((single_cb_df.iloc[:, 0] - frame_min)*10**(-9)).to_list()
            y_item = single_cb_df.iloc[:, 1].to_list()
        elif xaxis_type == 'index':
            x_item = single_cb_df.index
            y_item = single_cb_df.iloc[:, 1].to_list()
        elif xaxis_type == 'sim_time':
            x_item = single_cb_df.iloc[:, 0].to_list()
            y_item = single_cb_df.iloc[:, 1].to_list()
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

    def to_dataframe(self, xaxis_type: str = 'system_time'):
        """
        Get time series data for each callback in pandas DataFrame format.

        Parameters
        ----------
        xaxis_type : str
            Type of time for timestamp.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".

        Raises
        ------
        UnsupportedTypeError
            Argument xaxis_type is not "system_time", "index", or "sim_time".

        Notes
        -----
        xaxis_type "system_time" and "index" return the same DataFrame.

        """
        validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str):
        pass

    def _concate_cb_latency_table(self) -> pd.DataFrame:
        callbacks_latency_table = pd.DataFrame()
        for callback in self._callbacks:
            callback_latency_table = callback.to_dataframe()
            callbacks_latency_table = pd.concat(
                    [callbacks_latency_table, callback_latency_table],
                    axis=1)

        return callbacks_latency_table

    def _get_converter(
        self
    ) -> ClockConverter:
        converter = self._callbacks[0]._provider.get_sim_time_converter()

        return converter
