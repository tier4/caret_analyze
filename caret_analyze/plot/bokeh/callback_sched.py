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

from __future__ import annotations

from typing import Union

from bokeh.io import show
from bokeh.palettes import d3
from bokeh.plotting import ColumnDataSource, figure

import numpy as np

from ...runtime.callback_group import CallbackGroup
from ...runtime.executor import Executor
from ...runtime.node import Node


def callback_sched(
    target: Union[Node, CallbackGroup, Executor],
    lstrip_s: float = 0,
    rstrip_s: float = 0
                  ):
    if isinstance(target, Node):
        callback_sched_node(target, lstrip_s, rstrip_s)
    elif isinstance(target, CallbackGroup):
        callback_sched_cbg(target, lstrip_s, rstrip_s)
    elif isinstance(target, Executor):
        callback_sched_exec(target, lstrip_s, rstrip_s)


def callback_sched_node(target: Node, lstrip_s, rstrip_s):
    node_name = target.node_name
    dataframes_cb = []
    dataframes_cb = get_dataframe(target, lstrip_s, rstrip_s)
    sched_plot(node_name, target, dataframes_cb)


def callback_sched_cbg(target: CallbackGroup, lstrip_s, rstrip_s):
    cbg_name = target.callback_group_name
    dataframes_cb = []
    dataframes_cb = get_dataframe(target, lstrip_s, rstrip_s)
    sched_plot(cbg_name, target, dataframes_cb)


def callback_sched_exec(target: Executor, lstrip_s, rstrip_s):
    executor_name = target.executor_name
    dataframes_cb = []
    dataframes_cb = get_dataframe(target, lstrip_s, rstrip_s)
    sched_plot_cbg(executor_name, target, dataframes_cb)


def get_dataframe(target, lstrip_s, rstrip_s):
    return [target.to_dataframe(lstrip_s=lstrip_s, rstrip_s=rstrip_s)
            for target in target.callbacks]


def sched_plot(target_name: str, target, dataframe):

    colors = d3['Category20'][20]
    TOOLTIPS = """
    <div style="width:400px; word-wrap: break-word;">
    callback_start = @x_min [ns] <br>
    callback_end = @x_max [ns] <br>
    latency = @latency [ms] <br>
    @desc <br>
    callback_type = @callback_type
    </div>
    """
    p = figure(x_axis_label='Time [ms]',
               y_axis_label='',
               title=f'Time-line of callbacks in {target_name}',
               width=1200,
               active_scroll='wheel_zoom',
               tooltips=TOOLTIPS)
    top = .0
    bottom = top - 0.2
    counter = 0
    for callback, df_cb in zip(target.callbacks, dataframe):
        rect_source = get_callback_rects(callback, df_cb, bottom, top)
        p.rect('x',
               'y',
               'width',
               'height',
               source=rect_source,
               color=colors[counter],
               legend_label=f'{callback.callback_name}')
        counter += 1
        top -= 0.3
        bottom = top - 0.2
    p.yaxis.visible = False
    p.legend.location = 'bottom_left'
    p.legend.click_policy = 'hide'
    p.add_layout(p.legend[0], 'right')

    show(p)


def sched_plot_cbg(target_name: str, target, dataframe):
    colors = d3['Category20'][20]
    TOOLTIPS = """
    <div style="width:400px; word-wrap: break-word;">
    callback_start = @x_min [ns] <br>
    callback_end = @x_max [ns] <br>
    latency = @latency [ms] <br>
    @desc <br>
    callback_type = @callback_type
    </div>
    """
    p = figure(x_axis_label='Time [ms]',
               y_axis_label='',
               title=f'Time-line of callbacks in {target_name}',
               width=1200,
               active_scroll='wheel_zoom',
               tooltips=TOOLTIPS)
    top = .0
    bottom = top - 0.2
    counter = 0

    for callback_group in target.callback_groups:
        for callback, df_cb in zip(callback_group.callbacks, dataframe):
            rect_source = get_callback_rects(callback, df_cb, bottom, top)
            p.rect('x',
                   'y',
                   'width',
                   'height',
                   source=rect_source,
                   color=colors[counter],
                   legend_label=f'{callback.callback_name} ({callback_group.callback_group_name})')
            top -= 0.3
            bottom = top - 0.2
        counter += 1

    p.yaxis.visible = False
    p.legend.location = 'bottom_left'
    p.legend.click_policy = 'hide'
    p.add_layout(p.legend[0], 'right')

    show(p)


def get_callback_rects(
    callback,
    dataframe,
    y_min,
    y_max
) -> ColumnDataSource:
    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        'x_min': [],
        'x_max': [],
        'width': [],
        'latency': [],
        'height': [],
        'desc': [],
        'callback_type': []
    })
    for item in dataframe.itertuples():
        callback_start = item._1
        callback_end = item._2
        rect = RectValues(callback_start, callback_end, y_min, y_max)
        new_data = {
            'x': [rect.x],
            'y': [rect.y],
            'x_min': [callback_start],
            'x_max': [callback_end],
            'width': [rect.width],
            'latency': [(callback_end-callback_start)*1.0e-6],
            'height': [rect.height],
            'desc': [f'symbol: {callback.symbol}'],
            'callback_type': [f'{callback.callback_type}']
                    }
        rect_source.stream(new_data)

    return rect_source


class RectValues():
    def __init__(
        self,
        callback_start: float,
        callback_end: float,
        y_min: int,
        y_max: int
    ) -> None:
        self._y = [y_min, y_max]
        self._x = [callback_start, callback_end]

    @property
    def x(self) -> float:
        return np.mean(self._x)

    @property
    def y(self) -> float:
        return np.mean(self._y)

    @property
    def width(self) -> float:
        return abs(self._x[0] - self._x[1])

    @property
    def height(self) -> float:
        return abs(self._y[0] - self._y[1])
