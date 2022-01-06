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

from typing import Sequence, Union

from bokeh.io import show
from bokeh.palettes import d3
from bokeh.plotting import ColumnDataSource, figure

import numpy as np

from ...exceptions import InvalidArgumentError
from ...runtime.callback_group import CallbackGroup
from ...runtime.executor import Executor
from ...runtime.node import Node


def callback_sched(
    target: Union[Node, CallbackGroup, Executor],
    lstrip_s: float = 0,
    rstrip_s: float = 0
):
    cbgs: Sequence[CallbackGroup]
    target_name: str

    if isinstance(target, Node):
        if target.callback_groups is None:
            raise InvalidArgumentError('target.callback_groups is None')

        cbgs = target.callback_groups
        target_name = target.node_name

    elif isinstance(target, Executor):
        cbgs = target.callback_groups
        target_name = target.executor_name

    else:
        cbgs = [target]
        target_name = target.callback_group_name

    sched_plot_cbg(target_name, cbgs, lstrip_s, rstrip_s)


def sched_plot_cbg(
    target_name: str,
    cbgs: Sequence[CallbackGroup],
    lstrip_s: float = 0,
    rstrip_s: float = 0
):
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

    for callback_group in cbgs:
        for callback in callback_group.callbacks:
            df_cb = callback.to_dataframe(lstrip_s=lstrip_s, rstrip_s=rstrip_s)
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
