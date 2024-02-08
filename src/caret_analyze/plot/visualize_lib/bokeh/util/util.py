# Copyright 2021 TIER IV, Inc.
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

import datetime

from bokeh.models import AdaptiveTicker, Range1d
from bokeh.plotting import figure as Figure

import numpy as np

from .....exceptions import UnsupportedTypeError
from .....runtime import CallbackBase, SubscriptionCallback, TimerCallback


class RectValues:
    def __init__(
        self,
        callback_start: float,
        callback_end: float,
        y_min: float,
        y_max: float
    ) -> None:
        self._y = [y_min, y_max]
        self._x = [callback_start, callback_end]

    @property
    def x(self) -> float:
        return float(np.mean(self._x))

    @property
    def y(self) -> float:
        return float(np.mean(self._y))

    @property
    def width(self) -> float:
        return abs(self._x[0] - self._x[1])

    @property
    def height(self) -> float:
        return abs(self._y[0] - self._y[1])


def init_figure(
    title: str,
    ywheel_zoom: bool,
    xaxis_type: str,
    y_axis_label: str | None = None,
    x_axis_label: str | None = None,
) -> Figure:
    if x_axis_label is None:
        if xaxis_type == 'system_time':
            x_axis_label = 'system time [s]'
        elif xaxis_type == 'sim_time':
            x_axis_label = 'simulation time [s]'
        else:
            x_axis_label = xaxis_type

    if ywheel_zoom:
        tools = ['wheel_zoom', 'pan', 'box_zoom', 'save', 'reset']
        active_scroll = 'wheel_zoom'
    else:
        tools = ['xwheel_zoom', 'xpan', 'save', 'reset']
        active_scroll = 'xwheel_zoom'

    return Figure(
        frame_height=270, frame_width=800, title=title, y_axis_label=y_axis_label or '',
        x_axis_label=x_axis_label, tools=tools, active_scroll=active_scroll
    )


def apply_x_axis_offset(
    fig: Figure,
    min_ns: float,
    max_ns: float
) -> None:
    """
    Apply an offset to the x-axis of the graph.

    Datetime is displayed instead of UNIX time for zero point.

    Parameters
    ----------
    fig : Figure
        Target figure.
    min_ns : float
        Minimum UNIX time.
    max_ns : float
        Maximum UNIX time.

    """
    # Initialize variables
    offset_s = min_ns*1.0e-9
    end_s = (max_ns-min_ns)*1.0e-9
    applied_range = Range1d(start=0, end=end_s)

    # Set ranges
    fig.x_range = applied_range  # type: ignore

    # set ticker
    fig.xaxis.ticker = AdaptiveTicker(min_interval=0.1, mantissas=[1, 2, 5])

    # Add xgrid
    fig.xgrid.minor_grid_line_color = 'black'
    fig.xgrid.minor_grid_line_alpha = 0.1

    # Replace 0 with datetime of offset_s
    datetime_s = datetime.datetime.fromtimestamp(offset_s).strftime('%Y-%m-%d %H:%M:%S')
    fig.xaxis.major_label_overrides = {0: datetime_s}

    # # Code to display hhmmss for x-axis
    # from bokeh.models import FuncTickFormatter
    # fig.xaxis.formatter = FuncTickFormatter(
    #     code = '''
    #     let time_ms = (tick + offset_s) * 1e3;
    #     let date_time = new Date(time_ms);
    #     let hh = date_time.getHours();
    #     let mm = date_time.getMinutes();
    #     let ss = date_time.getSeconds();
    #     return hh + ":" + mm + ":" + ss;
    #     ''',
    #     args={"offset_s": offset_s})


def get_callback_param_desc(callback: CallbackBase):
    if isinstance(callback, TimerCallback):
        return f'period_ns = {callback.period_ns}'

    if isinstance(callback, SubscriptionCallback):
        return f'topic_name = {callback.subscribe_topic_name}'

    raise UnsupportedTypeError('callback type must be '
                               '[ TimerCallback/ SubscriptionCallback]')
