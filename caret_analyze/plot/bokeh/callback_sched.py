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

from abc import abstractmethod

from typing import Dict, Sequence, Tuple, Union

from bokeh.colors import Color
from bokeh.io import show
from bokeh.palettes import d3
from bokeh.plotting import ColumnDataSource, figure

from .util import apply_x_axis_offset, get_callback_param_desc, RectValues
from ...common import Util
from ...exceptions import InvalidArgumentError
from ...record import Clip
from ...runtime import CallbackGroup, Executor, Node


def callback_sched(
    target: Union[Node, CallbackGroup, Executor],
    lstrip_s: float = 0,
    rstrip_s: float = 0,
    coloring_rule='callback',
):
    assert coloring_rule in ['callback', 'callback_group', 'node']

    cbgs, target_name = get_cbg_and_name(target)

    frame_min, frame_max = get_range(cbgs)
    clip_min = int(frame_min + lstrip_s*1.0e9)
    clip_max = int(frame_max - rstrip_s*1.0e9)
    clip = Clip(clip_min, clip_max)

    color_selector = ColorSelector.create_instance(coloring_rule)
    sched_plot_cbg(target_name, cbgs, color_selector, clip)


def get_cbg_and_name(
    target: Union[Node, CallbackGroup, Executor]
) -> Tuple[Sequence[CallbackGroup], str]:
    if isinstance(target, Node):
        if target.callback_groups is None:
            raise InvalidArgumentError('target.callback_groups is None')

        return target.callback_groups, target.node_name

    elif isinstance(target, Executor):
        return target.callback_groups, target.executor_name

    else:
        return [target], target.callback_group_name


def get_range(cbgs: Sequence[CallbackGroup]) -> Tuple[int, int]:
    frame_min = 2**64-1
    frame_max = 0

    callbacks = Util.flatten([cbg.callbacks for cbg in cbgs])
    for callback in callbacks:
        cb_min = min(callback.to_dataframe().min().values)
        cb_max = max(callback.to_dataframe().max().values)
        frame_min = min(frame_min, cb_min)
        frame_max = max(frame_max, cb_max)

    return frame_min, frame_max


def sched_plot_cbg(
    target_name: str,
    cbgs: Sequence[CallbackGroup],
    color_selector: ColorSelector,
    clipper: Clip,
):
    TOOLTIPS = """
    <div style="width:400px; word-wrap: break-word;">
    callback_start = @x_min [ns] <br>
    callback_end = @x_max [ns] <br>
    latency = @latency [ms] <br>
    <br>
    node_name = @node_name <br>
    callback_type = @callback_type <br>
    @callback_param <br>
    symbol = @symbol
    </div>
    """
    p = figure(x_axis_label='Time [s]',
               y_axis_label='',
               title=f'Time-line of callbacks in {target_name}',
               width=1200,
               tools=['xwheel_zoom', 'xpan', 'save', 'reset'],
               active_scroll='xwheel_zoom',
               tooltips=TOOLTIPS)

    x_range_name = 'x_plot_axis'
    apply_x_axis_offset(p, x_range_name, clipper.min_ns, clipper.max_ns)

    rect_y = 0.0
    rect_height = 0.3
    rect_y_step = -1.0

    for callback_group in cbgs:
        for callback in callback_group.callbacks:
            rect_source = get_callback_rects(callback, clipper, rect_y, rect_height)
            color = color_selector.get_color(
                callback.node_name,
                callback_group.callback_group_name,
                callback.callback_name)
            p.rect('x',
                   'y',
                   'width',
                   'height',
                   source=rect_source,
                   color=color,
                   legend_label=f'{callback.callback_name}',
                   x_range_name=x_range_name)
            rect_y += rect_y_step

    p.ygrid.grid_line_alpha = 0
    p.yaxis.visible = False
    p.legend.location = 'bottom_left'
    p.legend.click_policy = 'hide'
    p.add_layout(p.legend[0], 'right')

    show(p)


def get_callback_rects(
    callback,
    clip: Clip,
    y,
    height
) -> ColumnDataSource:
    y_min = y - height
    y_max = y + height

    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        'x_min': [],
        'x_max': [],
        'width': [],
        'latency': [],
        'height': [],
        'node_name': [],
        'callback_type': [],
        'callback_param': [],
        'symbol': []

    })

    callback_param = get_callback_param_desc(callback)

    df = callback.to_dataframe(shaper=clip)
    for item in df.itertuples():
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
            'node_name': [callback.node_name],
            'symbol': [callback.symbol],
            'callback_param': [callback_param],
            'callback_type': [f'{callback.callback_type}']
        }
        rect_source.stream(new_data)

    return rect_source


class ColorSelector:

    @staticmethod
    def create_instance(coloring_rule: str):
        if coloring_rule == 'callback':
            return ColorSelectorCallback()

        if coloring_rule == 'callback_group':
            return ColorSelectorCbg()

        if coloring_rule == 'node':
            return ColorSelectorNode()

    def __init__(self) -> None:
        self._palette = d3['Category20'][20]
        self._color_map: Dict[str, Color] = {}

    def get_color(self, node_name: str, cbg_name: str, callback_name: str) -> Color:
        color_hash = self._get_color_hash(node_name, cbg_name, callback_name)

        if color_hash not in self._color_map:
            color_index = len(self._color_map) % len(self._palette)
            self._color_map[color_hash] = self._palette[color_index]

        return self._color_map[color_hash]

    @abstractmethod
    def _get_color_hash(self, node_name: str, cbg_name: str, callback_name: str) -> Color:
        return


class ColorSelectorCallback(ColorSelector):

    def _get_color_hash(self, node_name: str, cbg_name: str, callback_name: str) -> Color:
        return callback_name


class ColorSelectorCbg(ColorSelector):

    def _get_color_hash(self, node_name: str, cbg_name: str, callback_name: str) -> Color:
        return cbg_name


class ColorSelectorNode(ColorSelector):

    def _get_color_hash(self, node_name: str, cbg_name: str, callback_name: str) -> Color:
        return node_name
