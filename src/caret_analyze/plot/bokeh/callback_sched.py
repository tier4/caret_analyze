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
from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Union

from bokeh.colors import Color, RGB
from bokeh.io import save, show
from bokeh.models import Arrow, HoverTool, NormalHead
from bokeh.plotting import ColumnDataSource, Figure, figure
from bokeh.resources import CDN

from caret_analyze.runtime.callback import TimerCallback

import colorcet as cc

import pandas as pd

from .util import (apply_x_axis_offset,
                   get_callback_param_desc, get_range, RectValues)
from ...common import ClockConverter, type_check_decorator, UniqueList, Util
from ...exceptions import InvalidArgumentError
from ...record import Clip
from ...runtime import (Application, CallbackBase, CallbackGroup,
                        Executor, Node, Path)

logger = getLogger(__name__)

CallbackGroupTypes = Union[Application, Executor, Path, Node,
                           CallbackGroup, List[CallbackGroup]]


@type_check_decorator
def callback_sched(
    target: CallbackGroupTypes,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
    coloring_rule: str = 'callback',
    use_sim_time: bool = False,
    export_path: Optional[str] = None
) -> Figure:
    """
    Visualize callback scheduling behavior.

    Parameters
    ----------
    target : CallbackGroupTypes
        CallbackGroupTypes = Union[Application,
                                   Executor,
                                   Path,
                                   Node,
                                   CallbackGroup,
                                   List[CallbackGroup]].
        The target which you want to visualize.
    lstrip_s : float
        Left strip. The default value is 0.
    rstrip_s : float
        Right strip. The default value is 0.
    coloring_rule : str
        The unit of color change.
        There are three rules which are callback, callback_group and node.
        The default value is "callback".
    use_sim_time: bool
        If you want to use the simulation time,
        you can set this Parameter to True.
    export_path : Optional[str]
        If you give path, the drawn graph will be saved as a file.

    Returns
    -------
    bokeh.plotting.Figure

    """
    assert coloring_rule in ['callback', 'callback_group', 'node']

    callback_groups, target_name = get_cbg_and_name(target)
    callbacks = Util.flatten([cbg.callbacks for cbg in callback_groups])
    frame_min, frame_max = get_range(callbacks)
    clip_min = int(frame_min + lstrip_s*1.0e9)
    clip_max = int(frame_max - rstrip_s*1.0e9)
    clip = Clip(clip_min, clip_max)

    color_selector = ColorSelector.create_instance(coloring_rule)
    figure = sched_plot_cbg(target_name, callback_groups, color_selector,
                            clip, use_sim_time, export_path)
    return figure


def get_cbg_and_name(
    target: CallbackGroupTypes
) -> Tuple[Sequence[CallbackGroup], str]:
    """
    Get callback group of target and its name.

    Parameters
    ----------
    target: CallbackGroupTypes
        CallbackGroupTypes = Union[Application,
                                   Executor,
                                   Path,
                                   Node,
                                   CallbackGroup,
                                   List[CallbackGroup]].
        The target which you want to visualize.

    Returns
    -------
    Tuple[Sequence[CallbackGroup], str]
        callback group instance and the name of target

    """
    if (isinstance(target, Application)):
        return target.callback_groups, 'application'

    elif (isinstance(target, Executor)):
        return target.callback_groups, target.executor_name

    elif (isinstance(target, Path)):
        callback_groups = UniqueList()
        for comm in target.communications:
            assert comm.publish_node.callback_groups is not None
            for cbg in comm.publish_node.callback_groups:
                callback_groups.append(cbg)

        callback_groups = target.communications[-1].subscribe_node.callback_groups or []
        for cbg in callback_groups:
            callback_groups.append(cbg)
        assert target.path_name is not None
        return callback_groups.as_list(), target.path_name

    elif (isinstance(target, Node)):
        if target.callback_groups is None:
            raise InvalidArgumentError('target.callback_groups is None')
        return target.callback_groups, target.node_name

    elif (isinstance(target, CallbackGroup)):
        return [target], target.callback_group_name

    else:
        return target, ' and '.join([t.callback_group_name for t in target])


def sched_plot_cbg(
    target_name: str,
    callback_groups: Sequence[CallbackGroup],
    color_selector: ColorSelector,
    clipper: Clip,
    use_sim_time: bool,
    export_path: Optional[str] = None
) -> Figure:
    """
    Show the graph of callback scheduling visualization.

    Parameters
    ----------
    target_name : str
        target name
    callback_groups : Sequence[CallbackGroup]
        callback group
    color_selector : ColorSelector
        color selector
    clipper : Clip
        Values outside the range are replaced by the minimum or maximum value
    use_sim_time : bool
        If you want to use the simulation time,
        you can set this Parameter to True.
    export_path : Optional[str]
        If you give path, the drawn graph will be saved as a file.

    Returns
    -------
    bokeh.plotting.Figure

    """
    p = figure(
               x_axis_label='Time [s]',
               y_axis_label='',
               title=f'Time-line of callbacks in {target_name}',
               width=1200,
               tools=['xwheel_zoom', 'xpan', 'save', 'reset'],
               active_scroll='xwheel_zoom',
                )
    p.sizing_mode = 'stretch_width'
    x_range_name = 'x_plot_axis'
    converter: Optional[ClockConverter] = None
    if use_sim_time:
        cbs: List[CallbackBase] = Util.flatten(
            cbg.callbacks for cbg in callback_groups if len(cbg.callbacks) > 0)
        # TODO(hsgwa): refactor
        converter = cbs[0]._provider.get_sim_time_converter()
        frame_min = converter.convert(clipper.min_ns)
        frame_max = converter.convert(clipper.max_ns)
    else:
        frame_min = clipper.min_ns
        frame_max = clipper.max_ns
    apply_x_axis_offset(p, x_range_name, frame_min, frame_max)

    rect_y = 0.0
    rect_height = 0.3
    rect_y_step = -1.5
    callback_idx = 0

    for callback_group in callback_groups:
        for callback in callback_group.callbacks:
            callback_idx += 1
            rect_source = get_callback_rect_list(callback, clipper, rect_y,
                                                 rect_height, converter)
            bar_source = get_callback_bar(callback, rect_y,
                                          frame_max, frame_min)
            color = color_selector.get_color(
                callback.node_name,
                callback_group.callback_group_name,
                callback.callback_name)
            plot1 = p.rect(
                'x',
                'y',
                'width',
                'height',
                source=rect_source,
                color=color,
                alpha=1.0,
                legend_label=f'callback{callback_idx}',
                # Since setting callback name to legend will narrow the graph,
                # sequential numbering is used here.
                hover_fill_color=color,
                hover_alpha=1.0,
                x_range_name=x_range_name
            )

            plot2 = p.rect(
                'x',
                'y',
                'width',
                'height',
                source=bar_source,
                fill_color=color,
                legend_label=f'callback{callback_idx}',
                hover_fill_color=color,
                hover_alpha=0.1,
                fill_alpha=0.1,
                level='underlay',
                x_range_name=x_range_name
            )

            Hover1 = HoverTool(
                renderers=[plot1],
                tooltips="""
                <div style="width:400px; word-wrap: break-word;">
                <br>
                callback_start = @x_min [ns] <br>
                callback_end = @x_max [ns] <br>
                latency = @latency [ms] <br>
                """,
                toggleable=False,
                attachment='above'
            )

            Hover2 = HoverTool(
                renderers=[plot2],
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
                toggleable=False,
                point_policy='follow_mouse',
                attachment='below'
            )
            p.add_tools(Hover1)
            p.add_tools(Hover2)

            if isinstance(callback, TimerCallback):
                y_start = rect_source.data['y'][1]+0.9
                y_end = rect_source.data['y'][1]+rect_height
                timer = callback.timer
                assert timer is not None
                df = timer.to_dataframe()
                for item in df.itertuples():
                    timer_stamp = item._1
                    callback_start = item._2
                    # callback_end = item._3
                    res = callback_start-timer_stamp
                    # The callback is considered delayed
                    # if this value is exceeded.
                    delayed_th = 500000
                    if not pd.isna(res):
                        if res > delayed_th:
                            p.add_layout(
                                Arrow(end=NormalHead(fill_color='red',
                                                     line_width=1,
                                                     size=10),
                                      x_start=(timer_stamp-frame_min)*1.0e-9,
                                      y_start=y_start,
                                      x_end=(timer_stamp-frame_min)*1.0e-9,
                                      y_end=y_end
                                      )
                            )
                        else:
                            p.add_layout(
                                Arrow(end=NormalHead(fill_color='white',
                                                     line_width=1,
                                                     size=10),
                                      x_start=(timer_stamp-frame_min)*1.0e-9,
                                      y_start=y_start,
                                      x_end=(timer_stamp-frame_min)*1.0e-9,
                                      y_end=y_end
                                      )
                            )
            rect_y += rect_y_step

    p.ygrid.grid_line_alpha = 0
    p.yaxis.visible = False
    p.legend.location = 'bottom_left'
    p.legend.click_policy = 'hide'
    p.add_layout(p.legend[0], 'right')

    if export_path is None:
        show(p)
    else:
        save(p, export_path,
             title='callback execution timing-chart', resources=CDN)

    return p


def get_callback_rect_list(
    callback: CallbackBase,
    clip: Clip,
    y,
    height,
    converter: Optional[ClockConverter]
) -> ColumnDataSource:
    """
    Get the DataSource of callback which in the target.

    Parameters
    ----------
    callback: CallbackBase
        callback
    clip: Clip
        clip
    y : int
        The start point of graph in y axis
    height : int
        The height of short rectangles
    converter : Optional[ClockConverter]
        converter

    Returns
    -------
    ColumnDataSource
        The DataSource of callback which in the target

    """
    y_min = y - height
    y_max = y + height

    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        'x_min': [],
        'x_max': [],
        'width': [],
        'latency': [],
        'height': []
    })

    df = callback.to_dataframe(shaper=clip)
    for item in df.itertuples():
        callback_start = item._1
        callback_end = item._2
        if converter:
            callback_start = converter.convert(callback_start)
            callback_end = converter.convert(callback_end)

        rect = RectValues(callback_start, callback_end, y_min, y_max)
        new_data = {
            'x': [rect.x],
            'y': [rect.y],
            'x_min': [callback_start],
            'x_max': [callback_end],
            'width': [rect.width],
            'latency': [(callback_end-callback_start)*1.0e-6],
            'height': [rect.height]
        }
        rect_source.stream(new_data)
    return rect_source


def get_callback_bar(
    callback: CallbackBase,
    y,
    frame_max,
    frame_min
) -> ColumnDataSource:
    """
    Get the DataSource of long rectangular.

    Parameters
    ----------
    callback : CallbackBase
        callback
    y : double
        y axis
    frame_max : int
        The end point of callback in x axis
    frame_min : int
        The start point of callback in x axis

    Returns
    -------
    ColumnDataSource
        The DataSource of long rectangular

    """
    y_min = y - 0.6
    y_max = y + 0.5

    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        'width': [],
        'height': [],
        'node_name': [],
        'callback_name': [],
        'callback_type': [],
        'callback_param': [],
        'symbol': []

    })

    callback_param = get_callback_param_desc(callback)
    bar_start = frame_min - 10000000000
    bar_end = frame_max + 10000000000
    rect = RectValues(bar_start, bar_end, y_min, y_max)
    rect_source = ColumnDataSource(data={
            'x': [rect.x],
            'y': [rect.y],
            'width': [rect.width],
            'height': [rect.height],
            'node_name': [callback.node_name],
            'callback_name': [callback.callback_name],
            'symbol': [callback.symbol],
            'callback_param': [callback_param],
            'callback_type': [f'{callback.callback_type}']
        })

    return rect_source


class ColorSelector:
    """
    Class that provides API for color selection.

    This class provides the API to get the color for each callback
    in the different rules.
    """

    @staticmethod
    def create_instance(coloring_rule: str):
        if coloring_rule == 'callback':
            return ColorSelectorCallback()

        if coloring_rule == 'callback_group':
            return ColorSelectorCbg()

        if coloring_rule == 'node':
            return ColorSelectorNode()

    def __init__(self) -> None:
        self._palette: Sequence[Color] = \
            [self._from_rgb(*rgb) for rgb in cc.glasbey_bw_minc_20]
        self._color_map: Dict[str, Color] = {}

    def get_color(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> Color:
        color_hash = self._get_color_hash(node_name, cbg_name, callback_name)

        if color_hash not in self._color_map:
            color_index = len(self._color_map) % len(self._palette)
            self._color_map[color_hash] = self._palette[color_index]

        return self._color_map[color_hash]

    @abstractmethod
    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> Color:
        return

    @staticmethod
    def _from_rgb(r: float, g: float, b: float) -> Color:
        r_ = int(r*255)
        g_ = int(g*255)
        b_ = int(b*255)
        return RGB(r_, g_, b_)


class ColorSelectorCallback(ColorSelector):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> Color:
        return callback_name


class ColorSelectorCbg(ColorSelector):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> Color:
        return cbg_name


class ColorSelectorNode(ColorSelector):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> Color:
        return node_name
