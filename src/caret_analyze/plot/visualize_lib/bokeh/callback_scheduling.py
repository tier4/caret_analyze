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

from collections.abc import Sequence
from typing import Any

from bokeh.models import HoverTool
from bokeh.models.annotations import Arrow, NormalHead
from bokeh.plotting import ColumnDataSource, figure as Figure

import pandas as pd

from .util import (apply_x_axis_offset, ColorSelectorFactory, get_callback_param_desc,
                   HoverKeysFactory, HoverSource, init_figure, LegendManager, RectValues)
from ....common import ClockConverter, Util
from ....record import Clip, Range
from ....runtime import CallbackBase, CallbackGroup, TimerCallback


class BokehCallbackSched:

    def __init__(
        self,
        callback_groups: Sequence[CallbackGroup],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        coloring_rule: str,
        lstrip_s: float,
        rstrip_s: float
    ) -> None:
        self._callback_groups = callback_groups
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._full_legends = full_legends
        self._coloring_rule = coloring_rule
        self._lstrip_s = lstrip_s
        self._rstrip_s = rstrip_s

    def create_figure(self) -> Figure:
        """
        Create Bokeh callback sched figure.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of Bokeh callback sched.

        """
        # Initialize figure
        title = ('Callback Scheduling in '
                 f"[{'/'.join([cbg.callback_group_name for cbg in self._callback_groups])}].")
        fig = init_figure(title, self._ywheel_zoom, self._xaxis_type)

        # Apply xaxis offset
        callbacks: list[CallbackBase] = Util.flatten(
            cbg.callbacks for cbg in self._callback_groups if len(cbg.callbacks) > 0)
        records_range = Range([cb.to_records() for cb in callbacks])
        range_min, range_max = records_range.get_range()
        clip_min = int(range_min + self._lstrip_s*1.0e9)
        clip_max = int(range_max - self._rstrip_s*1.0e9)
        clip = Clip(clip_min, clip_max)
        if self._xaxis_type == 'sim_time':
            # TODO(hsgwa): refactor
            converter = callbacks[0]._provider.get_sim_time_converter(range_min, range_max)
            frame_min = converter.convert(clip.min_ns)
            frame_max = converter.convert(clip.max_ns)
        else:
            converter = None
            frame_min = clip.min_ns
            frame_max = clip.max_ns
        apply_x_axis_offset(fig, frame_min, frame_max)

        # Draw callback scheduling
        color_selector = ColorSelectorFactory.create_instance(self._coloring_rule)
        legend_manager = LegendManager()
        rect_source_gen = CallbackSchedRectSource(legend_manager, callbacks[0],
                                                  clip, frame_min, converter)
        bar_source_gen = CallbackSchedBarSource(legend_manager, callbacks[0], frame_min, frame_max)

        for cbg in self._callback_groups:
            for callback in cbg.callbacks:
                color = color_selector.get_color(
                    callback.node_name,
                    cbg.callback_group_name,
                    callback.callback_name
                )
                rect_source = rect_source_gen.generate(callback)
                rect = fig.rect(
                    'x', 'y', 'width', 'height',
                    source=rect_source,
                    color=color,
                    alpha=1.0,
                    hover_fill_color=color,
                    hover_alpha=1.0
                )
                legend_manager.add_legend(callback, rect)
                fig.add_tools(rect_source_gen.create_hover(
                    {'attachment': 'above', 'renderers': [rect]}
                ))
                bar = fig.rect(
                    'x', 'y', 'width', 'height',
                    source=bar_source_gen.generate(callback, rect_source_gen.rect_y_base),
                    fill_color=color,
                    hover_fill_color=color,
                    hover_alpha=0.1,
                    fill_alpha=0.1,
                    level='underlay'
                )
                fig.add_tools(bar_source_gen.create_hover(
                    {'attachment': 'below', 'renderers': [bar]}
                ))

                if isinstance(callback, TimerCallback) and len(rect_source.data['y']) > 1:
                    # If the response time exceeds this value, it is considered a delay.
                    delay_threshold = 500000
                    y_start = rect_source.data['y'][1]+0.9
                    y_end = rect_source.data['y'][1]+rect_source_gen.RECT_HEIGHT
                    timer_df = callback.timer.to_dataframe()  # type: ignore
                    for row in timer_df.itertuples():
                        timer_stamp = row[1]
                        callback_start = row[2]
                        response_time = callback_start - timer_stamp
                        if pd.isna(response_time):
                            continue
                        fig.add_layout(Arrow(
                            end=NormalHead(
                                fill_color='red' if response_time > delay_threshold else 'white',
                                line_width=1, size=10
                            ),
                            x_start=(timer_stamp - frame_min) * 1.0e-9, y_start=y_start,
                            x_end=(timer_stamp - frame_min) * 1.0e-9, y_end=y_end
                        ))

                rect_source_gen.update_rect_y_base()

        # Draw legends
        num_legend_threshold = 20
        """
        In Autoware, the number of callbacks in a node is less than 20.
        Here, num_legend_threshold is set to 20 as the maximum value.
        """
        legends = legend_manager.create_legends(num_legend_threshold, self._full_legends)
        for legend in legends:
            fig.add_layout(legend, 'right')
        fig.legend.click_policy = 'hide'

        fig.ygrid.grid_line_alpha = 0
        fig.yaxis.visible = False

        return fig


class CallbackSchedRectSource:
    """Class to generate callback scheduling rect sources."""

    RECT_HEIGHT = 0.3
    _RECT_Y_STEP = -1.5

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: CallbackBase,
        clip: Clip,
        frame_min: float,
        converter: ClockConverter | None = None
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance(
            'callback_scheduling_rect', target_object)
        self._hover_source = HoverSource(self._hover_keys)
        self._legend_manager = legend_manager
        self._clip = clip
        self._frame_min = frame_min
        self._converter = converter
        self._rect_y_base = 0.0

    @property
    def rect_y_base(self) -> float:
        """
        Get rect y_base.

        Returns
        -------
        float
            rect y_base.

        """
        return self._rect_y_base

    def create_hover(self, options: dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool
            Created hover.

        """
        return self._hover_keys.create_hover(options)

    def generate(self, callback: CallbackBase) -> ColumnDataSource:
        """
        Generate callback scheduling rect source.

        Parameters
        ----------
        callback : CallbackBase
            target callback.

        Returns
        -------
        ColumnDataSource
            Generated callback scheduling rect source

        """
        rect_source = ColumnDataSource(data={
            k: [] for k in (['x', 'y', 'width', 'height'] + self._hover_keys.to_list())
        })
        latency_table = callback.to_dataframe(shaper=self._clip)
        for row in latency_table.itertuples():
            callback_start = self._converter.convert(row[1]) if self._converter else row[1]
            callback_end = self._converter.convert(row[-1]) if self._converter else row[-1]
            rect = RectValues(
                (callback_start - self._frame_min) * 10**-9,
                (callback_end - self._frame_min) * 10**-9,
                (self._rect_y_base-self.RECT_HEIGHT),
                (self._rect_y_base+self.RECT_HEIGHT)
            )
            rect_source.stream({
                **{
                    'x': [rect.x],
                    'y': [rect.y],
                    'width': [rect.width],
                    'height': [rect.height],
                },
                **self._hover_source.generate(callback, {
                    'callback_start': f'callback_start = {callback_start} [ns]',
                    'callback_end': f'callback_end = {callback_end} [ns]',
                    'latency': f'latency = {(callback_end - callback_start) * 1.0e-6} [ms]',
                    'legend_label': f'legend_label = {self._legend_manager.get_label(callback)}'
                })  # type: ignore
            })

        return rect_source

    def update_rect_y_base(self) -> None:
        """Update rect_y_base to the next step."""
        self._rect_y_base += self._RECT_Y_STEP


class CallbackSchedBarSource:
    """Class to generate callback scheduling bar sources."""

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: CallbackBase,
        frame_min: float,
        frame_max: float
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance(
            'callback_scheduling_bar', target_object)
        self._hover_source = HoverSource(self._hover_keys)
        self._legend_manager = legend_manager
        self._frame_min = frame_min
        self._frame_max = frame_max

    def create_hover(self, options: dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool
            Created hover.

        """
        return self._hover_keys.create_hover(options)

    def generate(self, callback: CallbackBase, rect_y_base: float) -> ColumnDataSource:
        """
        Generate callback scheduling bar source.

        Parameters
        ----------
        callback : CallbackBase
            target callback.
        rect_y_base : float
            The y-base of rect.

        Returns
        -------
        ColumnDataSource
            Generated callback scheduling bar source

        """
        rect = RectValues(
            0,
            (self._frame_max - self._frame_min) * 10**-9,
            rect_y_base - 0.5,
            rect_y_base + 0.5
        )
        hover_source = self._hover_source.generate(
            callback,
            {'legend_label': f'legend_label = {self._legend_manager.get_label(callback)}',
             'callback_param': get_callback_param_desc(callback)}
        )
        bar_source = ColumnDataSource(data={
            **{'x': [rect.x], 'y': [rect.y],
               'width': [rect.width], 'height': [rect.height]},
            **hover_source  # type: ignore
        })

        return bar_source
