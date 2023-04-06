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

from typing import Optional

from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource

from .util import HoverKeysFactory, HoverSource, LegendManager, RectValues

from ....common import ClockConverter
from ....record import Clip
from ....runtime import CallbackBase


class CallbackSchedRectSource:
    """Class to generate callback scheduling rect sources."""

    RECT_HEIGHT = 0.3
    _RECT_Y_STEP = -1.5

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: CallbackBase,
        clip: Clip,
        converter: Optional[ClockConverter] = None
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance(
            'callback_scheduling_rect', target_object)
        self._hover_source = HoverSource(legend_manager, self._hover_keys)
        self._clip = clip
        self._converter = converter
        self._rect_y_base = 0.0

    @property
    def rect_y_base(self) -> float:
        return self._rect_y_base

    def create_hover(self, options: dict = {}) -> HoverTool:
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

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

        """
        rect_source = ColumnDataSource(data={
            k: [] for k in (['x', 'y', 'width', 'height'] + self._hover_keys.to_list())
        })
        latency_table = callback.to_dataframe(shaper=self._clip)
        for row in latency_table.itertuples():
            callback_start = self._converter.convert(row[1]) if self._converter else row[1]
            callback_end = self._converter.convert(row[-1]) if self._converter else row[-1]
            rect = RectValues(
                callback_start, callback_end,
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
                    'latency': f'latency = {(callback_end - callback_start) * 1.0e-6} [ms]'
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
        self._hover_source = HoverSource(legend_manager, self._hover_keys)
        self._frame_min = frame_min
        self._frame_max = frame_max

    def create_hover(self, options: dict = {}) -> HoverTool:
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

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

        """
        rect = RectValues(
            self._frame_min, self._frame_max,
            rect_y_base - 0.5,
            rect_y_base + 0.5
        )
        hover_source = self._hover_source.generate(callback)
        bar_source = ColumnDataSource(data={
            **{'x': [rect.x], 'y': [rect.y],
               'width': [rect.width], 'height': [rect.height]},
            **hover_source  # type: ignore
        })

        return bar_source
