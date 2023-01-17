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

from collections import defaultdict
from logging import getLogger
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from bokeh.models import GlyphRenderer, HoverTool, Legend
from bokeh.plotting import ColumnDataSource

import numpy as np

from ....common import ClockConverter
from ....exceptions import InvalidArgumentError
from ....record import Clip, RecordsInterface
from ....runtime import (CallbackBase, Communication, Publisher, Subscription,
                         SubscriptionCallback, TimerCallback)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class LegendKeys:
    _SUPPORTED_GRAPH_TYPE = ['callback_scheduling_bar', 'callback_scheduling_rect', 'timeseries']

    def __init__(self, graph_type: str, target_object: TimeSeriesTypes) -> None:
        self._validate(graph_type, target_object)
        self._graph_type = graph_type
        self._target_object = target_object

    def _validate(self, graph_type: str, target_object: Any) -> None:
        if graph_type not in self._SUPPORTED_GRAPH_TYPE:
            raise InvalidArgumentError(
                f"'graph_type' must be [{'/'.join(self._SUPPORTED_GRAPH_TYPE)}]."
            )

        if graph_type == 'callback_scheduling' and not isinstance(target_object, CallbackBase):
            raise InvalidArgumentError(
                "'target_object' must be CallbackBase in callback scheduling graph."
            )

        if (graph_type == 'timeseries' and not isinstance(
                target_object, (CallbackBase, Communication, Publisher, Subscription))):
            raise InvalidArgumentError(
                "'target_object' must be [CallbackBase/Communication/Publisher/Subscription]"
                'in timeseries graph.'
            )

    def to_list(self) -> List[str]:
        if self._graph_type == 'callback_scheduling_bar':
            legend_keys = ['legend_label', 'node_name', 'callback_name',
                           'callback_type', 'callback_param', 'symbol']

        if self._graph_type == 'callback_scheduling_rect':
            legend_keys = ['legend_label', 'callback_start', 'callback_end', 'latency']

        if self._graph_type == 'timeseries':
            if isinstance(self._target_object, CallbackBase):
                legend_keys = ['legend_label', 'node_name', 'callback_name', 'callback_type',
                               'callback_param', 'symbol']
            elif isinstance(self._target_object, Communication):
                legend_keys = ['legend_label', 'topic_name',
                               'publish_node_name', 'subscribe_node_name']
            elif isinstance(self._target_object, (Publisher, Subscription)):
                legend_keys = ['legend_label', 'node_name', 'topic_name']

        return legend_keys


class HoverCreator:

    def __init__(self, legend_keys: LegendKeys) -> None:
        self._legend_keys = legend_keys

    def create(self, options: dict = {}) -> HoverTool:
        """
        Create HoverTool.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
        tips_str = '<div style="width:400px; word-wrap: break-word;">'
        for k in self._legend_keys.to_list():
            tips_str += f'@{k} <br>'
        tips_str += '</div>'

        return HoverTool(
            tooltips=tips_str, point_policy='follow_mouse', toggleable=False, **options
        )


class LegendSource:

    def __init__(self, legend_manager: LegendManager, legend_keys: LegendKeys) -> None:
        self._legend_manager = legend_manager
        self._legend_keys = legend_keys

    def generate(self, target_object: Any) -> Dict[str, str]:
        legend_values: Dict[str, Any] = {}
        for k in self._legend_keys.to_list():
            if hasattr(target_object, k):
                legend_values[k] = [f'{k} = {getattr(target_object, k)}']
            else:
                legend_values[k] = [self.get_description(target_object, k)]

        return legend_values

    def get_description(self, target_object: Any, key: str) -> str:
        if key == 'callback_param':
            if isinstance(target_object, TimerCallback):
                description = f'period_ns = {target_object.period_ns}'
            elif isinstance(target_object, SubscriptionCallback):
                description = f'subscribe_topic_name = {target_object.subscribe_topic_name}'

        elif key == 'legend_label':
            label = self._legend_manager.get_label(target_object)
            description = f'legend_label = {label}'

        else:
            raise NotImplementedError()

        return description


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


class CallbackSchedRectSource:
    """Class to generate callback scheduling rect sources."""

    RECT_HEIGHT = 0.3
    _RECT_Y_STEP = -1.5

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: TimeSeriesTypes,
        clip: Clip,
        converter: Optional[ClockConverter] = None
    ) -> None:
        self._legend_keys = LegendKeys('callback_scheduling_rect', target_object)
        self._hover = HoverCreator(self._legend_keys)
        self._legend_source = LegendSource(legend_manager, self._legend_keys)
        self._clip = clip
        self._converter = converter
        self._rect_y_base = 0.0

    @property
    def rect_y_base(self) -> float:
        return self._rect_y_base

    def create_hover(self, options: dict = {}) -> HoverTool:
        return self._hover.create(options)

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
            k: [] for k in (['x', 'y', 'width', 'height'] + self._legend_keys.to_list())
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
                'legend_label': [self._legend_source.get_description(callback, 'legend_label')],
                'x': [rect.x],
                'y': [rect.y],
                'width': [rect.width],
                'height': [rect.height],
                'callback_start': [f'callback_start = {callback_start} [ns]'],
                'callback_end': [f'callback_end = {callback_end} [ns]'],
                'latency': [f'latency = {(callback_end - callback_start) * 1.0e-6} [ms]']
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
        target_object: TimeSeriesTypes,
        frame_min: float,
        frame_max: float
    ) -> None:
        self._legend_keys = LegendKeys('callback_scheduling_bar', target_object)
        self._hover = HoverCreator(self._legend_keys)
        self._legend_source = LegendSource(legend_manager, self._legend_keys)
        self._frame_min = frame_min
        self._frame_max = frame_max

    def create_hover(self, options: dict = {}) -> HoverTool:
        return self._hover.create(options)

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
        legend_source = self._legend_source.generate(callback)
        bar_source = ColumnDataSource(data={
            **{'x': [rect.x], 'y': [rect.y],
               'width': [rect.width], 'height': [rect.height]},
            **legend_source  # type: ignore
        })

        return bar_source


class LineSource:
    """Class to generate timeseries line sources."""

    def __init__(
        self,
        legend_manager: LegendManager,
        target_object: TimeSeriesTypes,
        frame_min,
        xaxis_type: str,
    ) -> None:
        self._legend_keys = LegendKeys('timeseries', target_object)
        self._hover = HoverCreator(self._legend_keys)
        self._legend_source = LegendSource(legend_manager, self._legend_keys)
        self._frame_min = frame_min
        self._xaxis_type = xaxis_type

    def create_hover(self, options: dict = {}) -> HoverTool:
        return self._hover.create(options)

    def generate(
        self,
        target_object: TimeSeriesTypes,
        timeseries_records: RecordsInterface,
    ) -> ColumnDataSource:
        """
        Generate a line source for timeseries figure.

        Parameters
        ----------
        target_object : TimeSeriesTypes
            TimeSeriesPlotTypes = Union[
                CallbackBase, Communication, Union[Publisher, Subscription]
            ]
        timeseries_records : RecordsInterface
            Records containing timeseries data.

        Returns
        -------
        bokeh.plotting.ColumnDataSource
            Line source for timeseries figure.

        """
        line_source = ColumnDataSource(data={
            k: [] for k in (['x', 'y'] + self._legend_keys.to_list())
        })
        legend_source = self._legend_source.generate(target_object)
        x_item, y_item = self._get_x_y(timeseries_records)
        for x, y in zip(x_item, y_item):
            line_source.stream({**{'x': [x], 'y': [y]}, **legend_source})  # type: ignore

        return line_source

    def _get_x_y(
        self,
        timeseries_records: RecordsInterface
    ) -> Tuple[List[Union[int, float]], List[Union[int, float]]]:
        def ensure_not_none(
            target_seq: Sequence[Optional[Union[int, float]]]
        ) -> List[Union[int, float]]:
            """
            Ensure the inputted list does not include None.

            Notes
            -----
            The timeseries_records is implemented not to include None,
            so if None is included, an AssertionError is output.

            """
            not_none_list = [_ for _ in target_seq if _ is not None]
            assert len(target_seq) == len(not_none_list)

            return not_none_list

        ts_column = timeseries_records.columns[0]
        value_column = timeseries_records.columns[1]
        timestamps = ensure_not_none(timeseries_records.get_column_series(ts_column))
        values = ensure_not_none(timeseries_records.get_column_series(value_column))
        if 'latency' in value_column.lower() or 'period' in value_column.lower():
            values = [v*10**(-6) for v in values]  # [ns] -> [ms]

        x_item: List[Union[int, float]]
        y_item: List[Union[int, float]] = values
        if self._xaxis_type == 'system_time':
            x_item = [(ts-self._frame_min)*10**(-9) for ts in timestamps]
        elif self._xaxis_type == 'index':
            x_item = list(range(0, len(values)))
        elif self._xaxis_type == 'sim_time':
            x_item = timestamps

        return x_item, y_item


class LegendManager:
    """Class that manages legend in Bokeh figure."""

    def __init__(self) -> None:
        self._legend_count_map: Dict[str, int] = defaultdict(int)
        self._legend_items: List[Tuple[str, List[GlyphRenderer]]] = []
        self._legend: Dict[Any, str] = {}

    def add_legend(
        self,
        target_object: Any,
        renderer: GlyphRenderer
    ) -> None:
        """
        Store a legend of the input object internally.

        Parameters
        ----------
        target_object : Any
            Instance of any class.
        renderer : bokeh.models.GlyphRenderer
            Instance of renderer.

        """
        label = self.get_label(target_object)
        self._legend_items.append((label, [renderer]))
        self._legend[target_object] = label

    def create_legends(
        self,
        max_legends: int = 20,
        full_legends: bool = False
    ) -> List[Legend]:
        """
        Create legends.

        Parameters
        ----------
        max_legends : int, optional
            Maximum number of legends to display, by default 20.
        full_legends : bool, optional
            Display all legends even if they exceed max_legends, by default False.

        Returns
        -------
        List[Legend]
            List of Legend instances separated by 10.

        """
        legends: List[Legend] = []
        for i in range(0, len(self._legend_items)+10, 10):
            if not full_legends and i >= max_legends:
                logger.warning(
                    f'The maximum number of legends drawn by default is {max_legends}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            legends.append(Legend(items=self._legend_items[i:i+10]))

        return legends

    def get_label(self, target_object: Any) -> str:
        if target_object in self._legend:
            return self._legend[target_object]

        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._legend_count_map[class_name]}'
        self._legend_count_map[class_name] += 1
        self._legend[target_object] = label

        return label
