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

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.palettes import Bokeh8
from bokeh.plotting import ColumnDataSource, figure
from bokeh.resources import CDN

from ...record.data_frame_shaper import Clip, Strip
from ...runtime.path import Path
from ...exceptions import InvalidArgumentError


def message_flow(
    path: Path,
    export_path: Optional[str] = None,
    granularity: Optional[str] = None,
    treat_drop_as_delay=False,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
) -> None:
    granularity = granularity or 'raw'
    if granularity not in ['raw', 'node']:
        raise InvalidArgumentError('granularity must be [ raw / node ]')

    TOOLTIPS = """
    <div style="width:400px; word-wrap: break-word;">
    t = @x [ns] <br>
    t_start = @x_min [ns] <br>
    t_end = @x_max [ns] <br>
    latency = @latency [ms] <br>
    @desc
    </div>
    """

    fig = figure(
        plot_width=1000,
        plot_height=400,
        active_scroll='wheel_zoom',
        tooltips=TOOLTIPS,
    )
    fig.add_tools(CrosshairTool(line_alpha=0.4))

    color_palette = ColorPalette(Bokeh8)

    df = path.to_dataframe(treat_drop_as_delay=treat_drop_as_delay)

    clip = None
    if lstrip_s > 0 or rstrip_s > 0:
        strip = Strip(lstrip_s, rstrip_s)
        clip = strip.to_clip(df)
        df = clip.execute(df)

    formatter = FormatterFactory.create(granularity)
    formatter.remove_columns(df)
    formatter.rename_columns(df, path)

    yaxis_property = YAxisProperty(df)
    yaxis_values = YAxisValues(df)
    fig.yaxis.ticker = yaxis_property.values
    fig.yaxis.major_label_overrides = yaxis_property.labels_dict

    rect_source = get_callback_rects(path, yaxis_values, granularity, clip)
    fig.rect('x', 'y', 'width', 'height', source=rect_source, color='black', alpha=0.15)

    line_sources = get_flow_lines(df)
    for i, line_source in enumerate(line_sources):
        fig.line(
            x='x',
            y='y',
            line_width=1.5,
            line_color=color_palette.get_index_color(i),
            line_alpha=1,
            source=line_source,
        )

    if export_path is None:
        show(fig)
    else:
        save(fig, export_path, title='time vs tracepoint', resources=CDN)


def get_callback_rects(
    path: Path,
    y_axi_values: YAxisValues,
    granularity: str,
    clip: Optional[Clip]
) -> ColumnDataSource:
    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        'x_min': [],
        'x_max': [],
        'desc': [],
        'width': [],
        'latency': [],
        'height': []
    })

    if path.callbacks is None:
        return rect_source

    for callback in path.callbacks:
        if granularity == 'raw':
            search_name = callback.callback_name
        elif granularity == 'callback':
            search_name = callback.callback_name
        elif granularity == 'node':
            search_name = callback.node_name

        y_maxs = np.array(y_axi_values.get_start_indexes(search_name))
        y_mins = y_maxs - 1

        for y_min, y_max in zip(y_mins, y_maxs):
            for _, row in callback.to_dataframe(shaper=clip).iterrows():
                callback_start = row.to_list()[0]
                callback_end = row.to_list()[-1]
                rect = RectValues(callback_start, callback_end, y_min, y_max)
                new_data = {
                    'x': [rect.x],
                    'y': [rect.y],
                    'x_min': [callback_start],
                    'x_max': [callback_end],
                    'width': [rect.width],
                    'latency': [(callback_end-callback_start)*1.0e-6],
                    'height': [rect.height],
                    'desc': ['symbol: ' + callback.symbol],
                }
                rect_source.stream(new_data)

    return rect_source


def get_flow_lines(df: pd.DataFrame) -> ColumnDataSource:
    tick_labels = YAxisProperty(df)
    line_sources = []

    for _, row in df.iterrows():
        x_min = min(row.values)
        x_max = max(row.values)
        width = x_max - x_min

        line_source = ColumnDataSource({
            'x': row.values,
            'y': tick_labels.values,
            'x_min': [x_min]*len(row.values),
            'x_max': [x_max]*len(row.values),
            'width': [width]*len(row.values),
            'height': [0]*len(row.values),
            'latency': [width*1.0e-6]*len(row.values),
            'desc': ['']*len(row.values),
        })

        line_sources.append(line_source)

    return line_sources


class DataFrameColumnNameParsed:

    def __init__(self, column_name) -> None:
        split_text = column_name.split('/')
        tracepoint_name = split_text[-2]
        self.tracepoint_name = tracepoint_name
        self.name = '/'.join(split_text[:-2])
        self.node_name = '/'.join(split_text[:-3])


class ColorPalette:

    def __init__(self, color_palette):
        self._color_palette = color_palette
        self._palette_size = len(color_palette)

    def get_index_color(self, i: int):
        return self._color_palette[i % self._palette_size]


class YAxisProperty:

    def __init__(self, df) -> None:
        self._tick_labels: Dict[int, str] = {}
        y_axis_step = -1

        y_value = 0
        for column_name in df.columns:
            self._tick_labels[y_value] = column_name
            y_value += y_axis_step

        return None

    @property
    def values(self):
        return list(self._tick_labels.keys())

    @property
    def labels(self):
        return list(self._tick_labels.values())

    @property
    def labels_dict(self) -> Dict[int, str]:
        return self._tick_labels


class YAxisValues:

    def __init__(self, column_names) -> None:
        self._column_names = column_names

    def _search_values(self, search_name) -> np.array:
        indexes = np.array([], dtype=int)
        for i, column_name in enumerate(self._column_names):
            if 'callback_end' in column_name:
                continue
            if search_name in column_name:
                indexes = np.append(indexes, i)
        return indexes

    def get_start_indexes(self, search_name) -> List[int]:
        indexes = self._search_values(search_name)
        return list((indexes) * -1)

    def get_end_values(self, search_name) -> List[int]:
        indexes = self._search_values(search_name)
        return list((indexes + 1) * -0.5)


class DataFrameFormatter(metaclass=ABCMeta):

    @abstractmethod
    def remove_columns(self, df: pd.DataFrame) -> None:
        pass

    @abstractmethod
    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        pass


class FormatterFactory():

    @classmethod
    def create(self, granularity: str) -> DataFrameFormatter:
        if granularity == 'raw':
            return RawLevelFormatter()
        elif granularity == 'node':
            return NodeLevelFormatter()

        raise NotImplementedError()


class RawLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        df
        return None

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        topic_name = ''
        for column_name in df.columns:
            if '_timestamp' in column_name:
                idx = column_name.rfind('_timestamp')
                renames[column_name] = column_name[:idx]

        df.rename(columns=renames, inplace=True)


class NodeLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        raw_level_formatter = RawLevelFormatter()
        raw_level_formatter.remove_columns(df)

        drop_columns = []

        # remove callbacks in the same node
        for column_name_, column_name in zip(df.columns[:-1], df.columns[1:]):
            parsed_before = DataFrameColumnNameParsed(column_name_)
            parsed = DataFrameColumnNameParsed(column_name)

            is_before_same_node = parsed.node_name == parsed_before.node_name

            if is_before_same_node:
                drop_columns.append(column_name)
        df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            if '_timestamp' in column_name:
                idx = column_name.rfind('_timestamp')
                renames[column_name] = column_name[:idx]

        df.rename(columns=renames, inplace=True)


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
