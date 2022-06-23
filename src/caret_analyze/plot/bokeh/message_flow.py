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

from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.palettes import Bokeh8
from bokeh.plotting import ColumnDataSource, figure
from bokeh.resources import CDN
import numpy as np
import pandas as pd

from .util import apply_x_axis_offset, get_callback_param_desc, RectValues
from ...common import ClockConverter
from ...exceptions import InvalidArgumentError
from ...record.data_frame_shaper import Clip, Strip
from ...runtime.path import Path


def message_flow(
    path: Path,
    export_path: Optional[str] = None,
    granularity: Optional[str] = None,
    treat_drop_as_delay=False,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
    use_sim_time: bool = False
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

    <br>
    @desc
    </div>
    """

    fig = figure(
        x_axis_label='Time [s]',
        y_axis_label='',
        title=f'Message flow of {path.path_name}',
        plot_width=1000,
        plot_height=400,
        active_scroll='wheel_zoom',
        tooltips=TOOLTIPS,
    )
    fig.add_tools(CrosshairTool(line_alpha=0.4))

    color_palette = ColorPalette(Bokeh8)

    df = path.to_dataframe(treat_drop_as_delay=treat_drop_as_delay)

    converter: Optional[ClockConverter] = None
    if use_sim_time:
        assert path.callbacks is not None and len(path.callbacks) > 0
        cb = path.callbacks[0]
        converter = cb._provider.get_sim_time_converter()  # TODO(hsgwa): refactor

    strip = Strip(lstrip_s, rstrip_s)
    clip = strip.to_clip(df)
    df = clip.execute(df)

    frame_min: float = clip.min_ns
    frame_max: float = clip.max_ns

    if converter:
        frame_min = converter.convert(frame_min)
        frame_max = converter.convert(frame_max)

    x_range_name = 'x_plot_axis'
    apply_x_axis_offset(fig, x_range_name, frame_min, frame_max)

    formatter = FormatterFactory.create(path, granularity)
    formatter.remove_columns(df)
    formatter.rename_columns(df, path)

    yaxis_property = YAxisProperty(df)
    yaxis_values = YAxisValues(df)
    fig.yaxis.ticker = yaxis_property.values
    fig.yaxis.major_label_overrides = yaxis_property.labels_dict

    rect_source = get_callback_rects(path, yaxis_values, granularity, clip, converter)
    fig.rect(
        'x',
        'y',
        'width',
        'height',
        source=rect_source,
        color='black',
        alpha=0.15,
        hover_fill_color='black',
        hover_alpha=0.4,
        x_range_name=x_range_name
    )

    line_sources = get_flow_lines(df, converter)
    for i, line_source in enumerate(line_sources):
        fig.line(
            x='x',
            y='y',
            line_width=1.5,
            line_color=color_palette.get_index_color(i),
            line_alpha=1,
            source=line_source,
            x_range_name=x_range_name
        )

    if export_path is None:
        show(fig)
    else:
        save(fig, export_path, title='time vs tracepoint', resources=CDN)


def get_callback_rects(
    path: Path,
    y_axi_values: YAxisValues,
    granularity: str,
    clip: Optional[Clip],
    converter: Optional[ClockConverter]
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

    x = []
    y = []
    x_min = []
    x_max = []
    desc = []
    width = []
    latency = []
    height = []

    for callback in path.callbacks:
        if granularity == 'raw':
            search_name = callback.callback_name
        elif granularity == 'node':
            search_name = callback.node_name

        y_maxs = np.array(y_axi_values.get_start_indexes(search_name))
        y_mins = y_maxs - 1

        callback_desc = get_callback_param_desc(callback)

        for y_min, y_max in zip(y_mins, y_maxs):
            df = callback.to_dataframe(shaper=clip)
            for _, row in df.iterrows():
                callback_start = row.to_list()[0]
                callback_end = row.to_list()[-1]
                if converter:
                    callback_start = converter.convert(callback_start)
                    callback_end = converter.convert(callback_end)
                rect = RectValues(callback_start, callback_end, y_min, y_max)
                x.append(rect.x)
                y.append(rect.y)
                x_min.append(callback_start)
                x_max.append(callback_end)
                width.append(rect.width)
                latency.append((callback_end-callback_start)*1.0e-6)
                height.append(rect.height)

                desc_str = f'callback_type: {callback.callback_type}' + \
                    f', {callback_desc}, symbol: {callback.symbol}'
                desc.append(desc_str)

    rect_source = ColumnDataSource(data={
        'x': x,
        'y': y,
        'x_min': x_min,
        'x_max': x_max,
        'desc': desc,
        'width': width,
        'latency': latency,
        'height': height
    })
    return rect_source


def get_flow_lines(df: pd.DataFrame, converter: Optional[ClockConverter]) -> ColumnDataSource:
    tick_labels = YAxisProperty(df)
    line_sources = []

    for i, row in df.iterrows():
        row_values = row.dropna().values
        if converter:
            row_values = [converter.convert(_) for _ in row_values]
        x_min = min(row_values)
        x_max = max(row_values)
        width = x_max - x_min

        line_source = ColumnDataSource({
            'x': row_values,
            'y': tick_labels.values[:len(row_values)],
            'x_min': [x_min]*len(row_values),
            'x_max': [x_max]*len(row_values),
            'width': [width]*len(row_values),
            'height': [0]*len(row_values),
            'latency': [width*1.0e-6]*len(row_values),
            'desc': [f'index: {i}']*len(row_values),
        })

        line_sources.append(line_source)

    return line_sources


class DataFrameColumnNameParsed:

    def __init__(self, path: Path, column_name) -> None:
        split_text = column_name.split('/')
        tracepoint_name = split_text[-2]
        self.tracepoint_name = tracepoint_name

        self.loop_index = split_text[-1]
        self.name = '/'.join(split_text[:-2])
        self.node_name = None
        self.topic_name = None
        if self._has_node_name(path.node_names, column_name):
            self.node_name = '/'.join(split_text[:-3])
        if self._has_topic_name(path.topic_names, column_name):
            self.topic_name = '/'.join(split_text[:-3])

    @staticmethod
    def _has_topic_name(topic_names, column_name):
        for topic_name in topic_names:
            if topic_name in column_name:
                return True
        return False

    @staticmethod
    def _has_node_name(topic_names, column_name):
        for topic_name in topic_names:
            if topic_name in column_name:
                return True
        return False


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
    def create(self, path: Path, granularity: str) -> DataFrameFormatter:
        if granularity == 'raw':
            return RawLevelFormatter(path)
        elif granularity == 'node':
            return NodeLevelFormatter(path)

        raise NotImplementedError()


class RawLevelFormatter(DataFrameFormatter):

    def __init__(self, path: Path) -> None:
        pass

    def remove_columns(self, df: pd.DataFrame) -> None:
        df
        return None

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            if '_timestamp' in column_name:
                idx = column_name.rfind('_timestamp')
                renames[column_name] = column_name[:idx]

        df.rename(columns=renames, inplace=True)


class NodeLevelFormatter(DataFrameFormatter):

    def __init__(self, path: Path) -> None:
        self._path = path

    def remove_columns(self, df: pd.DataFrame) -> None:
        raw_level_formatter = RawLevelFormatter(self._path)
        raw_level_formatter.remove_columns(df)

        drop_columns = []

        # remove callbacks in the same node
        for column_name_, column_name in zip(df.columns[:-1], df.columns[1:]):
            parsed_before = DataFrameColumnNameParsed(self._path, column_name_)
            parsed = DataFrameColumnNameParsed(self._path, column_name)

            if parsed_before.node_name is not None and parsed.node_name is not None and \
                    parsed_before.node_name == parsed.node_name:
                drop_columns.append(column_name)

            if parsed_before.topic_name is not None and parsed.topic_name is not None and \
                    parsed_before.topic_name == parsed.topic_name:
                drop_columns.append(column_name)

        df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            if '_timestamp' in column_name:
                idx = column_name.rfind('_timestamp')
                renames[column_name] = column_name[:idx]

        df.rename(columns=renames, inplace=True)
