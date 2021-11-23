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
from ...record import RecordInterface

from ...callback import SubscriptionCallback
from ...data_frame_shaper import Clip, Strip
from ...path import Path
from ...record.trace_points import TRACE_POINT
from ...util import Util


def message_flow(
    path: Path,
    export_path: Optional[str] = None,
    granularity: Optional[str] = None,
    treat_drop_as_delay=False,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
) -> None:
    granularity = granularity or 'raw'
    assert granularity in ['raw', 'callback', 'node']

    TOOLTIPS = """
    <div style="width:400px; word-wrap: break-word;">
    t = @x [ns] <br>
    t_start = @x_min [ns] <br>
    t_end = @x_max [ns] <br>
    latency = @width [ns] <br>
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

    line_sources = get_flow_lines(df, path)
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
        'height': []
    })

    for callback in path.callbacks:
        if granularity == 'raw':
            search_name = callback.unique_name
        elif granularity == 'callback':
            search_name = callback.unique_name
        elif granularity == 'node':
            search_name = callback.node_name

        y_maxs = y_axi_values.get_start_indexes(search_name)
        y_mins = y_axi_values.get_end_values(search_name)

        data = callback.to_records().data
        for y_min, y_max in zip(y_mins, y_maxs):
            for record in data:
                callback_start = record.get(
                    TRACE_POINT.CALLBACK_START_TIMESTAMP)
                callback_end = record.get(TRACE_POINT.CALLBACK_END_TIMESTAMP)
                rect = RectValues(callback_start, callback_end, y_min, y_max)
                new_data = {
                    'x': [rect.x],
                    'y': [rect.y],
                    'x_min': [str(callback_start)],
                    'x_max': [str(callback_end)],
                    'width': [rect.width],
                    'height': [rect.height],
                    'desc': ['symbol: ' + callback.symbol],
                }
                rect_source.stream(new_data)

    return rect_source


def get_path_end_time(record: RecordInterface, columns: List[str]) -> int:
    x_max = 0
    for column in columns:
        if column not in record.columns:
            continue
        if TRACE_POINT.CALLBACK_END_TIMESTAMP in column and column != columns[-1]:
            continue
        time = record.get(column)
        x_max = max(x_max, time)
    return x_max


def get_flow_lines(df: pd.DataFrame, path: Path) -> ColumnDataSource:
    tick_labels = YAxisProperty(df)
    line_sources = []

    records = path.to_records()

    for i, record in enumerate(records.data):
        x = np.array([record.get(c)
                     for c in path.column_names if c in record.columns])
        x_min = min(x)
        x_max = get_path_end_time(record, path.column_names)
        width = x_max - x_min

        line_source = ColumnDataSource({
            'x': x,
            'y': tick_labels.values[:len(x)],
            'x_min': [str(x_min)]*len(x),
            'x_max': [str(x_max)]*len(x),
            'width': [width]*len(x),
            'height': [0]*len(x),
            'desc': [f'record index: {i}']*len(x),
        })

        line_sources.append(line_source)

    return line_sources


class DataFrameColumnNameParser:

    def __init__(self, column_name) -> None:
        split_text = column_name.split('/')
        tracepoint_name = split_text[-2]
        self.tracepoint_name = tracepoint_name
        self.unique_name = '/'.join(split_text[:-2])
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
            if '[end]' in column_name or '[publish]' in column_name:
                continue
            if search_name in column_name:
                indexes = np.append(indexes, i)
        return indexes

    def get_start_indexes(self, search_name) -> List[int]:
        indexes = self._search_values(search_name)
        return list((indexes) * -1)

    def get_end_values(self, search_name) -> List[int]:
        indexes = self._search_values(search_name)
        return list((indexes + 1) * -1)


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
        elif granularity == 'callback':
            return CallbackLevelFormatter()
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
        for column_name in df.columns[::-1]:
            parser = DataFrameColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name
            unique_name = parser.unique_name

            callback = Util.find_one(path.callbacks, lambda x: x.unique_name == unique_name)
            assert callback is not None

            if 'subscription_callback' in unique_name and \
                    tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                assert isinstance(callback, SubscriptionCallback)
                topic_name = callback.topic_name

            new_column_name = ''
            if tracepoint_name in [
                TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
                TRACE_POINT.DDS_WRITE_TIMESTAMP,
                TRACE_POINT.RCL_PUBLISH_TIMESTAMP,
                TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
                TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            ]:
                new_column_name = f'{topic_name}/{tracepoint_name}'
                new_column_name = new_column_name[:-(len('_timestamp'))]
            elif tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                new_column_name = f'{unique_name} [start]'
            elif tracepoint_name == TRACE_POINT.CALLBACK_END_TIMESTAMP:
                new_column_name = f'{unique_name} [end]'

            renames[column_name] = new_column_name

        df.rename(columns=renames, inplace=True)


class CallbackLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        drop_columns = []
        for column_name_, column_name in zip(df.columns[:-1], df.columns[1:]):
            # For variable passing, callback_end, calllback_start
            # In case of communication, callback_end,rclcpp_publish/rclcpp_intra_publish
            tracepoint_name_ = DataFrameColumnNameParser(column_name_).tracepoint_name
            tracepoint_name = DataFrameColumnNameParser(column_name).tracepoint_name

            is_callback_end = tracepoint_name_ == TRACE_POINT.CALLBACK_END_TIMESTAMP
            is_variable_pasisng = tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP

            if is_callback_end and is_variable_pasisng:
                continue

            if tracepoint_name_ not in [
                TRACE_POINT.CALLBACK_START_TIMESTAMP,
                TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP,
            ]:
                drop_columns.append(column_name_)

        df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            parser = DataFrameColumnNameParser(column_name)
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [start]'
            elif parser.tracepoint_name == TRACE_POINT.CALLBACK_END_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [end]'
            elif parser.tracepoint_name == TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [publish]'
            elif parser.tracepoint_name == TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [publish]'
        df.rename(columns=renames, inplace=True)


class NodeLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        callback_level_formatter = CallbackLevelFormatter()
        callback_level_formatter.remove_columns(df)

        drop_columns = []

        # remove callbacks in the same node
        for i, column_name in enumerate(df.columns):
            parser = DataFrameColumnNameParser(column_name)

            is_next_same_node = False
            is_before_same_node = False
            if i+1 < len(df.columns):
                parser_ = DataFrameColumnNameParser(df.columns[i+1])
                is_next_same_node = parser.node_name == parser_.node_name
            if i-1 >= 0:
                parser_ = DataFrameColumnNameParser(df.columns[i-1])
                is_before_same_node = parser.node_name == parser_.node_name

            if is_next_same_node and is_before_same_node:
                drop_columns.append(column_name)
        df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        path

        renames = {}
        for column_name in df.columns:
            parser = DataFrameColumnNameParser(column_name)
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [start]'
            elif parser.tracepoint_name == TRACE_POINT.CALLBACK_END_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [end]'
            elif parser.tracepoint_name == TRACE_POINT.RCLCPP_PUBLISH_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [publish]'
            elif parser.tracepoint_name == TRACE_POINT.RCLCPP_INTRA_PUBLISH_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [publish]'

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
