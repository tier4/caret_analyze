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
from datetime import datetime
from functools import cached_property
from typing import Dict, List, Optional

from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource

import numpy as np
import pandas as pd

from .util import HoverCreator, HoverKeys, RectValues

from ....common import ClockConverter
from ....exceptions import UnsupportedTypeError
from ....record.data_frame_shaper import Clip
from ....runtime import CallbackBase, Path, SubscriptionCallback, TimerCallback


class Offset:
    def __init__(
        self,
        offset_ns: int
    ) -> None:
        self._offset = offset_ns

    def __str__(self) -> str:
        return self._str

    @cached_property
    def _str(self) -> str:
        t_offset = datetime.fromtimestamp(self._offset * 10**-9)
        return t_offset.isoformat(sep=' ', timespec='seconds')

    @property
    def value(self) -> int:
        return self._offset


def to_format_str(ns: int) -> str:
    s = (ns) * 10**-9
    return '{:.3f}'.format(s)


def get_callback_param_desc(callback: CallbackBase):
    if isinstance(callback, TimerCallback):
        return f'period_ns: {callback.period_ns}'

    if isinstance(callback, SubscriptionCallback):
        return f'topic_name: {callback.subscribe_topic_name}'

    raise UnsupportedTypeError('callback type must be '
                               '[ TimerCallback/ SubscriptionCallback]')


def get_callback_rect_list(
    path: Path,
    y_axi_values: YAxisValues,
    granularity: str,
    clip: Optional[Clip],
    converter: Optional[ClockConverter],
    offset: Offset
) -> ColumnDataSource:
    rect_source = ColumnDataSource(data={
        'x': [],
        'y': [],
        't_start': [],
        't_end': [],
        't_offset': [],
        'desc': [],
        'width': [],
        'latency': [],
        'height': []
    })

    if path.callback_chain is None:
        return rect_source

    x = []
    y = []
    t_start = []
    t_end = []
    t_offset = []
    desc = []
    width = []
    latency = []
    height = []

    for callback in path.callback_chain:
        if granularity == 'raw':
            search_name = callback.callback_name
        elif granularity == 'node':
            search_name = callback.node_name

        y_max_list = np.array(y_axi_values.get_start_indexes(search_name))
        y_mins = y_max_list - 1

        callback_desc = get_callback_param_desc(callback)

        for y_min, y_max in zip(y_mins, y_max_list):
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
                t_offset.append(f't_offset = {offset}')
                t_start.append(f't_start = {to_format_str(callback_start - offset.value)} [s]')
                t_end.append(f't_end = {to_format_str(callback_end - offset.value)} [s]')
                width.append(rect.width)
                latency.append(f'latency = {(callback_end-callback_start)*1.0e-6} [ms]')
                height.append(rect.height)

                desc_str = f'callback_type: {callback.callback_type}' + \
                    f', {callback_desc}, symbol: {callback.symbol}'
                desc.append(desc_str)

    rect_source = ColumnDataSource(data={
        'x': x,
        'y': y,
        't_start': t_start,
        't_end': t_end,
        't_offset': t_offset,
        'desc': desc,
        'width': width,
        'latency': latency,
        'height': height
    })
    return rect_source


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

    def _search_values(self, search_name) -> np.ndarray:
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

            # Exclude all columns except the first column of the node and
            # the last publish column of the node.
            if parsed_before.node_name is not None and parsed.node_name is not None and \
                    parsed_before.node_name == parsed.node_name and \
                    'rclcpp_publish' not in parsed.tracepoint_name and \
                    not ('callback_start' in parsed_before.tracepoint_name and
                         'callback_end' in parsed.tracepoint_name):
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


class MessageFlowSource:

    def __init__(
        self,
        target_path: Path
    ) -> None:
        self._hover_keys = HoverKeys('message_flow', target_path)
        self._hover = HoverCreator(self._hover_keys)

    def create_hover(self,) -> HoverTool:
        return self._hover.create()

    def generate(
        self,
        df: pd.DataFrame,
        converter: Optional[ClockConverter],
        offset: Offset
    ) -> ColumnDataSource:
        tick_labels = YAxisProperty(df)
        line_sources = []

        for i, row in df.iterrows():
            row_values = row.dropna().values
            if converter:
                row_values = [converter.convert(_) for _ in row_values]
            x_min = min(row_values)
            x_max = max(row_values)
            width = x_max - x_min

            t_start_s = to_format_str(x_min-offset.value)
            t_end_s = to_format_str(x_max-offset.value)
            line_source = ColumnDataSource({
                'x': row_values,
                'y': tick_labels.values[:len(row_values)],
                'width': [width]*len(row_values),
                'height': [0]*len(row_values),
                't_start': [f't_start = {t_start_s} [s]']*len(row_values),
                't_end': [f't_end = {t_end_s} [s]']*len(row_values),
                't_offset': [f't_offset = {offset}']*len(row_values),
                'latency': [f'latency = {width*1.0e-6} [ms]']*len(row_values),
                'desc': [f'index: {i}']*len(row_values),
            })

            line_sources.append(line_source)

        return line_sources
