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

from abc import ABCMeta, abstractmethod
from typing import Dict, Optional

from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.palettes import Bokeh8
from bokeh.plotting import figure
from bokeh.resources import CDN
import pandas as pd

from ...callback import SubscriptionCallback
from ...path import Path
from ...record.trace_points import TRACE_POINT
from ...util import Util


def message_flow(
    path: Path,
    export_path: Optional[str] = None,
    granularity: Optional[str] = None,
    treat_drop_as_delay=False
):

    granularity = granularity or 'raw'
    assert granularity in ['raw', 'callback', 'node']

    fig = figure(plot_width=1000, plot_height=400)
    color_palette = ColorPalette(Bokeh8)

    df = path.to_dataframe(treat_drop_as_delay=treat_drop_as_delay)

    formatter = FormatterFactory.create(granularity)
    formatter.remove_columns(df)
    formatter.rename_columns(df, path)

    tick_labels = TickLabels(df)

    for i, row in df.iterrows():
        fig.line(
            x=row.values,
            y=tick_labels.values,
            line_width=1.5,
            line_color=color_palette.get_index_color(i),
            line_alpha=1,
        )

    fig.yaxis.ticker = tick_labels.values
    fig.yaxis.major_label_overrides = tick_labels.labels_dict
    fig.add_tools(CrosshairTool(line_alpha=0.4))

    if export_path is None:
        show(fig)
    else:
        save(fig, export_path, title='time vs tracepoint', resources=CDN)


class ColumnNameParser:

    def __init__(self, column_name):
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


class TickLabels:

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
            parser = ColumnNameParser(column_name)
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
            else:
                new_column_name = f'{unique_name}/{tracepoint_name}'

            new_column_name = new_column_name[:-(len('_timestamp'))]
            renames[column_name] = new_column_name

        df.rename(columns=renames, inplace=True)


class CallbackLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        # 購読側のtopic_nameから扱うため、逆順で処理
        drop_columns = []
        for column_name in df.columns:
            parser = ColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name

            if tracepoint_name not in [
                TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
                TRACE_POINT.CALLBACK_START_TIMESTAMP,
                TRACE_POINT.CALLBACK_END_TIMESTAMP,
            ]:
                drop_columns.append(column_name)

        df.drop(drop_columns, axis=1, inplace=True)
        drop_columns = []

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            parser = ColumnNameParser(column_name)
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [start]'
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_END_TIMESTAMP:
                renames[column_name] = parser.unique_name + ' [end]'
        df.rename(columns=renames, inplace=True)


class NodeLevelFormatter(DataFrameFormatter):

    def remove_columns(self, df: pd.DataFrame) -> None:
        drop_columns = []
        for column_name in df.columns:
            parser = ColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name

            if tracepoint_name not in [
                TRACE_POINT.ON_DATA_AVAILABLE_TIMESTAMP,
                TRACE_POINT.CALLBACK_START_TIMESTAMP,
                TRACE_POINT.CALLBACK_END_TIMESTAMP,
            ]:
                drop_columns.append(column_name)

        for i, column_name in enumerate(df.columns):
            parser = ColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name

            is_next_same_node = False
            is_before_same_node = False
            if i+1 < len(df.columns):
                parser_ = ColumnNameParser(df.columns[i+1])
                is_next_same_node = parser.node_name == parser_.node_name
            if i-1 >= 0:
                parser_ = ColumnNameParser(df.columns[i-1])
                is_before_same_node = parser.node_name == parser_.node_name

            if is_next_same_node and is_before_same_node:
                drop_columns.append(column_name)
        df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(self, df: pd.DataFrame, path: Path) -> None:
        renames = {}
        for column_name in df.columns:
            parser = ColumnNameParser(column_name)
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_START_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [start]'
            if parser.tracepoint_name == TRACE_POINT.CALLBACK_END_TIMESTAMP:
                renames[column_name] = parser.node_name + ' [end]'

        df.rename(columns=renames, inplace=True)
