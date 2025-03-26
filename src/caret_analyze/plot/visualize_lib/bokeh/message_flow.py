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

from abc import ABCMeta, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import Any

from bokeh.models import CrosshairTool, HoverTool
from bokeh.plotting import ColumnDataSource, figure as Figure

import numpy as np

import pandas as pd

from .util import (apply_x_axis_offset, ColorSelectorFactory, get_callback_param_desc,
                   HoverKeysFactory, HoverSource, init_figure, RectValues)
from ....common import ClockConverter
from ....record.data_frame_shaper import Clip, Strip
from ....runtime import Path


class BokehMessageFlow:

    def __init__(
        self,
        target_path: Path,
        xaxis_type: str,
        ywheel_zoom: bool,
        granularity: str,
        treat_drop_as_delay: bool,
        lstrip_s: float,
        rstrip_s: float
    ) -> None:
        self._target_path = target_path
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._granularity = granularity
        self._treat_drop_as_delay = treat_drop_as_delay
        self._lstrip_s = lstrip_s
        self._rstrip_s = rstrip_s

    def create_figure(self) -> Figure:
        """
        Create message flow figure.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of message flow.

        """
        # Initialize figure
        fig = init_figure(
            f'Message flow of {self._target_path.path_name}', self._ywheel_zoom, self._xaxis_type)
        fig.add_tools(CrosshairTool(line_alpha=0.4))

        # Strip
        df = self._target_path.to_dataframe(treat_drop_as_delay=self._treat_drop_as_delay)
        strip = Strip(self._lstrip_s, self._rstrip_s)
        clip = strip.to_clip(df)
        df = clip.execute(df)

        # Apply xaxis offset
        frame_min: float = clip.min_ns
        frame_max: float = clip.max_ns
        converter: ClockConverter | None = None
        if self._xaxis_type == 'sim_time':
            assert len(self._target_path.child) > 0
            # TODO(hsgwa): refactor
            provider = self._target_path.child[0]._provider  # type: ignore
            converter = provider.get_sim_time_converter(frame_min, frame_max)
        if converter:
            frame_min = converter.convert(frame_min)
            frame_max = converter.convert(frame_max)
        offset =\
            Offset(round(converter.convert(clip.min_ns))) if converter else Offset(clip.min_ns)
        apply_x_axis_offset(fig, frame_min, frame_max)

        # Format
        formatter = FormatterFactory.create(self._target_path, self._granularity)
        formatter.remove_columns(df)
        formatter.rename_columns(df, self._target_path)

        yaxis_property = YAxisProperty(df)
        yaxis_values = YAxisValues(df)
        fig.yaxis.ticker = yaxis_property.values
        fig.yaxis.major_label_overrides = yaxis_property.labels_dict

        # Draw callback rect
        rect_source = MessageFlowRectSource(self._target_path)
        rect = fig.rect(
            'x',
            'y',
            'width',
            'height',
            source=rect_source.generate(yaxis_values, self._granularity, clip, converter, offset),
            color='black',
            alpha=0.15,
            hover_fill_color='black',
            hover_alpha=0.4
        )
        fig.add_tools(rect_source.create_hover({'renderers': [rect]}))

        # Draw message flow
        color_selector = ColorSelectorFactory.create_instance('unique')
        flow_source = MessageFlowLineSource(self._target_path)
        for source in flow_source.generate(df, converter, offset):
            line = fig.line(
                x='x',
                y='y',
                line_width=1.5,
                line_color=color_selector.get_color(),
                line_alpha=1,
                source=source
            )
            fig.add_tools(flow_source.create_hover({'renderers': [line]}))

        return fig


class MessageFlowRectSource:
    """Class to generate message flow rect sources."""

    def __init__(
        self,
        target_path: Path
    ) -> None:
        self._target_path = target_path
        self._hover_keys = HoverKeysFactory.create_instance('message_flow_rect', target_path)
        self._hover_source = HoverSource(self._hover_keys)

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

    def generate(
        self,
        y_axis_values: YAxisValues,
        granularity: str,
        clip: Clip | None,
        converter: ClockConverter | None,
        offset: Offset
    ) -> ColumnDataSource:
        """
        Generate message flow rect source.

        Parameters
        ----------
        y_axis_values : YAxisValues
            Y-axis values.
        granularity : str
            Granularity of chain with two value; [raw/node].
        clip : Clip | None
            Clip the first and last few seconds.
        converter : ClockConverter | None
            Converter to simulation time.
        offset : Offset
            Offset of x-axis.

        Returns
        -------
        ColumnDataSource
            Generated message flow rect source

        """
        rect_source = ColumnDataSource(data={
            k: [] for k in (['x', 'y', 'width', 'height'] + self._hover_keys.to_list())
        })

        if self._target_path.callback_chain is None:
            return rect_source

        for callback in self._target_path.callback_chain:
            if granularity == 'raw':
                search_name = callback.callback_name
            elif granularity == 'node':
                search_name = callback.node_name

            y_max_list = np.array(y_axis_values.get_start_indexes(search_name))
            y_min_list = y_max_list - 1

            for y_min, y_max in zip(y_min_list, y_max_list):
                df = callback.to_dataframe(shaper=clip)
                for _, row in df.iterrows():
                    callback_start = row.to_list()[0]
                    callback_end = row.to_list()[-1]
                    if converter:
                        callback_start = converter.convert(callback_start)
                        callback_end = converter.convert(callback_end)
                    rect = RectValues((callback_start - offset.value) * 10**-9,
                                      (callback_end - offset.value) * 10**-9,
                                      y_min, y_max)
                    rect_source.stream({
                        **{'x': [rect.x],
                           'y': [rect.y],
                           'width': [rect.width],
                           'height': [rect.height]},
                        **self._hover_source.generate(callback, {
                            't_start':
                            f't_start = {to_format_str(callback_start - offset.value)} [s]',
                            't_end': f't_end = {to_format_str(callback_end - offset.value)} [s]',
                            't_offset': f't_offset = {offset}',
                            'latency': f'latency = {(callback_end-callback_start)*1.0e-6} [ms]',
                            'callback_param': get_callback_param_desc(callback)
                        })  # type: ignore
                    })

        return rect_source


class MessageFlowLineSource:
    """Class to generate message flow line sources."""

    def __init__(
        self,
        target_path: Path
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance('message_flow_line', target_path)

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

    def generate(
        self,
        df: pd.DataFrame,
        converter: ClockConverter | None,
        offset: Offset
    ) -> list[ColumnDataSource]:
        """
        Generate message flow line source.

        Parameters
        ----------
        df : pd.DataFrame
            Formatted latency table for the target path.
        converter : ClockConverter | None
            Converter to simulation time.
        offset : Offset
            Offset of x-axis.

        Returns
        -------
        ColumnDataSource
            Generated message flow line source

        """
        tick_labels = YAxisProperty(df)
        line_sources: list[ColumnDataSource] = []

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
                'x': [(value - offset.value) * 10**-9 for value in row_values],
                'y': tick_labels.values[:len(row_values)],
                'width': [width]*len(row_values),
                'height': [0]*len(row_values),
                't_start': [f't_start = {t_start_s} [s]']*len(row_values),
                't_end': [f't_end = {t_end_s} [s]']*len(row_values),
                't_offset': [f't_offset = {offset}']*len(row_values),
                'latency': [f'latency = {width*1.0e-6} [ms]']*len(row_values),
                'index': [f'index: {i}']*len(row_values),
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


class YAxisProperty:

    def __init__(self, df) -> None:
        self._tick_labels: dict[int, str] = {}
        y_axis_step = -1

        y_value = 0
        for column_name in df.columns:
            self._tick_labels[y_value] = column_name
            y_value += y_axis_step

        return None

    @property
    def values(self):
        """Get values."""
        return list(self._tick_labels.keys())

    @property
    def labels(self):
        """Get labels."""
        return list(self._tick_labels.values())

    @property
    def labels_dict(self) -> dict[int, str]:
        """
        Get labels dict.

        Returns
        -------
        dict[int, str]
            Tick labels.

        """
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

    def get_start_indexes(self, search_name) -> list[int]:
        """
        Get start indexes.

        Parameters
        ----------
        search_name
            The target name.

        Returns
        -------
        list[int]
            Start index of search results.

        """
        indexes = self._search_values(search_name)
        return list((indexes) * -1)

    def get_end_values(self, search_name) -> list[int]:
        """
        Get end indexes.

        Parameters
        ----------
        search_name
            The target name.

        Returns
        -------
        list[int]
            End index of search results.

        """
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
        """
        Create formatter.

        Parameters
        ----------
        path : Path
            The target path.
        granularity : str
            Granularity of chain with two value; [raw/node].

        Returns
        -------
        DataFrameFormatter
            Raw level formatter.

        Raises
        ------
        NotImplementedError
            Argument metrics_name is not "raw" or "node".

        """
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
        """
        Rename columns.

        Parameters
        ----------
        df : pd.DataFrame
            The target pd data frame.
        path : Path
            The target path.

        """
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
        """
        Remove columns.

        Parameters
        ----------
        df : pd.DataFrame
            The target pd data frame.

        """
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
        """
        Rename columns.

        Parameters
        ----------
        df : pd.DataFrame
            The target pd data frame.
        path : Path
            The target path.

        """
        renames = {}
        for column_name in df.columns:
            if '_timestamp' in column_name:
                idx = column_name.rfind('_timestamp')
                renames[column_name] = column_name[:idx]

        df.rename(columns=renames, inplace=True)


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
        """
        Get offset value.

        Returns
        -------
        int
            Offset value.

        """
        return self._offset


def to_format_str(ns: int) -> str:
    s = (ns) * 10**-9
    return '{:.3f}'.format(s)
