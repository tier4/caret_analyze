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

from logging import getLogger
from typing import Dict, List, Optional, Sequence, Tuple, Union

from bokeh.models import Arrow, NormalHead
from bokeh.plotting import Figure

import pandas as pd

from .callback_scheduling_source import CallbackSchedBarSource, CallbackSchedRectSource
from .message_flow import BokehMessageFlow
from .stacked_bar_source import StackedBarSource
from .timeseries_source import LineSource
from .util import apply_x_axis_offset, ColorSelectorFactory, init_figure, LegendManager
from ..visualize_lib_interface import VisualizeLibInterface
from ...metrics_base import MetricsBase
from ....common import Util
from ....record import Clip, Range
from ....runtime import (CallbackBase, CallbackGroup, Communication, Path, Publisher, Subscription,
                         TimerCallback)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class Bokeh(VisualizeLibInterface):
    """Class that visualizes data using Bokeh library."""

    def __init__(self) -> None:
        pass

    def message_flow(
        self,
        target_path: Path,
        xaxis_type: str,
        ywheel_zoom: bool,
        granularity: str,
        treat_drop_as_delay: bool,
        lstrip_s: float,
        rstrip_s: float
    ) -> Figure:
        message_flow = BokehMessageFlow(
            target_path, xaxis_type, ywheel_zoom, granularity,
            treat_drop_as_delay, lstrip_s, rstrip_s,
        )
        return message_flow.create_figure()

    def stacked_bar(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,  # best or worst
    ) -> Figure:

        # NOTE: relation between stacked bar graph and data struct
        # # data = {
        # #     a : [a1, a2, a3],
        # #     b : [b1, b2, b3],
        # #     'start time': [s1, s2, s3]
        # # }
        # # y_labels = [a, b]

        # # ^               ^
        # # |               |       ^       [] a
        # # |       ^       |       |       [] b
        # # |       |       a2      |
        # # |       a1      ^       a3
        # # |       ^       |       ^
        # # |       |       |       |
        # # |       b1      b2      b3
        # # +-------s1------s2------s3---------->

        # # get stacked bar data
        data: Dict[str, list[int | float]] = {}
        y_labels: List[str] = []
        y_axis_label = 'latency [ms]'
        target_objects = metrics.target_objects
        data, y_labels = metrics.to_stacked_bar_data()
        title: str = f"Stacked bar of '{getattr(target_objects, 'path_name')}'"

        fig = init_figure(title, ywheel_zoom, xaxis_type, y_axis_label)
        frame_min = data['start time'][0]
        frame_max = data['start time'][-1]
        x_label = 'start time'
        if xaxis_type == 'system_time':
            apply_x_axis_offset(fig, frame_min, frame_max)
        elif xaxis_type == 'index':
            x_label = 'index'
        else:  # sim_time
            raise NotImplementedError()

        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        if case == 'best':
            color_selector.get_color()
        legend_manager = LegendManager()
        stacked_bar_source = StackedBarSource(legend_manager, target_objects, 0.0, xaxis_type)
        fig.add_tools(stacked_bar_source.create_hover())
        stacked_bar_data, x_width_list = \
            Bokeh._get_stacked_bar_data(data, y_labels, xaxis_type, x_label)
        bottom_labels = Bokeh._get_bottom_labels(y_labels)
        bottom_labels = bottom_labels[1:]
        source = stacked_bar_source.generate(
            target_objects,
            stacked_bar_data,
            y_labels,
            bottom_labels,
            x_width_list
        )

        for y_label, bottom in zip(y_labels[:-1], bottom_labels):
            color = color_selector.get_color(y_label)
            renderer = fig.vbar(
                x=x_label,
                top=y_label,
                width='x_width_list',
                source=source,
                color=color,
                bottom=bottom
            )
            legend_manager.add_legend(y_label, renderer, y_label)
        color = color_selector.get_color(y_labels[-1])
        renderer = fig.vbar(
            x=x_label,
            top=y_labels[-1],
            width='x_width_list',
            source=source,
            color=color
        )
        legend_manager.add_legend(y_labels[-1], renderer, y_labels[-1])

        num_legend_threshold = 20
        legends = legend_manager.create_legends(num_legend_threshold,
                                                full_legends,
                                                location='bottom_left'
                                                )
        for legend in legends:
            fig.add_layout(legend, 'below')
        fig.legend.click_policy = 'mute'

        return fig

    @staticmethod
    def _get_stacked_bar_data(
        data: Dict[str, List[int | float]],
        y_labels: List[str],
        xaxis_type: str,
        x_label: str,
    ) -> Tuple[Dict[str, List[float]],  List[float]]:
        """
        Calculate stacked bar data.

        Parameters
        ----------
        data : Dict[str, List[int]]
            Source data.
        y_labels : List[str]
            Y axis labels that are Node/Topic name.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        x_label : str
            X-axis label of data dict.
            "start time" or "index".

        Returns
        -------
        Dict[str, List[float]]
            Stacked bar data.
        List[float]
            Width list of bars.

        """
        output_data: Dict[str, List[float]] = {}
        x_width_list: List[float] = []

        # Convert the data unit to second
        output_data = Bokeh._updated_with_unit(data, y_labels, 1e-6)
        output_data = Bokeh._updated_with_unit(output_data, ['start time'], 1e-9)

        # Calculate the stacked y values
        output_data = Bokeh._stacked_y_values(output_data, y_labels)

        if xaxis_type == 'system_time':
            # Update the timestamps from absolutely time to offset time
            output_data[x_label] = Bokeh._updated_timestamps_to_offset_time(output_data[x_label])

            x_width_list = Bokeh._get_x_width_list(output_data[x_label])
            half_width_list = [x / 2 for x in x_width_list]

            # Slide x axis values so that the bottom left of bars are the start time.
            output_data[x_label] = Bokeh._add_shift_value(output_data[x_label], half_width_list)
        elif xaxis_type == 'sim_time':
            raise NotImplementedError()
        else:  # index
            output_data[x_label] = list(range(0, len(output_data[y_labels[0]])))
            x_width_list = Bokeh._get_x_width_list(output_data[x_label])

        return output_data, x_width_list

    @staticmethod
    def _updated_with_unit(
        data: Dict[str, List[Union[int, float]]],
        columns: Optional[List[str]],
        unit: float,
    ) -> Dict[str, List[float]]:
        # TODO: make timeseries and callback scheduling function use this function.
        #       create bokeh_util.py
        if columns is None:
            output_data: Dict[str, List[float]] = {}
            for key in data.keys():
                output_data[key] = [d * unit for d in data[key]]
        else:
            output_data = data
            for key in columns:
                output_data[key] = [d * unit for d in data[key]]
        return output_data

    @staticmethod
    def _stacked_y_values(
        data: Dict[str, List[float]],
        y_values: List[str],
    ) -> Dict[str, List[float]]:
        for prev_, next_ in zip(reversed(y_values[:-1]), reversed(y_values[1:])):
            data[prev_] = [data[prev_][i] + data[next_][i] for i in range(len(data[next_]))]
        return data

    @staticmethod
    def _updated_timestamps_to_offset_time(
        x_values: List[float]
    ):
        new_values: List[float] = []
        first_time = x_values[0]
        for time in x_values:
            new_values.append(time - first_time)
        return new_values

    @staticmethod
    def _get_x_width_list(x_values: List[float]) -> List[float]:
        """
        Get width between a x value and next x value.

        Parameters
        ----------
        x_values : List[float]
            X values list.

        Returns
        -------
        List[float]
            Width list.

        """
        # TODO: create bokeh_util.py and move this.
        x_width_list: List[float] = \
            [(x_values[i+1]-x_values[i]) * 0.99 for i in range(len(x_values)-1)]
        x_width_list.append(x_width_list[-1])
        return x_width_list

    @staticmethod
    def _add_shift_value(
        values: List[float],
        shift_values: List[float]
    ) -> List[float]:
        """
        Add shift values to target values.

        Parameters
        ----------
        values : List[float]
            Target values.
        shift_values : List[float]
            Shift values

        Returns
        -------
        List[float]
            Updated values.

        """
        # TODO: create bokeh_util.py and move this.
        return [values[i] + shift_values[i] for i in range(len(values))]

    @staticmethod
    def _get_bottom_labels(labels: List[str]) -> List[str]:
        return [label + '_bottom' for label in labels]

    def callback_scheduling(
        self,
        callback_groups: Sequence[CallbackGroup],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        coloring_rule: str,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> Figure:
        """
        Get callback scheduling figure.

        Parameters
        ----------
        callback_groups : Sequence[CallbackGroup]
            The target callback groups.
        xaxis_type : str, optional
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool, optional
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool, optional
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.
        coloring_rule : str, optional
            The unit of color change
            There are there rules which are [callback/callback_group/node], by default 'callback'
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        bokeh.plotting.Figure

        """
        # Initialize figure
        title = ('Callback Scheduling in '
                 f"[{'/'.join([cbg.callback_group_name for cbg in callback_groups])}].")
        fig = init_figure(title, ywheel_zoom, xaxis_type)

        # Apply xaxis offset
        callbacks: List[CallbackBase] = Util.flatten(
            cbg.callbacks for cbg in callback_groups if len(cbg.callbacks) > 0)
        records_range = Range([cb.to_records() for cb in callbacks])
        range_min, range_max = records_range.get_range()
        clip_min = int(range_min + lstrip_s*1.0e9)
        clip_max = int(range_max - rstrip_s*1.0e9)
        clip = Clip(clip_min, clip_max)
        if xaxis_type == 'sim_time':
            # TODO(hsgwa): refactor
            converter = callbacks[0]._provider.get_sim_time_converter()
            frame_min = converter.convert(clip.min_ns)
            frame_max = converter.convert(clip.max_ns)
        else:
            converter = None
            frame_min = clip.min_ns
            frame_max = clip.max_ns
        x_range_name = 'x_plot_axis'
        apply_x_axis_offset(fig, frame_min, frame_max, x_range_name)

        # Draw callback scheduling
        color_selector = ColorSelectorFactory.create_instance(coloring_rule)
        legend_manager = LegendManager()
        rect_source_gen = CallbackSchedRectSource(legend_manager, callbacks[0], clip, converter)
        bar_source_gen = CallbackSchedBarSource(legend_manager, callbacks[0], frame_min, frame_max)

        for cbg in callback_groups:
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
                    hover_alpha=1.0,
                    x_range_name=x_range_name
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
                    level='underlay',
                    x_range_name=x_range_name
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
        legends = legend_manager.create_legends(num_legend_threshold, full_legends)
        for legend in legends:
            fig.add_layout(legend, 'right')
        fig.legend.click_policy = 'hide'

        fig.ygrid.grid_line_alpha = 0
        fig.yaxis.visible = False

        return fig

    def timeseries(
        self,
        metrics: MetricsBase,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        """
        Get a timeseries figure.

        Parameters
        ----------
        metrics : MetricsBase
            Metrics to be y-axis in visualization.
        xaxis_type : str
            Type of x-axis of the line graph to be plotted.
            "system_time", "index", or "sim_time" can be specified.
            The default is "system_time".
        ywheel_zoom : bool
            If True, the drawn graph can be expanded in the y-axis direction
            by the mouse wheel.
        full_legends : bool
            If True, all legends are drawn
            even if the number of legends exceeds the threshold.

        Returns
        -------
        bokeh.plotting.Figure
            Figure of timeseries.

        """
        target_objects = metrics.target_objects
        timeseries_records_list = metrics.to_timeseries_records_list(xaxis_type)

        # Initialize figure
        y_axis_label = timeseries_records_list[0].columns[1]
        if y_axis_label == 'frequency':
            y_axis_label = y_axis_label + ' [Hz]'
        elif y_axis_label in ['period', 'latency']:
            y_axis_label = y_axis_label + ' [ms]'
        else:
            raise NotImplementedError()
        if isinstance(target_objects[0], CallbackBase):
            title = f'Time-line of callbacks {y_axis_label}'
        elif isinstance(target_objects[0], Communication):
            title = f'Time-line of communications {y_axis_label}'
        else:
            title = f'Time-line of publishes/subscribes {y_axis_label}'
        fig = init_figure(title, ywheel_zoom, xaxis_type, y_axis_label)

        # Apply xaxis offset
        records_range = Range([to.to_records() for to in target_objects])
        frame_min, frame_max = records_range.get_range()
        if xaxis_type == 'system_time':
            apply_x_axis_offset(fig, frame_min, frame_max)

        # Draw lines
        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        legend_manager = LegendManager()
        line_source = LineSource(legend_manager, target_objects[0], frame_min, xaxis_type)
        fig.add_tools(line_source.create_hover())
        for to, timeseries in zip(target_objects, timeseries_records_list):
            renderer = fig.line(
                'x', 'y',
                source=line_source.generate(to, timeseries),
                color=color_selector.get_color()
            )
            legend_manager.add_legend(to, renderer)

        # Draw legends
        num_legend_threshold = 20
        """
        In Autoware, the number of callbacks in a node is less than 20.
        Here, num_legend_threshold is set to 20 as the maximum value.
        """
        legends = legend_manager.create_legends(num_legend_threshold, full_legends)
        for legend in legends:
            fig.add_layout(legend, 'right')
        fig.legend.click_policy = 'hide'

        return fig
