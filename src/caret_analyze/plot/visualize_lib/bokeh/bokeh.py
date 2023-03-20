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

import datetime
from logging import getLogger
from typing import List, Optional, Sequence, Union

from bokeh.models import AdaptiveTicker, Arrow, LinearAxis, NormalHead, Range1d
from bokeh.models import CrosshairTool
from bokeh.plotting import Figure, figure

import pandas as pd

from .callback_scheduling_source import CallbackSchedBarSource, CallbackSchedRectSource
from .color_selector import ColorSelectorFactory
from .legend import LegendManager
from .message_flow_source import (
    FormatterFactory, get_callback_rect_list, MessageFlowSource,
    Offset, YAxisProperty, YAxisValues)
from .timeseries_source import LineSource
from ..visualize_lib_interface import VisualizeLibInterface
from ...metrics_base import MetricsBase
from ....common import ClockConverter, Util
from ....record import Clip, Range
from ....record.data_frame_shaper import Strip
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
        # Initialize figure
        fig = self._init_figure(
            f'Message flow of {target_path.path_name}', ywheel_zoom, xaxis_type)
        fig.add_tools(CrosshairTool(line_alpha=0.4))

        # Strip
        df = target_path.to_dataframe(treat_drop_as_delay=treat_drop_as_delay)
        strip = Strip(lstrip_s, rstrip_s)
        clip = strip.to_clip(df)
        df = clip.execute(df)
        offset = Offset(clip.min_ns)

        # Apply xaxis offset
        frame_min: float = clip.min_ns
        frame_max: float = clip.max_ns
        converter: Optional[ClockConverter] = None
        if xaxis_type == 'sim_time':
            assert len(target_path.child) > 0
            # TODO(hsgwa): refactor
            converter = target_path.child[0]._provider.get_sim_time_converter()  # type: ignore
        if converter:
            frame_min = converter.convert(frame_min)
            frame_max = converter.convert(frame_max)
        x_range_name = 'x_plot_axis'
        self._apply_x_axis_offset(fig, frame_min, frame_max, x_range_name)

        # Format
        formatter = FormatterFactory.create(target_path, granularity)
        formatter.remove_columns(df)
        formatter.rename_columns(df, target_path)

        yaxis_property = YAxisProperty(df)
        yaxis_values = YAxisValues(df)
        fig.yaxis.ticker = yaxis_property.values
        fig.yaxis.major_label_overrides = yaxis_property.labels_dict

        # Draw callback rect
        rect_source = get_callback_rect_list(
            target_path, yaxis_values, granularity, clip, converter, offset)
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

        # Draw message flow
        color_selector = ColorSelectorFactory.create_instance('unique')
        flow_source = MessageFlowSource(target_path)
        fig.add_tools(flow_source.create_hover())
        for source in flow_source.generate(df, converter, offset):
            fig.line(
                x='x',
                y='y',
                line_width=1.5,
                line_color=color_selector.get_color(),
                line_alpha=1,
                source=source,
                x_range_name=x_range_name
            )

        return fig

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
        fig = self._init_figure(title, ywheel_zoom, xaxis_type)

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
        self._apply_x_axis_offset(fig, frame_min, frame_max, x_range_name)

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
        fig = self._init_figure(title, ywheel_zoom, xaxis_type, y_axis_label)

        # Apply xaxis offset
        records_range = Range([to.to_records() for to in target_objects])
        frame_min, frame_max = records_range.get_range()
        if xaxis_type == 'system_time':
            self._apply_x_axis_offset(fig, frame_min, frame_max)

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

    def _init_figure(
        self,
        title: str,
        ywheel_zoom: bool,
        xaxis_type: str,
        y_axis_label: Optional[str] = None,
    ) -> Figure:
        if xaxis_type == 'system_time':
            x_axis_label = 'system time [s]'
        elif xaxis_type == 'sim_time':
            x_axis_label = 'simulation time [s]'
        else:
            x_axis_label = xaxis_type

        if ywheel_zoom:
            tools = ['wheel_zoom', 'pan', 'box_zoom', 'save', 'reset']
            active_scroll = 'wheel_zoom'
        else:
            tools = ['xwheel_zoom', 'xpan', 'save', 'reset']
            active_scroll = 'xwheel_zoom'

        return figure(
            frame_height=270, frame_width=800, title=title, y_axis_label=y_axis_label or '',
            x_axis_label=x_axis_label, tools=tools, active_scroll=active_scroll
        )

    @staticmethod
    def _apply_x_axis_offset(
        fig: Figure,
        min_ns: float,
        max_ns: float,
        x_range_name: str = ''
    ) -> None:
        """
        Apply an offset to the x-axis of the graph.

        Datetime is displayed instead of UNIX time for zero point.

        Parameters
        ----------
        fig : Figure
            Target figure.
        min_ns : float
            Minimum UNIX time.
        max_ns : float
            Maximum UNIX time.
        x_range_name : str, optional
            Name of the actual range, by default ''.
            Specify this if you want to refer to the actual range later.

        """
        # Initialize variables
        offset_s = min_ns*1.0e-9
        end_s = (max_ns-min_ns)*1.0e-9
        actual_range = Range1d(start=min_ns, end=max_ns)
        applied_range = Range1d(start=0, end=end_s)

        # Set ranges
        fig.extra_x_ranges = {x_range_name: actual_range}
        fig.x_range = applied_range

        # Add xaxis for actual_range
        xaxis = LinearAxis(x_range_name=x_range_name)
        xaxis.visible = False  # type: ignore
        fig.add_layout(xaxis, 'below')
        fig.xaxis.ticker = AdaptiveTicker(min_interval=0.1, mantissas=[1, 2, 5])

        # Add xgrid
        fig.xgrid.minor_grid_line_color = 'black'
        fig.xgrid.minor_grid_line_alpha = 0.1

        # Replace 0 with datetime of offset_s
        datetime_s = datetime.datetime.fromtimestamp(offset_s).strftime('%Y-%m-%d %H:%M:%S')
        fig.xaxis.major_label_overrides = {0: datetime_s}

        # # Code to display hhmmss for x-axis
        # from bokeh.models import FuncTickFormatter
        # fig.xaxis.formatter = FuncTickFormatter(
        #     code = '''
        #     let time_ms = (tick + offset_s) * 1e3;
        #     let date_time = new Date(time_ms);
        #     let hh = date_time.getHours();
        #     let mm = date_time.getMinutes();
        #     let ss = date_time.getSeconds();
        #     return hh + ":" + mm + ":" + ss;
        #     ''',
        #     args={"offset_s": offset_s})
