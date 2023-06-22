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

from typing import Any

from bokeh.models import HoverTool, Legend
from bokeh.plotting import ColumnDataSource, Figure

from .util import (apply_x_axis_offset, ColorSelectorFactory,
                   HoverKeysFactory, HoverSource, init_figure)


class BokehStackedBar:

    def __init__(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,
    ) -> None:
        self._metrics = metrics
        self._xaxis_type = xaxis_type
        self._ywheel_zoom = ywheel_zoom
        self._full_legends = full_legends
        self._case = case

    def create_figure(self) -> Figure:
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
        data: dict[str, list[int | float]]
        y_labels: list[str] = []
        y_axis_label = 'latency [ms]'
        target_objects = self._metrics.target_objects
        data, y_labels = self._metrics.to_stacked_bar_data()
        title: str = f"Stacked bar of '{getattr(target_objects, 'path_name')}'"

        fig = init_figure(title, self._ywheel_zoom, self._xaxis_type, y_axis_label)
        frame_min = data['start time'][0]
        frame_max = data['start time'][-1]
        x_label = 'start time'
        if self._xaxis_type == 'system_time':
            apply_x_axis_offset(fig, frame_min, frame_max)
        elif self._xaxis_type == 'index':
            x_label = 'index'
        else:  # sim_time
            raise NotImplementedError()

        color_selector = ColorSelectorFactory.create_instance(coloring_rule='unique')
        if self._case == 'best':
            color_selector.get_color()
        stacked_bar_source = StackedBarSource(target_objects)
        fig.add_tools(stacked_bar_source.create_hover())
        stacked_bar_data, x_width_list = \
            self._get_stacked_bar_data(data, y_labels, self._xaxis_type, x_label)
        bottom_labels = self._get_bottom_labels(y_labels)

        vbar_stacks = []
        for y_label, bottom_label in zip(y_labels, bottom_labels):
            vbar_stack = fig.vbar(
                x='start time',
                top=y_label,
                width='x_width_list',
                source=stacked_bar_source.generate(y_label, stacked_bar_data,
                                                   x_width_list, bottom_label),
                color=color_selector.get_color(y_label),
                bottom=bottom_label
            )
            vbar_stacks.append((y_label, [vbar_stack]))

        legend = Legend(items=vbar_stacks, location='bottom_left')
        fig.add_layout(legend, 'below')
        fig.legend.click_policy = 'mute'

        return fig

    @staticmethod
    def _get_stacked_bar_data(
        data: dict[str, list[int | float]],
        y_labels: list[str],
        xaxis_type: str,
        x_label: str,
    ) -> tuple[dict[str, list[float]],  list[float]]:
        """
        Calculate stacked bar data.

        Parameters
        ----------
        data : dict[str, list[int]]
            Source data.
        y_labels : list[str]
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
        dict[str, list[float]]
            Stacked bar data.
        list[float]
            Width list of bars.

        """
        output_data: dict[str, list[float]] = {}
        x_width_list: list[float] = []

        # Convert the data unit to second
        output_data = BokehStackedBar._updated_with_unit(data, y_labels, 1e-6)
        output_data = BokehStackedBar._updated_with_unit(output_data, ['start time'], 1e-9)

        # Calculate the stacked y values
        output_data = BokehStackedBar._stacked_y_values(output_data, y_labels)

        if xaxis_type == 'system_time':
            # Update the timestamps from absolutely time to offset time
            output_data[x_label] = BokehStackedBar._updated_timestamps_to_offset_time(
                output_data[x_label])

            x_width_list = BokehStackedBar._get_x_width_list(output_data[x_label])
            half_width_list = [x / 2 for x in x_width_list]

            # Slide x axis values so that the bottom left of bars are the start time.
            output_data[x_label] = BokehStackedBar._add_shift_value(
                output_data[x_label], half_width_list)
        elif xaxis_type == 'sim_time':
            raise NotImplementedError()
        else:  # index
            output_data[x_label] = list(range(0, len(output_data[y_labels[0]])))
            x_width_list = BokehStackedBar._get_x_width_list(output_data[x_label])

        return output_data, x_width_list

    @staticmethod
    def _updated_with_unit(
        data: dict[str, list[int | float]],
        columns: list[str] | None,
        unit: float,
    ) -> dict[str, list[float]]:
        # TODO: make timeseries and callback scheduling function use this function.
        #       create bokeh_util.py
        if columns is None:
            output_data: dict[str, list[float]] = {}
            for key in data.keys():
                output_data[key] = [d * unit for d in data[key]]
        else:
            output_data = data
            for key in columns:
                output_data[key] = [d * unit for d in data[key]]
        return output_data

    @staticmethod
    def _stacked_y_values(
        data: dict[str, list[float]],
        y_values: list[str],
    ) -> dict[str, list[float]]:
        for prev_, next_ in zip(reversed(y_values[:-1]), reversed(y_values[1:])):
            data[prev_] = [data[prev_][i] + data[next_][i] for i in range(len(data[next_]))]
        return data

    @staticmethod
    def _updated_timestamps_to_offset_time(
        x_values: list[float]
    ):
        new_values: list[float] = []
        first_time = x_values[0]
        for time in x_values:
            new_values.append(time - first_time)
        return new_values

    @staticmethod
    def _get_x_width_list(x_values: list[float]) -> list[float]:
        """
        Get width between a x value and next x value.

        Parameters
        ----------
        x_values : list[float]
            X values list.

        Returns
        -------
        list[float]
            Width list.

        """
        # TODO: create bokeh_util.py and move this.
        x_width_list: list[float] = \
            [(x_values[i+1]-x_values[i]) * 0.99 for i in range(len(x_values)-1)]
        x_width_list.append(x_width_list[-1])
        return x_width_list

    @staticmethod
    def _add_shift_value(
        values: list[float],
        shift_values: list[float]
    ) -> list[float]:
        """
        Add shift values to target values.

        Parameters
        ----------
        values : list[float]
            Target values.
        shift_values : list[float]
            Shift values

        Returns
        -------
        list[float]
            Updated values.

        """
        # TODO: create bokeh_util.py and move this.
        return [values[i] + shift_values[i] for i in range(len(values))]

    @staticmethod
    def _get_bottom_labels(labels: list[str]) -> list[str]:
        return [label + '_bottom' for label in labels]


class StackedBarSource:
    """Class to generate stacked bar source."""

    def __init__(
        self,
        target_object,
    ) -> None:
        self._hover_keys = HoverKeysFactory.create_instance('stacked_bar', target_object)
        self._hover_source = HoverSource(self._hover_keys)

    def create_hover(self, options: dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the hover keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
        return self._hover_keys.create_hover(options)

    def generate(
        self,
        y_label: str,
        stacked_bar_data: dict[str, list[float]],
        x_width_list: list[float],
        bottom_label: str
    ) -> ColumnDataSource:
        target_data = stacked_bar_data[y_label]
        y_labels = list(stacked_bar_data.keys())
        idx_one_below = y_labels.index(y_label) + 1
        # HACK: This assumes that 'start time' is at the bottom of y_labels.
        if y_labels[idx_one_below] != 'start time':
            latencies = [
                target - below for target, below in
                zip(target_data, stacked_bar_data[y_labels[idx_one_below]])
            ]
        else:
            latencies = target_data

        bottom_value: list[float] = [0] * len(stacked_bar_data[y_label])
        label_idx_of_bottom_value = y_labels.index(y_label) + 1
        if label_idx_of_bottom_value < len(y_labels) - 1:
            bottom_value = stacked_bar_data[y_labels[label_idx_of_bottom_value]]

        source = ColumnDataSource({y_label: target_data})
        source.add(stacked_bar_data['start time'], 'start time')
        source.add(bottom_value, bottom_label)
        source.add(x_width_list, 'x_width_list')
        source.add([f'{y_label}'] * len(x_width_list), 'label')
        source.add(['latency = ' + str(_) for _ in latencies], 'latency')

        return source
