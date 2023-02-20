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

from typing import List, Optional, Sequence, Tuple, Union

from bokeh.models import HoverTool
from bokeh.plotting import ColumnDataSource

from .legend import HoverCreator, LegendKeys, LegendManager, LegendSource

from ....record import RecordsInterface
from ....runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


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
        """
        Create HoverTool based on the legend keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
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
