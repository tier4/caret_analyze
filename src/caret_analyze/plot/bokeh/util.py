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

from logging import getLogger
from typing import Sequence, Tuple, Union

from bokeh.models import LinearAxis, Range1d, SingleIntervalTicker
from bokeh.plotting import Figure

import numpy as np


from ...exceptions import UnsupportedTypeError
from ...runtime import (CallbackBase, Communication, Publisher,
                        Subscription, SubscriptionCallback, TimerCallback)

logger = getLogger(__name__)


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


def apply_x_axis_offset(
    fig: Figure,
    x_range_name: str,
    min_ns: float,
    max_ns: float
) -> None:
    offset_s = min_ns*1.0e-9
    end_s = (max_ns-min_ns)*1.0e-9

    fig.extra_x_ranges = {x_range_name: Range1d(start=min_ns, end=max_ns)}

    xaxis = LinearAxis(x_range_name=x_range_name)
    xaxis.visible = False

    ticker = SingleIntervalTicker(interval=1, num_minor_ticks=10)
    fig.xaxis.ticker = ticker
    fig.add_layout(xaxis, 'below')

    fig.x_range = Range1d(start=0, end=end_s)

    fig.xgrid.minor_grid_line_color = 'black'
    fig.xgrid.minor_grid_line_alpha = 0.1

    fig.xaxis.major_label_overrides = {0: f'0+{offset_s}'}


def get_callback_param_desc(callback: CallbackBase):
    if isinstance(callback, TimerCallback):
        return f'period_ns: {callback.period_ns}'

    if isinstance(callback, SubscriptionCallback):
        return f'topic_name: {callback.subscribe_topic_name}'

    raise UnsupportedTypeError('callback type must be '
                               '[ TimerCallback/ SubscriptionCallback]')


PlotObjectTypes = Union[CallbackBase, Communication, Publisher, Subscription]


def get_range(
    plot_objects: Sequence[PlotObjectTypes]
) -> Tuple[int, int]:
    """
    Get measurement duration.

    Parameters
    ----------
    plot_objects: Sequence[Union[CallbackBase, Communication]]
        plot objects

    Returns
    -------
    Tuple[int, int]
        The timestamp of measurement start and measurement end

    """
    po_valid = [po for po in plot_objects if len(po.to_records()) > 0]
    if len(po_valid) == 0:
        logger.warning('Failed to found measurement results.')
        return 0, 1

    po_dfs = [po.to_dataframe(remove_dropped=True) for po in plot_objects]
    po_dfs_valid = [po_df for po_df in po_dfs if len(po_df) > 0]

    # NOTE:
    # The first column is system time for now.
    # The other columns could be other than system time.
    # Only the system time is picked out here.
    base_series = [df.iloc[:, 0] for df in po_dfs_valid]
    po_min = min(series.min() for series in base_series)
    po_max = max(series.max() for series in base_series)

    return po_min, po_max
