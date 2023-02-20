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
from typing import List, Optional, Union

from bokeh.plotting import Figure

from .plot_facade import Plot
from ..runtime import (Application, CallbackGroup,
                       Executor, Node, Path)

logger = getLogger(__name__)

CallbackGroupTypes = Union[Application, Executor, Path, Node,
                           CallbackGroup, List[CallbackGroup]]


def callback_sched(
    target: CallbackGroupTypes,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
    coloring_rule: str = 'callback',
    use_sim_time: bool = False,
    export_path: Optional[str] = None
) -> Figure:
    logger.warning("The 'callback_sched' method is deprecated, "
                   "please use 'Plot.create_callback_scheduling_plot()' method.")

    plot = Plot.create_callback_scheduling_plot(target)
    xaxis_type = 'sim_time' if use_sim_time else 'system_time'
    p = plot.show(xaxis_type=xaxis_type, coloring_rule=coloring_rule)
    if export_path:
        plot.save(export_path=export_path, xaxis_type=xaxis_type, coloring_rule=coloring_rule)

    return p
