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
from typing import Optional

from bokeh.plotting import Figure

from .plot_facade import Plot

from ..runtime.path import Path

logger = getLogger(__name__)


def message_flow(
    path: Path,
    export_path: Optional[str] = None,
    granularity: Optional[str] = None,
    treat_drop_as_delay=False,
    lstrip_s: float = 0,
    rstrip_s: float = 0,
    use_sim_time: bool = False
) -> Figure:
    logger.warning("The 'message_flow' method is deprecated, "
                   "please use 'Plot.create_message_flow_plot()' method.")

    plot = Plot.create_message_flow_plot(
        path, granularity, treat_drop_as_delay, lstrip_s, rstrip_s)
    xaxis_type = 'sim_time' if use_sim_time else 'system_time'
    p = plot.figure(xaxis_type=xaxis_type)
    if export_path:
        plot.save(export_path=export_path, xaxis_type=xaxis_type)

    return p
