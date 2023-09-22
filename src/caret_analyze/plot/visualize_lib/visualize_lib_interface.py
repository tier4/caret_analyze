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
from collections.abc import Sequence

from bokeh.plotting import Figure

from ..metrics_base import MetricsBase
from ...record import Frequency, Latency, Period
from ...runtime import CallbackBase, CallbackGroup, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription)
MetricsTypes = Frequency | Latency | Period
HistTypes = CallbackBase | Communication


class VisualizeLibInterface(metaclass=ABCMeta):
    """Interface class for VisualizeLib."""

    @abstractmethod
    def response_time_hist(
        self,
        target_paths: Sequence[Path],
        case: str,
        binsize_ns: int,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
    ) -> Figure:
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def callback_scheduling(
        self,
        callback_groups: Sequence[CallbackGroup],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        coloring_rule: str,
        lstrip_s: float,
        rstrip_s: float
    ) -> Figure:
        raise NotImplementedError()

    @abstractmethod
    def timeseries(
        self,
        metrics: MetricsBase,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str
    ) -> Figure:
        raise NotImplementedError()

    @abstractmethod
    def stacked_bar(
        self,
        metrics,
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool,
        case: str,
    ) -> Figure:
        raise NotImplementedError()

    def histogram(
        self,
        metrics: list[MetricsTypes],
        target_objects: Sequence[HistTypes],
        data_type: str
    ) -> Figure:
        raise NotImplementedError()
