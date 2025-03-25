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

from collections.abc import Sequence

from ..common import ClockConverter
from ..record import Range
from ..runtime import CallbackBase, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path


def get_clock_converter(
    target_objects: Sequence[TimeSeriesTypes],
) -> ClockConverter:
    """
    Construct an instance to convert between simulation time and system time from time series data.

    Parameters
    ----------
    target_objects : Sequence[TimeSeriesTypes]
        TimeSeriesTypes target objects.

    Returns
    -------
    ClockConverter
        converter instance.

    """
    records_range = Range([to.to_records() for to in target_objects])
    frame_min, frame_max = records_range.get_range()
    if isinstance(target_objects[0], Communication):
        for comm in target_objects:
            assert isinstance(comm, Communication)
            if comm._callback_subscription:
                converter_cb = comm._callback_subscription
                break
        provider = converter_cb._provider
        converter = provider.get_sim_time_converter(frame_min, frame_max)
    elif isinstance(target_objects[0], Path):
        assert len(target_objects[0].child) > 0
        provider = target_objects[0].child[0]._provider  # type: ignore
        converter = provider.get_sim_time_converter(frame_min, frame_max)
    else:
        provider = target_objects[0]._provider
        converter = provider.get_sim_time_converter(frame_min, frame_max)

    return converter
