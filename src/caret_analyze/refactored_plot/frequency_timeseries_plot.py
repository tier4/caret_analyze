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

from typing import Collection, Union

from .timeseries_plot_interface import TimeSeriesPlotInterface
from ..record import Frequency, RecordsInterface
from ..runtime import CallbackBase, Communication, Publisher, Subscription


TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class FrequencyTimeSeriesPlot(TimeSeriesPlotInterface):

    def __init__(self, target_objects: Collection[TimeSeriesTypes]) -> None:
        super().__init__(target_objects)

    def _to_records_per_object(
        self,
        target_object: TimeSeriesTypes,
        xaxis_type: str
    ) -> RecordsInterface:
        frequency = Frequency(target_object.to_records())
        frequency_records = frequency.to_records()

        if xaxis_type == 'sim_time':
            self._convert_records_to_sim_time(frequency_records)

        return frequency_records
