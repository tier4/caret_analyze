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

from abc import ABCMeta, abstractmethod
from typing import List, Union

from bokeh.plotting import Figure

from ...record import RecordsInterface
from ...runtime import (CallbackBase, Communication, Publisher, Subscription)

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class VisualizeLibInterface(metaclass=ABCMeta):

    @abstractmethod
    def timeseries(
        self,
        target_objects: List[TimeSeriesTypes],
        timeseries_records: List[RecordsInterface],
        xaxis_type: str,
        ywheel_zoom: bool,
        full_legends: bool
    ) -> Figure:
        raise NotImplementedError()
