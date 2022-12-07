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
from typing import List, Sequence, Union

import pandas as pd

from ..record import RecordsInterface
from ..runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class MetricsBase(metaclass=ABCMeta):

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes],
    ) -> None:
        self._target_objects = list(target_objects)

    @property
    def target_objects(self) -> List[TimeSeriesTypes]:
        return self._target_objects

    @abstractmethod
    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> List[RecordsInterface]:
        raise NotImplementedError()

    def _convert_timeseries_records_to_sim_time(
        self,
        timeseries_records_list: List[RecordsInterface]
    ) -> None:
        # get converter
        if isinstance(self._target_objects[0], Communication):
            for comm in self._target_objects:
                assert isinstance(comm, Communication)
                if comm._callback_subscription:
                    converter_cb = comm._callback_subscription
                    break
            converter = converter_cb._provider.get_sim_time_converter()
        else:
            converter = self._target_objects[0]._provider.get_sim_time_converter()

        # convert
        ts_column_name = timeseries_records_list[0].columns[0]
        for records in timeseries_records_list:
            for record in records:
                record.data[ts_column_name] = converter.convert(record.get(ts_column_name))
