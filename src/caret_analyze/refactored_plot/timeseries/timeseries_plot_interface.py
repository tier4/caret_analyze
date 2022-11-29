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

from abc import abstractmethod
from typing import List, Sequence, Union

from bokeh.plotting import Figure

from ..plot_interface import PlotInterface
from ..visualize_lib import VisualizeInterface
from ...exceptions import UnsupportedTypeError
from ...record import RecordsInterface
from ...runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class TimeSeriesInterface(PlotInterface):

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes],
        visualize_lib: VisualizeInterface
    ) -> None:
        self._target_objects = list(target_objects)
        self._visualize_lib = visualize_lib

    @abstractmethod
    def _create_timeseries_records(self) -> List[RecordsInterface]:
        raise NotImplementedError()

    def figure(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> Figure:
        self._validate_xaxis_type(xaxis_type)
        timeseries_records_list = self._create_timeseries_records()
        if xaxis_type == 'sim_time':
            self._convert_timeseries_records_to_sim_time(timeseries_records_list)

        return self._visualize_lib.timeseries(
            self._target_objects,
            timeseries_records_list,
            xaxis_type,
            ywheel_zoom,
            full_legends
        )

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

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
