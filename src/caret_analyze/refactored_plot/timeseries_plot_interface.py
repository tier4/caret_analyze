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
from typing import Collection, Union

from bokeh.plotting import Figure

import pandas as pd

from .plot_interface import PlotInterface
from .visualize_lib import VisualizeLib
from ..exceptions import UnsupportedTypeError
from ..record import RecordsInterface
from ..runtime import CallbackBase, Communication, Publisher, Subscription


TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class TimeSeriesPlotInterface(PlotInterface):

    def __init__(self, target_objects: Collection[TimeSeriesTypes]):
        self._target_objects = list(target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        self._validate_xaxis_type(xaxis_type)

        all_df = pd.DataFrame()
        for target_object in self._target_objects:
            records = self._to_records_per_object(target_object, xaxis_type)
            all_df = pd.concat([all_df, records.to_dataframe()], axis=1)

        return all_df

    def figure(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> Figure:
        self._validate_xaxis_type(xaxis_type)
        records_list = [self._to_records_per_object(obj, xaxis_type)
                        for obj in self._target_objects]

        return VisualizeLib.create_timeseries_plot(
            self._target_objects,
            records_list,
            xaxis_type,
            ywheel_zoom,
            full_legends
        )

    @abstractmethod
    def _to_records_per_object(
        self,
        target_object: TimeSeriesTypes,
        xaxis_type: str
    ) -> RecordsInterface:
        raise NotImplementedError()

    def _convert_records_to_sim_time(
        self,
        target_records: RecordsInterface
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
        ts_column_name = target_records.columns[0]
        for record in target_records:
            record.data[ts_column_name] = converter.convert(record.get(ts_column_name))

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )
