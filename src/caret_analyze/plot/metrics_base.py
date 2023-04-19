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

from ..record import ColumnValue, RecordFactory, RecordsFactory, RecordsInterface
from ..runtime import CallbackBase, Communication, Publisher, Subscription

TimeSeriesTypes = Union[CallbackBase, Communication, Union[Publisher, Subscription]]


class MetricsBase(metaclass=ABCMeta):
    """Metics base class."""

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

    # TODO: Migrate into records.
    def _convert_timeseries_records_to_sim_time(
        self,
        timeseries_records_list: List[RecordsInterface]
    ) -> List[RecordsInterface]:
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
        converted_records_list: List[RecordsInterface] = []
        ts_column_name = timeseries_records_list[0].columns[0]
        for records in timeseries_records_list:
            # TODO: Refactor after Records class supports quadrature operations.
            values = [
                RecordFactory.create_instance({
                    # NOTE: Loss of accuracy may be occurred with sim_time due to rounding process.'
                    # TODO: 'tid' is magic number
                    k: v if k == 'tid' else round(converter.convert(record.get(k)))
                    for k, v
                    in record.data.items()
                })
                for record
                in records
            ]

            columns = [
                ColumnValue(column)
                for column
                in values[0].columns
            ]

            converted_records_list.append(RecordsFactory.create_instance(values, columns))

        return converted_records_list

    # TODO: Multi-column DataFrame are difficult for users to handle,
    #       so this function is unnecessary.
    @staticmethod
    def _get_ts_column_name(
        target_object: TimeSeriesTypes
    ) -> str:
        if isinstance(target_object, Publisher):
            callback_names = (f'{target_object.callback_names[0]}/'
                              if target_object.callback_names else '')
            ts_column_name = f'{callback_names}rclcpp_publish_timestamp'
        elif isinstance(target_object, Subscription):
            ts_column_name = f'{target_object.column_names[0]}'
        else:
            ts_column_name = target_object.column_names[0].split('/')[-1]

        return ts_column_name + ' [ns]'

    # TODO: Multi-column DataFrame are difficult for users to handle,
    #       so this function is unnecessary.
    @staticmethod
    def _add_top_level_column(
        target_df: pd.DataFrame,
        target_object: TimeSeriesTypes
    ) -> pd.DataFrame:
        if isinstance(target_object, CallbackBase):
            column_name = target_object.callback_name
        elif isinstance(target_object, Communication):
            column_name = (f'{target_object.publish_node_name}|'
                           f'{target_object.topic_name}|'
                           f'{target_object.subscribe_node_name}')
        elif isinstance(target_object, (Publisher, Subscription)):
            column_name = target_object.topic_name

        return pd.concat([target_df], keys=[column_name], axis=1)
