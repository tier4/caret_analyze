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

import pandas as pd

from ..record import ColumnValue, Range, RecordFactory, RecordsFactory, RecordsInterface
from ..runtime import CallbackBase, Communication, Path, Publisher, Subscription

TimeSeriesTypes = CallbackBase | Communication | (Publisher | Subscription) | Path


class MetricsBase(metaclass=ABCMeta):
    """Metics base class."""

    def __init__(
        self,
        target_objects: Sequence[TimeSeriesTypes],
    ) -> None:
        self._target_objects = list(target_objects)

    @property
    def target_objects(self) -> list[TimeSeriesTypes]:
        return self._target_objects

    @abstractmethod
    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def to_timeseries_records_list(
        self,
        xaxis_type: str = 'system_time'
    ) -> list[RecordsInterface]:
        raise NotImplementedError()

    # TODO: Migrate into records.
    def _convert_timeseries_records_to_sim_time(
        self,
        timeseries_records_list: list[RecordsInterface]
    ) -> list[RecordsInterface]:
        # get converter
        records_range = Range([to.to_records() for to in self.target_objects])
        frame_min, frame_max = records_range.get_range()
        if isinstance(self._target_objects[0], Communication):
            for comm in self._target_objects:
                assert isinstance(comm, Communication)
                if comm._callback_subscription:
                    converter_cb = comm._callback_subscription
                    break
            provider = converter_cb._provider
            converter = provider.get_sim_time_converter(frame_min, frame_max)
        elif isinstance(self._target_objects[0], Path):
            assert len(self._target_objects[0].child) > 0
            provider = self._target_objects[0].child[0]._provider
            converter = provider.get_sim_time_converter(frame_min, frame_max)
        else:
            provider = self._target_objects[0]._provider
            converter = provider.get_sim_time_converter(frame_min, frame_max)
        # convert
        converted_records_list: list[RecordsInterface] = []
        for records in timeseries_records_list:
            # TODO: Refactor after Records class supports quadrature operations.
            values = [
                RecordFactory.create_instance({
                    # NOTE: Loss of accuracy may be occurred with sim_time due to rounding process.
                    k: round(converter.convert(v))
                    for k, v
                    in record.data.items()
                })
                for record
                in records
            ]

            columns: list[ColumnValue] = \
                [ColumnValue(_) for _ in records.columns]

            converted_records_list.append(RecordsFactory.create_instance(values, columns=columns))

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
        elif isinstance(target_object, Path):
            column_name = f'{target_object.column_names[0]} to {target_object.column_names[-1]}'

        return pd.concat([target_df], keys=[column_name], axis=1)
