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

from .latency import Latency
from ..column import ColumnValue
from ..interface import RecordsInterface
from ..record_factory import RecordsFactory
from ...common import ClockConverter


class StackedBar:
    def __init__(
        self,
        records: RecordsInterface,
        converter: ClockConverter | None = None
    ) -> None:
        """
        Generate records for stacked bar.

        Parameters
        ----------
        records : RecordsInterface
            Records of response time.

        converter : ClockConverter | None, optional
            Converter to simulation time.

        Raises
        ------
        ValueError
            Error occurs if the records are empty.

        """
        # rename columns to nodes and topics granularity
        self._records = records
        self._first_latency_name = '[worst - best] response time'
        rename_map: dict[str, str] = \
            self._get_rename_column_map(self._records.columns)
        renamed_records: RecordsInterface = \
            self._rename_columns(self._records, rename_map)
        columns = list(rename_map.values())
        if len(columns) < 2:
            raise ValueError(f'Column size is {len(columns)} and must be more 2.')

        # add stacked bar data
        xlabel: str = 'start time'
        x_axis_values: RecordsInterface = \
            self._get_x_axis_values(renamed_records, columns[0], xlabel)
        stacked_bar_records = self._to_stacked_bar_records(renamed_records, columns)
        series_seq: Sequence[int | None] = x_axis_values.get_column_series(xlabel)
        if converter:
            series_seq = [round(converter.convert(float(t))) for t in series_seq if t is not None]
        series_list: list[int] = self._convert_sequence_to_list(series_seq)
        stacked_bar_records = \
            self._merge_column_series(
                stacked_bar_records,
                series_list,
                xlabel,
            )
        self._stacked_bar_records = stacked_bar_records
        self._columns = columns[:-1]

    @staticmethod
    def _rename_columns(
        records: RecordsInterface,
        rename_map: dict[str, str],
    ) -> RecordsInterface:
        """
        Rename columns of records.

        Parameters
        ----------
        records : RecordsInterface
            Target records.
        rename_map : dict[str, str]
            Names before and after changed.

        Returns
        -------
        RecordsInterface
            Renamed records

        """
        for before, after in rename_map.items():
            records.rename_columns({before: after})
        return records

    def _get_rename_column_map(
        self,
        raw_columns: list[str],
    ) -> dict[str, str]:
        """
        Generate rename map to visualize Node/Topic granularity.

        Parameters
        ----------
        raw_columns : list[str]
            Source columns.

        Returns
        -------
        dict[str, str]
            Names before and after changed.

        """
        rename_map: dict[str, str] = {}
        end_word: str = '_min'
        for column in raw_columns:
            if column.endswith(end_word):
                rename_map[column] = self._first_latency_name
            elif 'rclcpp_publish' in column:
                topic_name = column.split('/')[:-2]
                rename_map[column] = '/'.join(topic_name)
            elif 'callback_start' in column:
                node_name = column.split('/')[:-2]
                rename_map[column] = '/'.join(node_name)
            elif 'callback_end' in column:
                node_name = column.split('/')[:-2]
                rename_map[column] = '/'.join(node_name)
                rename_map[column] += '_end'

        return rename_map

    @staticmethod
    def _get_x_axis_values(
        records: RecordsInterface,
        column: str,
        xlabel: str,
    ) -> RecordsInterface:
        """
        Get x axis values.

        Parameters
        ----------
        records : RecordsInterface
            Target records.
        column : str
            Target column.
        xlabel : str
            Label name.

        Returns
        -------
        RecordsInterface
            Target column's records.

        """
        series = records.get_column_series(column)
        record_dict = [{xlabel: _} for _ in series]
        record: RecordsInterface = \
            RecordsFactory.create_instance(record_dict, columns=[ColumnValue(xlabel)])
        return record

    def _to_stacked_bar_records(
        self,
        records: RecordsInterface,
        columns: list[str],
    ) -> RecordsInterface:
        """
        Calculate stacked bar data.

        Parameters
        ----------
        records : RecordsInterface
            Target records.
        columns : list[str]
            Target columns (Node/Topic granularity).

        Returns
        -------
        RecordsInterface
            Stacked bar records.

        """
        output_records: RecordsInterface = RecordsFactory.create_instance()
        record_size = len(records.data)
        for column in columns[:-1]:
            output_records.append_column(ColumnValue(column), [])

        for column_from, column_to in zip(columns[:-1], columns[1:]):
            latency_handler = Latency(records, column_from, column_to)
            assert record_size == len(latency_handler.to_records())

            latency_records = latency_handler.to_records()
            latency_seq: Sequence[int | None] = latency_records.get_column_series('latency')
            latency_list: list[int] = self._convert_sequence_to_list(latency_seq)

            output_records = self._merge_column_series(output_records, latency_list, column_from)

        return output_records

    @staticmethod
    def _merge_column_series(
        records: RecordsInterface,
        series: list[int],
        column: str,
    ) -> RecordsInterface:
        """
        Append series to records.

        Parameters
        ----------
        records : RecordsInterface
            Source records.
        series : list[int]
            Data to append.
        column : str
            The column with appended data.

        Returns
        -------
        RecordsInterface
            Appended records.

        """
        record_dict = [{column: t} for t in series]

        if len(records.data) == 0:
            new_records: RecordsInterface = \
                RecordsFactory.create_instance(
                    record_dict, columns=[ColumnValue(column)])
            records.concat(new_records)
        else:
            records.append_column(ColumnValue(column), series)
        return records

    def to_dict(self) -> dict[str, list[int]]:
        """
        Get stacked bar dict data.

        Returns
        -------
        dict[str, list[int]]
            Stacked bar dict data.

        """
        return self._to_dict(self._stacked_bar_records)

    def _to_dict(self, records: RecordsInterface) -> dict[str, list[int]]:
        """
        Generate dict from records.

        Parameters
        ----------
        records : RecordsInterface
            Target records.

        Returns
        -------
        dict[str, list[int]]
            Dict generated from records.

        """
        columns = records.columns
        output_dict: dict[str, list[int]] = {}
        for column in columns:
            series_seq: Sequence[int | None] = records.get_column_series(column)
            series_list: list[int] = self._convert_sequence_to_list(series_seq)
            output_dict[column] = series_list
        return output_dict

    @staticmethod
    def _convert_sequence_to_list(
        seq: Sequence[int | None],
    ) -> list[int]:
        assert not any(x is None for x in seq)
        return [x for x in seq if x is not None]

    @property
    def columns(self) -> list[str]:
        return self._columns

    @property
    def records(self) -> RecordsInterface:
        return self._stacked_bar_records
