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

from ..interface import RecordsInterface
from ..record_factory import RecordsFactory
from ..record import ColumnValue
from .latency import Latency


from typing import Dict, List

class StackedBar:
    def __init__(
        self,
        records: RecordsInterface,
    ) -> None:
        """
        Generate records for stacked bar.

        Parameters
        ----------
        records : RecordsInterface
            Records of response time.

        Raises
        ------
        ValueError
            Error occurs if the records are empty.
        """

        # rename columns to nodes and topics granularity
        self._records = records
        self._diff_response_time_name = '[worst - best] response time'
        rename_map: Dict[str, str] = \
            self._get_rename_column_map(self._records.columns)
        renamed_records: RecordsInterface = \
            self._rename_columns(self._records, rename_map)
        columns = list(rename_map.values())
        if len(columns) < 2:
            raise ValueError(f'Column size is {len(columns)} and must be more 2.')

        # add stacked bar data
        xlabel: str = 'start time'
        x_axis_values: RecordsInterface = self._get_x_axis_values(renamed_records, self._diff_response_time_name, xlabel)
        stacked_bar_records = self._to_stacked_bar_records(renamed_records, columns)
        stacked_bar_records = \
            self._append_column_series(
                stacked_bar_records,
                x_axis_values.get_column_series(xlabel),
                xlabel,
            )
        self._stacked_bar_records = stacked_bar_records
        self._columns = columns[:-1]

    @staticmethod
    def _rename_columns(
        records: RecordsInterface,
        rename_map: Dict[str, str],
    ) -> RecordsInterface:
        """
        Rename columns of records.

        Parameters
        ----------
        records : RecordsInterface
            Target records.
        rename_map : Dict[str, str]
            Names before and after changed.

        Returns
        -------
        RecordsInterface
            Renamed records
        """

        for before, after in rename_map.items():
            records.rename_columns({before : after})
        return records

    def _get_rename_column_map(
        self,
        raw_columns: List[str],
    ) -> Dict[str, str]:
        """
        Generate rename map to visualize Node/Topic granularity.

        Parameters
        ----------
        raw_columns : List[str]
            Source columns.

        Returns
        -------
        Dict[str, str]
            Names before and after changed.
        """

        rename_map: Dict[str, str] = {}
        end_word: str = '_min'
        for column in raw_columns:
            if column.endswith(end_word):
                rename_map[column] = self._diff_response_time_name
            elif 'rclcpp_publish' in column:
                topic_name = column.split('/')[:-2]
                rename_map[column] = '/'.join(topic_name)
            elif 'callback_start' in column:
                node_name = column.split('/')[:-2]
                rename_map[column] = '/'.join(node_name)

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
        record_dict = [{xlabel : _ } for _ in series]
        record: RecordsInterface = RecordsFactory.create_instance(record_dict, [ColumnValue(xlabel)])
        return record

    def _to_stacked_bar_records(
        self,
        records: RecordsInterface,
        columns: List[str],
    ) -> RecordsInterface:
        """
        Caluculate stacked bar data.

        Parameters
        ----------
        records : RecordsInterface
            Target records.
        columns : List[str]
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
            latency = latency_records.get_column_series('latency')

            output_records = self._append_column_series(output_records, list(latency), column_from)

        return output_records

    @staticmethod
    def _append_column_series(
        records: RecordsInterface,
        series: List[int],
        column: str,
    ) -> RecordsInterface:
        """
        Append series to records.

        Parameters
        ----------
        records : RecordsInterface
            Source records.
        series : List[int]
            Data to append.
        column : str
            The column with appneded data.

        Returns
        -------
        RecordsInterface
            Appended records.
        """

        record_dict = [{column : t} for t in series]

        if len(records.data) == 0:
            new_records: RecordsInterface = \
                RecordsFactory.create_instance(record_dict, [ColumnValue(column)])
            records.concat(new_records)
        else:
            records.append_column(ColumnValue(column), series)
        return records

    def to_dict(self) -> Dict[str, List[int]]:
        """
        Get stacked bar dict data.

        Returns
        -------
        Dict[str, List[int]]
            Stacked bar dict data.
        """

        return self._to_dict(self._stacked_bar_records)

    @staticmethod
    def _to_dict(records: RecordsInterface) -> Dict[str, List[int]]:
        """
        Generate dict from records.

        Parameters
        ----------
        records : RecordsInterface
            Target records.

        Returns
        -------
        Dict[str, List[int]]
            Dict generated from records.

        """
        columns = records.columns
        output_dict: Dict[str, List[int]] = {}
        for column in columns:
            output_dict[column] = records.get_column_series(column)
        return output_dict

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def records(self) -> RecordsInterface:
        return self._stacked_bar_records
