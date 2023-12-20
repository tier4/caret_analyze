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

from collections.abc import Iterator, Sequence

import math
from warnings import warn

import numpy as np

from ..column import ColumnValue
from ..interface import RecordInterface, RecordsInterface
from ..record_factory import RecordFactory, RecordsFactory
from ...common import ClockConverter
from ...exceptions import InvalidRecordsError


class TimeRange:
    """Class that holds records and minimum values."""

    def __init__(
        self,
        min_value: int,
        record: RecordInterface,
        input_column: str
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        min_value : int
            minimum value.
        record : RecordInterface
            target record.
        input_column : str
            input column (first column).

        """
        self._min = min_value
        self._record = record
        self._column = input_column

    @property
    def max_value(self) -> int:
        """
        Get maximum value.

        Returns
        -------
        int
            maximum value.

        """
        return self._record.get(self._column)

    @property
    def min_value(self) -> int:
        """
        Get minimum value.

        Returns
        -------
        int
            minimum value.

        """
        return self._min

    def update(self, record: RecordInterface) -> None:
        """
        Update range.

        Parameters
        ----------
        record : RecordInterface
            record to apply.
            latest record is valid.

        """
        if self.max_value < record.get(self._column):
            self._record = record

    @property
    def record(self) -> RecordInterface:
        return self._record


class ResponseMap():
    """Class that manages the dictionary type that is the source of response time calculation."""

    def __init__(
        self,
        records: RecordsInterface,
        columns: list[str]
    ):
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        columns: list[str]
            List of column names to be used.
            first column name is used as input.
            last column name is used as output.

        """
        self._columns = columns
        new_data = {}

        input_min_time = None

        for data in records.data:
            if not set(self._columns) <= set(data.columns):
                continue

            input_time, output_time = data.get(self.input_column), data.get(self.output_column)

            if input_min_time is None:
                input_min_time = input_time

            if output_time not in new_data:
                new_data[output_time] = TimeRange(input_min_time, data, self.input_column)
            else:
                new_data[output_time].update(data)

        self._data = new_data

    def sorted_iter(self) -> Iterator[int]:
        """
        Get iterator which returns output time in ascending order.

        Yields
        ------
        Iterator[int]
            iterator which returns output time.

        """
        return iter(sorted(self._data))

    def __len__(self) -> int:
        """
        Get number of output time.

        Returns
        -------
        int
            number of output time. It is same as number of TimeRange.

        """
        return len(self._data)

    def at(self, end_time: int) -> TimeRange:
        """
        Get TimeRange.

        Parameters
        ----------
        end_time : int
            output time to get.

        Returns
        -------
        TimeRange
            TimeRange that matches the output time.

        """
        return self._data[end_time]

    @property
    def input_column(self) -> str:
        """
        Get input column name.

        Returns
        -------
        str
            input column name.

        """
        return self._columns[0]

    @property
    def output_column(self) -> str:
        """
        Get output column name.

        Returns
        -------
        str
            output column name.

        """
        return self._columns[-1]

    @property
    def columns(self) -> list[str]:
        return self._columns


# NOTE: Rename ResponseMap after refactoring
class ResponseMapAll:

    def __init__(
        self,
        records: RecordsInterface,
        columns: list[str]
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate latency.
        columns : list[str] | None, optional
            Column name of start timestamps used in the calculation, by default None
            If None, the first column of records is selected.

        """
        if columns:
            self._start_column = columns[0]
            self._end_column = columns[-1]
            self._columns = columns
        else:
            self._start_column = records.columns[0]
            self._end_column = records.columns[-1]
            self._columns = records.columns

        self._start_timestamps: list[int] = []
        self._end_timestamps: list[int] = []
        self._records: list[RecordInterface] = []

        for record in reversed(records.data):
            if self._end_column in record.columns:
                end_ts = record.get(self._end_column)
            elif len(self._end_timestamps) > 0:
                end_ts = min(self._end_timestamps)
            else:
                continue

            if self._start_column not in record.columns:
                continue
            start_ts = record.get(self._start_column)

            if end_ts < start_ts:
                warn('Record data is invalid. '
                     'The end time of the path is recorded before the start time.',
                     UserWarning)
                continue

            if start_ts not in self._start_timestamps:
                self._start_timestamps.insert(0, start_ts)
                self._end_timestamps.insert(0, end_ts)
                self._records.insert(0, record)
            else:
                idx = self._start_timestamps.index(start_ts)
                if end_ts < self._end_timestamps[idx]:
                    self._end_timestamps[idx] = end_ts
                    self._records[idx] = record

    def to_worst_with_external_latency_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:

        end_timestamps: list[int] = []
        start_timestamps: list[int] = []
        worst_to_best_timestamps: list[int] = []
        for start_ts, end_ts, prev_start_ts in zip(self._start_timestamps[1:],
                                                   self._end_timestamps[1:],
                                                   self._start_timestamps[:-1]):
            if end_ts not in end_timestamps:
                start_timestamps.append(start_ts)
                end_timestamps.append(end_ts)
                worst_to_best_timestamps.append(start_ts - prev_start_ts)
            else:
                idx = end_timestamps.index(end_ts)
                if start_ts < start_timestamps[idx]:
                    start_timestamps[idx] = start_ts
                    worst_to_best_timestamps[idx] = start_ts - prev_start_ts

        records = self._create_empty_records()
        for start_ts, end_ts, worst_to_best_ts in sorted(zip(start_timestamps,
                                                             end_timestamps,
                                                             worst_to_best_timestamps),
                                                         key=lambda x: x[0]):
            if converter:
                record = {
                    self._start_column: round(converter.convert(start_ts - worst_to_best_ts)),
                    'response_time': end_ts - (start_ts - worst_to_best_ts)
                }
            else:
                record = {
                    self._start_column: start_ts - worst_to_best_ts,
                    'response_time': end_ts - (start_ts - worst_to_best_ts)
                }
            records.append(record)

        return records

    def to_best_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:

        end_timestamps: list[int] = []
        start_timestamps: list[int] = []
        for start_ts, end_ts in zip(self._start_timestamps, self._end_timestamps):

            if end_ts not in end_timestamps:
                start_timestamps.append(start_ts)
                end_timestamps.append(end_ts)
            else:
                idx = end_timestamps.index(end_ts)
                if start_ts > start_timestamps[idx]:
                    start_timestamps[idx] = start_ts

        records = self._create_empty_records()
        for start_ts, end_ts in sorted(zip(start_timestamps, end_timestamps), key=lambda x: x[0]):
            if converter:
                record = {
                    self._start_column: round(converter.convert(start_ts)),
                    'response_time': end_ts - start_ts
                }
            else:
                record = {
                    self._start_column: start_ts,
                    'response_time': end_ts - start_ts
                }
            records.append(record)

        return records

    def to_all_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        records = self._create_empty_records()

        for start_ts, end_ts in zip(self._start_timestamps, self._end_timestamps):
            if converter:
                record = {
                    self._start_column: round(converter.convert(start_ts)),
                    'response_time': end_ts - start_ts
                }
            else:
                record = {
                    self._start_column: start_ts,
                    'response_time': end_ts - start_ts
                }
            records.append(record)

        return records

    def to_worst_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        end_timestamps: list[int] = []
        start_timestamps: list[int] = []
        for start_ts, end_ts in zip(self._start_timestamps, self._end_timestamps):
            if end_ts not in end_timestamps:
                start_timestamps.append(start_ts)
                end_timestamps.append(end_ts)
            else:
                idx = end_timestamps.index(end_ts)
                if start_ts < start_timestamps[idx]:
                    start_timestamps[idx] = start_ts

        records = self._create_empty_records()
        for start_ts, end_ts in sorted(zip(start_timestamps, end_timestamps), key=lambda x: x[0]):
            if converter:
                record = {
                    self._start_column: round(converter.convert(start_ts)),
                    'response_time': end_ts - start_ts
                }
            else:
                record = {
                    self._start_column: start_ts,
                    'response_time': end_ts - start_ts
                }
            records.append(record)

        return records

    def to_all_stacked_bar(self) -> RecordsInterface:
        end_column_record_dict: dict[int, list[RecordInterface]] = {}

        # classify records which have same end timestamp
        end_ts = None
        for record in reversed(self._records):
            if self._end_column in record.columns:
                end_ts = record.get(self._end_column)
            elif end_ts is None:
                continue

            if end_ts in end_column_record_dict.keys():
                end_column_record_dict[end_ts].insert(0, record)
            else:
                end_column_record_dict[end_ts] = [record]

        # fill empty data
        filled_records_dict: dict[int, list[RecordInterface]] = {}
        # for each records which have same end timestamp
        for end_ts, record_list in end_column_record_dict.items():
            filled_records_dict[end_ts] = []

            for record in reversed(record_list):
                # if record doesn't have some timestamps,
                # record timestamps just after
                for column in self._columns:
                    if column in record.columns:
                        continue
                    timestamps = [record.get(column) for record in filled_records_dict[end_ts]
                                  if column in record.columns]
                    record.add(column, min(timestamps))
                record.drop_columns(list(record.columns - set(self._columns)))
                filled_records_dict[end_ts].append(record)

        columns = [ColumnValue(c) for c in self._columns]
        init = [_.data for _ in sum(filled_records_dict.values(), [])]
        stacked_bar_records: RecordsInterface =\
            RecordsFactory.create_instance(init,
                                           columns=columns)
        stacked_bar_records.sort_column_order()
        return stacked_bar_records

    def to_worst_case_stacked_bar(self) -> RecordsInterface:
        end_column_record_dict: dict[int, list[RecordInterface]] = {}

        # classify records which have same end timestamp
        end_ts = None
        for record in reversed(self._records):
            if self._end_column in record.columns:
                end_ts = record.get(self._end_column)
            elif end_ts is None:
                continue

            if end_ts in end_column_record_dict.keys():
                end_column_record_dict[end_ts].append(record)
            else:
                end_column_record_dict[end_ts] = [record]

        # generate worst-case work flow
        filled_record_list: list[RecordInterface] = []
        for record_list in end_column_record_dict.values():
            worst_record = RecordFactory.create_instance()

            for column in self._columns:
                timestamps = [r.get(column) for r in record_list if column in r.columns]
                worst_record.add(column, min(timestamps))
            worst_record.drop_columns(list(worst_record.columns - set(self._columns)))
            filled_record_list.append(worst_record)

        columns = [ColumnValue(c) for c in self._columns]
        init = [_.data for _ in filled_record_list]
        stacked_bar_records: RecordsInterface =\
            RecordsFactory.create_instance(init,
                                           columns=columns)
        stacked_bar_records.sort_column_order()
        return stacked_bar_records

    def _create_empty_records(
        self
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(None, columns=[
            ColumnValue(self._start_column), ColumnValue('response_time')
        ])


class ResponseTime:
    """
    Class which calculates response time.

    Examples
    --------
    >>> from caret_analyze import Application, Architecture, Lttng
    >>> from caret_analyze.record import ResponseTime

    >>> # Load results
    >>> arch = Architecture('yaml', '/path/to/yaml')
    >>> lttng = Lttng('/path/to/ctf')
    >>> app = Application(arch, lttng)

    >>> # Select target instance
    >>> node = app.get_node('node_name')
    >>> callback = node.callbacks[0]
    >>> callback.summary.pprint()

    >>> # Calculate response time
    >>> records = callback.to_records()
    >>> response = ResponseTime(records)
    >>> response_time_records = response.to_best_case_records()
    >>> response_df = response_time_records.to_dataframe()

    >>> path = app.get_path('path_name')
    >>> records = path.to_records()
    >>> response = ResponseTime(records)
    >>> response_time_records = response.to_best_case_records()
    >>> response_df = response_time_records.to_dataframe()

    """

    def __init__(
        self,
        records: RecordsInterface,
        *,
        columns: list[str] | None = None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        columns : str | None
            List of column names to be used in return value.
            If None, only first and last columns are used.

        """
        columns = columns or [records.columns[0], records.columns[-1]]
        response_map = ResponseMap(records, columns)
        self._response_map_all = ResponseMapAll(records, columns)
        self._records = ResponseRecords(response_map)
        self._timeseries = ResponseTimeseries(self._records)
        self._histogram = ResponseHistogram(self._records, self._timeseries)

    def to_all_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        """
        Calculate the data of all records for response time.

        This represents the response time for all cases
        of message flows with the same output.

        Returns
        -------
        RecordsInterface
            Records of the all response time.
        converter : ClockConverter | None, optional
            Converter to simulation time.

        Parameters
        ----------
        converter : ClockConverter | None, optional
            Converter to simulation time.

            Columns
            - {columns[0]}
            - {'response_time'}

        """
        return self._response_map_all.to_all_records(converter=converter)

    def to_worst_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        """
        Calculate data of the worst case records for response time.

        This represents the response time for the oldest case
        in the message flow with the same output.

        Parameters
        ----------
        converter : ClockConverter | None, optional
            Converter to simulation time.

        Returns
        -------
        RecordsInterface
            Records of the worst cases response time.

            Columns
            - {columns[0]}
            - {'response_time'}

        """
        return self._response_map_all.to_worst_case_records(converter=converter)

    def to_best_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        """
        Calculate data of the best case records for response time.

        This represents the response time for the newest case
        in the message flow with the same output.

        Parameters
        ----------
        converter : ClockConverter | None, optional
            Converter to simulation time.

        Returns
        -------
        RecordsInterface
            Records of the best cases response time.

            Columns
            - {columns[0]}
            - {'response_time'}

        """
        return self._response_map_all.to_best_case_records(converter=converter)

    def to_worst_with_external_latency_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        """
        Calculate data of the worst-with-external-latency case records for response time.

        This represents the response time for the oldest case
        in the message flow with the same output
        as well as delays caused by various factors such as lost messages.

        Parameters
        ----------
        converter : ClockConverter | None, optional
            Converter to simulation time.

        Returns
        -------
        RecordsInterface
            Records of the worst-with-external-latency cases response time.

            Columns
            - {columns[0]}
            - {'response_time'}

        """
        return self._response_map_all.to_worst_with_external_latency_case_records(
            converter=converter
        )

    def to_all_stacked_bar(self) -> RecordsInterface:
        """
        Calculate records for stacked bar.

        Returns
        -------
        RecordsInterface
            Records of the all response time.

            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        return self._response_map_all.to_all_stacked_bar()

    def to_worst_case_stacked_bar(self) -> RecordsInterface:
        """
        Calculate records for stacked bar.

        Returns
        -------
        RecordsInterface
            Records of the worst-with-external-latency cases response time.

            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        return self._response_map_all.to_worst_case_stacked_bar()

    def to_best_case_stacked_bar(self) -> RecordsInterface:
        """
        Calculate records for stacked bar.

        Returns
        -------
        RecordsInterface
            Records of the best cases response time.

            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        return self._records.to_range_records('best')

    def to_worst_with_external_latency_case_stacked_bar(self) -> RecordsInterface:
        """
        Calculate records for stacked bar.

        Returns
        -------
        RecordsInterface
            The best and worst cases are separated into separate columns.

            Columns
            - {columns[0]}_min
            - {columns[0]}_max
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        return self._records.to_range_records('worst-with-external-latency')

    def to_best_case_timeseries(self) -> tuple[np.ndarray, np.ndarray]:
        warn('This API will be moved to the Plot package in the near future.', DeprecationWarning)
        """
        Calculate the best-case time series data for response time.

        The best case for response time are included message flow latency.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            input time[ns], latency[ns]

        """
        return self._timeseries.to_best_case_timeseries()

    def to_worst_case_timeseries(self) -> tuple[np.ndarray, np.ndarray]:
        warn('This API will be moved to the Plot package in the near future.', DeprecationWarning)
        """
        Calculate the worst-case time series data for response time.

        The worst case in response time includes message flow latencies
        as well as delays caused by various factors such as lost messages.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            input time[ns], latency[ns]

        """
        return self._timeseries.to_worst_case_timeseries()

    def to_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate response time histogram.

        Parameters
        ----------
        binsize_ns : int, optional
            binsize [ns], by default 1000000
        density : bool, optional
            If False, the result will contain the number of samples in each bin.
            If True, the result is the value of the probability density function at the bin,
            normalized such that the integral over the range is 1.
            Note that the sum of the histogram values will not be equal to 1
            unless bins of unity width are chosen; it is not a probability mass function.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_histogram(binsize_ns, density)

    def to_best_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate the best-case histogram for response time.

        The best case for response time are included message flow latency.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_best_case_histogram(binsize_ns, density)

    def to_worst_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate the worst-case histogram for response time.

        The worst case in response time includes message flow latencies
        as well as delays caused by various factors such as lost messages.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_worst_case_histogram(binsize_ns, density)


class ResponseRecords:

    def __init__(
        self,
        response_map: ResponseMap,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        response_map : ResponseMap
            response map for calculating response time.

        """
        self._response_map = response_map

    def to_range_records(
        self,
        case: str = 'worst-with-external-latency',
    ) -> RecordsInterface:
        """
        Calculate response time records.

        Returns
        -------
        RecordsInterface
            The best and worst-with-external-latency cases are separated into separate columns.
            Columns
            - {columns[0]}_min
            - {columns[0]}_max
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        columns = []
        if case == 'best':
            columns = [ColumnValue(self._columns[0])]
        else:  # worst
            columns = [
                ColumnValue(f'{self._input_column}_min'),
                ColumnValue(f'{self._input_column}_max'),
            ]

        columns += [ColumnValue(column) for column in self._columns[1:]]

        records = self._create_empty_records(columns)

        def add_records(
            input_time_min: int,
            record: RecordInterface
        ):
            if case == 'best':
                record = self._create_best_case_record(
                    record, self._columns
                )
            else:  # worst
                record = self._create_worst_to_best_case_record(
                    record, self._columns, input_time_min
                )

            records.append(record)

        self._create_response_records_core(add_records)

        records.sort_column_order()

        return records

    def to_best_case_stacked_bar(self) -> RecordsInterface:
        """
        Calculate best case response time records for stacked bar.

        Returns
        -------
        RecordsInterface
            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        records = self._create_empty_records()

        def add_records(
            _: int,
            record: RecordInterface
        ):
            record_best_case = self._create_best_case_record(record, self._columns)
            records.append(record_best_case)

        self._create_response_records_core(add_records)

        records.sort_column_order()

        return records

    def to_worst_case_stacked_bar(self) -> RecordsInterface:
        """
        Calculate worst case response time records for stacked bar.

        Returns
        -------
        RecordsInterface
            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        records = self._create_empty_records()

        def add_records(
            input_time_min: int,
            record: RecordInterface
        ):
            record_worst_case = self._create_worst_case_record(
                record, self._columns, input_time_min)
            records.append(record_worst_case)

        self._create_response_records_core(add_records)

        records.sort_column_order()

        return records

    @property
    def _input_column(self):
        return self._response_map.input_column

    @property
    def _output_column(self):
        return self._response_map.output_column

    @property
    def _columns(self) -> list[str]:
        return self._response_map.columns

    def _create_empty_records(
        self,
        columns: Sequence[ColumnValue] | None = None
    ) -> RecordsInterface:
        columns = columns or [ColumnValue(column) for column in self._columns]
        return RecordsFactory.create_instance(
            None,
            columns=columns
        )

    def _create_all_pattern_records(self) -> RecordsInterface:
        records = self._create_empty_records()

        for output_time in self._response_map.sorted_iter():
            input_time_range = self._response_map.at(output_time)
            record = input_time_range.record

            record_worst_case = self._create_worst_case_record(
                record, self._columns, input_time_range.min_value)
            records.append(record_worst_case)

            if input_time_range.min_value == input_time_range.max_value:
                continue

            record_best_case = self._create_best_case_record(record, self._columns)
            records.append(record_best_case)

        records.sort_column_order()

        return records

    def _create_response_records(self) -> RecordsInterface:
        records = self._create_empty_records()

        def add_records(
            input_time_min: int,
            record: RecordInterface
        ) -> None:
            record_worst_case = self._create_worst_case_record(
                record, self._columns, input_time_min)
            records.append(record_worst_case)

            record_best_case = self._create_best_case_record(record, self._columns)
            records.append(record_best_case)

        self._create_response_records_core(add_records)

        records.sort_column_order()

        return records

    def _create_response_records_core(self, callback) -> RecordsInterface:
        records = self._create_empty_records()

        input_max_time_ = 0

        for output_time in self._response_map.sorted_iter():
            input_time_range = self._response_map.at(output_time)

            input_min_time = max(input_max_time_, input_time_range.min_value)
            input_max_time = max(input_max_time_, input_time_range.max_value)

            # store max_value in previous iteration
            input_max_time_ = input_time_range.max_value

            if input_min_time == input_max_time:
                continue

            callback(input_min_time, input_time_range.record)

        records.sort_column_order()

        return records

    @staticmethod
    def _create_best_case_record(
        record: RecordInterface,
        columns: list[str]
    ) -> RecordInterface:
        record_dict: dict[str, int] = {}
        for column in columns:
            record_dict[column] = record.get(column)
        return RecordFactory.create_instance(record_dict)

    @staticmethod
    def _create_worst_case_record(
        record: RecordInterface,
        columns: list[str],
        input_time_min: int
    ) -> RecordInterface:
        record_dict: dict[str, int] = {}

        record_dict[columns[0]] = input_time_min
        for column in columns[1:]:
            record_dict[column] = record.get(column)
        return RecordFactory.create_instance(record_dict)

    @staticmethod
    def _create_worst_to_best_case_record(
        record: RecordInterface,
        columns: list[str],
        input_time_min: int
    ) -> RecordInterface:
        record_dict: dict[str, int] = {}

        input_min_column = f'{columns[0]}_min'
        input_max_column = f'{columns[0]}_max'
        record_dict[input_min_column] = input_time_min
        record_dict[input_max_column] = record.get(columns[0])
        for column in columns[1:]:
            record_dict[column] = record.get(column)
        return RecordFactory.create_instance(record_dict)


class ResponseTimeseries:

    def __init__(
        self,
        response_records: ResponseRecords
    ) -> None:
        self._records = response_records

    def to_best_case_timeseries(self):
        records = self._records.to_range_records()
        input_column = records.columns[1]
        output_column = records.columns[-1]
        return self._to_timeseries(input_column, output_column)

    def to_worst_case_timeseries(self):
        records = self._records.to_range_records()
        input_column = records.columns[0]
        output_column = records.columns[-1]
        return self._to_timeseries(input_column, output_column)

    def _to_timeseries(self, input_column, output_column):
        records = self._records.to_range_records()

        t_ = records.get_column_series(input_column)
        t_in = np.array(t_, dtype=np.int64)

        t_out_ = records.get_column_series(output_column)
        t_out = np.array(t_out_, dtype=np.int64)

        latency = t_out - t_in

        return t_in, latency

    def to_best_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        records = self._records.to_range_records()
        input_column = records.columns[1]
        output_column = records.columns[-1]
        return self._to_records(input_column, output_column, converter)

    def to_worst_with_external_latency_case_records(
        self,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        records = self._records.to_range_records()
        input_column = records.columns[0]
        output_column = records.columns[-1]
        return self._to_records(input_column, output_column, converter)

    def _to_records(
        self,
        input_column: str,
        output_column: str,
        converter: ClockConverter | None = None
    ) -> RecordsInterface:
        records: RecordsInterface = self._create_empty_records(input_column)

        range_records = self._records.to_range_records()
        t_in = range_records.get_column_series(input_column)
        t_out = range_records.get_column_series(output_column)

        for start_ts, end_ts in zip(t_in, t_out):
            if start_ts is None or end_ts is None:
                continue
            if converter:
                start_ts = round(converter.convert(start_ts))
                end_ts = round(converter.convert(end_ts))
            record = {
                input_column: start_ts,
                'response_time': end_ts - start_ts
            }
            records.append(record)

        return records

    def _create_empty_records(
        self,
        input_column: str
    ) -> RecordsInterface:
        return RecordsFactory.create_instance(None, columns=[
            ColumnValue(input_column), ColumnValue('response_time')
        ])


class ResponseHistogram:
    """Class that calculates response time histogram."""

    def __init__(
        self,
        response_records: ResponseRecords,
        response_timeseries: ResponseTimeseries
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        response_records : ResponseRecords
            records for calculating histogram.
        response_timeseries: ResponseTimeseries
            response time series

        """
        self._response_records = response_records
        self._timeseries = response_timeseries

    def to_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Calculate histogram.

        Parameters
        ----------
        binsize_ns : int, optional
            binsize [ns], by default 1000000
        density : bool, optional
            If False, the result will contain the number of samples in each bin.
            If True, the result is the value of the probability density function at the bin,
            normalized such that the integral over the range is 1.
            Note that the sum of the histogram values will not be equal to 1
            unless bins of unity width are chosen; it is not a probability mass function.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        assert binsize_ns > 0

        records = self._response_records.to_range_records()

        input_min_column = records.columns[0]
        input_max_column = records.columns[1]
        output_column = records.columns[2]

        latency_ns = []

        def to_bin_sized(num):
            return (num // binsize_ns) * binsize_ns

        # Note: need to speed up.
        for record in records:
            output_time = record.get(output_column)
            input_time_min = record.get(input_min_column)
            input_time_max = record.get(input_max_column)
            bin_sized_latency_min = to_bin_sized(output_time - input_time_max)

            for input_time in range(input_time_min, input_time_max + binsize_ns, binsize_ns):
                bin_sized_latency = to_bin_sized(output_time - input_time)
                if bin_sized_latency < bin_sized_latency_min:
                    break
                latency_ns.append(bin_sized_latency)

        return self._to_histogram(latency_ns, binsize_ns, density)

    def to_best_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Return the histogram generated by only the best case of response time.

        Parameters
        ----------
        binsize_ns : int, optional
            _description_, by default 1000000
        density : bool, optional
            If False, the result will contain the number of samples in each bin.
            If True, the result is the value of the probability density function at the bin,
            normalized such that the integral over the range is 1.
            Note that the sum of the histogram values will not be equal to 1
            unless bins of unity width are chosen; it is not a probability mass function.
            Default value is False.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        latency_ns = self._timeseries.to_best_case_records().get_column_series('response_time')
        return self._to_histogram([_ for _ in latency_ns if _ is not None], binsize_ns, density)

    def to_worst_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> tuple[np.ndarray, np.ndarray]:
        """
        Return the histogram generated by only the worst case of response time.

        Parameters
        ----------
        binsize_ns : int, optional
            _description_, by default 1000000
        density : bool, optional
            If False, the result will contain the number of samples in each bin.
            If True, the result is the value of the probability density function at the bin,
            normalized such that the integral over the range is 1.
            Note that the sum of the histogram values will not be equal to 1
            unless bins of unity width are chosen; it is not a probability mass function.
            Default value is False.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        latency_ns =\
            self._timeseries.to_worst_with_external_latency_case_records()\
                .get_column_series('response_time')
        return self._to_histogram([_ for _ in latency_ns if _ is not None], binsize_ns, density)

    @staticmethod
    def _to_histogram(latency_ns: Sequence[int], binsize_ns: int, density: bool):
        if len(latency_ns) == 0:
            raise InvalidRecordsError(
                'Failed to calculate histogram.'
                'There is no amount of data required to calculate histograms.')

        range_min = math.floor(min(latency_ns) / binsize_ns) * binsize_ns
        range_max = math.ceil(max(latency_ns) / binsize_ns) * binsize_ns + binsize_ns
        bin_num = math.ceil((range_max - range_min) / binsize_ns)
        return np.histogram(
            latency_ns, bins=bin_num, range=(range_min, range_max), density=density)
