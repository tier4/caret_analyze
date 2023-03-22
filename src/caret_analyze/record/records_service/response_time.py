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

import math

from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import numpy as np

from ..column import ColumnValue
from ..interface import RecordInterface, RecordsInterface
from ..record_factory import RecordFactory, RecordsFactory
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
        columns: List[str]
    ):
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        columns: List[str]
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
    def columns(self) -> List[str]:
        return self._columns


class ResponseTime:
    """
    Class which calculates response time.

    Parameters
    ----------
    records : RecordsInterface
        records to calculate response time.
    columns : str
        List of column names to be used in return value.
        If None, the first and last columns are used.

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
    >>> response_records = response.to_response_records()
    >>> response_df = response_records.to_dataframe()

    >>> path = app.get_path('path_name')
    >>> records = path.to_records()
    >>> response = ResponseTime(records)
    >>> response_records = response.to_response_records()
    >>> response_df = response_records.to_dataframe()

    """

    def __init__(
        self,
        records: RecordsInterface,
        *,
        columns: Optional[List[str]] = None
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        columns : Optional[str]
            List of column names to be used in return value.
            If None, only first and last columns are used.

        """
        columns = columns or [records.columns[0], records.columns[-1]]
        response_map = ResponseMap(records, columns)
        self._records = ResponseRecords(response_map)
        self._timeseries = ResponseTimeseries(self._records)
        self._histogram = ResponseHistogram(self._records, self._timeseries)

    def to_records(self, *, all_pattern=False) -> RecordsInterface:
        """
        Calculate response time records.

        Parameters
        ----------
        all_pattern : bool, optional
            If True, get response times with time overlap, by default False. [for debug]

        Returns
        -------
        RecordsInterface
            response time records.
            The best and worst cases alternate line by line.
            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        return self._records.to_records(all_pattern)

    def to_response_records(self) -> RecordsInterface:
        """
        Calculate response records.

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
        return self._records.to_range_records()

    def to_best_case_response_records(self) -> RecordsInterface:
        """
        Calculate response records.

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

    def to_worst_case_response_records(self) -> RecordsInterface:
        # NOTE:
        # We think this function is unnecessary.
        # If necessary, please contact us.
        raise NotImplementedError()

    def to_best_case_timeseries(self) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate the best-case time series data for response time.

        The best case for response time are included message flow latency.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            input time[ns], latency[ns]

        """
        return self._timeseries.to_best_case_timeseries()

    def to_worst_case_timeseries(self) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate the worst-case time series data for response time.

        The worst case in response time includes message flow latencies
        as well as delays caused by various factors such as lost messages.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            input time[ns], latency[ns]

        """
        return self._timeseries.to_worst_case_timeseries()

    def to_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
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
        Tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_histogram(binsize_ns, density)

    def to_best_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate the best-case histogram for response time.

        The best case for response time are included message flow latency.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_best_case_histogram(binsize_ns, density)

    def to_worst_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate the worst-case histogram for response time.

        The worst case in response time includes message flow latencies
        as well as delays caused by various factors such as lost messages.

        Returns
        -------
        Tuple[np.ndarray, np.ndarray]
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

    def to_records(
        self,
        all_pattern: bool
    ) -> RecordsInterface:
        """
        Calculate records.

        Parameters
        ----------
        all_pattern : bool
            Calculate response times with time overlap, by default False. [for debug]

        Returns
        -------
        RecordsInterface
            response time.
            The best and worst cases alternate line by line.
            Columns
            - {columns[0]}
            - {columns[1]}
            - {...}
            - {columns[n-1]}

        """
        if len(self._response_map) == 0:
            return self._create_empty_records()

        if all_pattern:
            return self._create_all_pattern_records()

        return self._create_response_records()

    def to_range_records(
        self,
        case: str = 'worst',
    ) -> RecordsInterface:
        """
        Calculate response time records.

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

    def to_best_case_records(self) -> RecordsInterface:
        """
        Calculate best case response time records.

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

    def to_worst_case_records(self) -> RecordsInterface:
        """
        Calculate worst case response records.

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
    def _columns(self) -> List[str]:
        return self._response_map.columns

    def _create_empty_records(
        self,
        columns: Optional[Sequence[ColumnValue]] = None
    ) -> RecordsInterface:
        columns = columns or [ColumnValue(column) for column in self._columns]
        return RecordsFactory.create_instance(
            None,
            columns
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
        columns: List[str]
    ) -> RecordInterface:
        record_dict: Dict[str, int] = {}
        for column in columns:
            record_dict[column] = record.get(column)
        return RecordFactory.create_instance(record_dict)

    @staticmethod
    def _create_worst_case_record(
        record: RecordInterface,
        columns: List[str],
        input_time_min: int
    ) -> RecordInterface:
        record_dict: Dict[str, int] = {}

        record_dict[columns[0]] = input_time_min
        for column in columns[1:]:
            record_dict[column] = record.get(column)
        return RecordFactory.create_instance(record_dict)

    @staticmethod
    def _create_worst_to_best_case_record(
        record: RecordInterface,
        columns: List[str],
        input_time_min: int
    ) -> RecordInterface:
        record_dict: Dict[str, int] = {}

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
    ) -> Tuple[np.ndarray, np.ndarray]:
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
        Tuple[np.ndarray, np.ndarray]
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
    ) -> Tuple[np.ndarray, np.ndarray]:
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
        Tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        _, latency_ns = self._timeseries.to_best_case_timeseries()
        return self._to_histogram(latency_ns, binsize_ns, density)

    def to_worst_case_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
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
        Tuple[np.ndarray, np.ndarray]
            frequency, latencies[ns].
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        _, latency_ns = self._timeseries.to_worst_case_timeseries()

        return self._to_histogram(latency_ns, binsize_ns, density)

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
