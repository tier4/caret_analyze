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

from typing import Iterator, Optional, Sequence, Tuple

import numpy as np

from .column import ColumnValue
from .interface import RecordsInterface
from .record_factory import RecordFactory, RecordsFactory
from ..exceptions import InvalidRecordsError


class TimeRange:
    """Class that holds minimum and maximum values."""

    def __init__(
        self,
        min_value: int,
        max_value: int
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        min_value : int
            minimum value.
        max_value : int
            maximum value.

        """
        self._min = min_value
        self._max = max_value

    @property
    def max_value(self) -> int:
        """
        Get maximum value.

        Returns
        -------
        int
            maximum value.

        """
        return self._max

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

    def update(self, value: int) -> None:
        """
        Update range.

        Parameters
        ----------
        value : int
            value to apply.

        """
        self._max = max(self._max, value)


class ResponseMap():
    """Class that manages the dictionary type that is the source of response time calculation."""

    def __init__(
        self,
        records: RecordsInterface,
        input_column: str,
        output_column: str
    ):
        """
        Constructor.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        input_column : str
            column name which is input.
        output_column : str
            column name which is output.

        """
        d = {}

        input_min_time = None

        for data in records.data:
            if input_column not in data.columns or output_column not in data.columns:
                continue

            input_time, output_time = data.get(input_column), data.get(output_column)

            if input_min_time is None:
                input_min_time = input_time

            if output_time not in d:
                d[output_time] = TimeRange(input_min_time, input_time)

            d[output_time].update(input_time)

        self._d = d
        self._input_column = input_column
        self._output_column = output_column

    def sorted_iter(self) -> Iterator[int]:
        """
        Get iterator which returns output time in ascending order.

        Yields
        ------
        Iterator[int]
            iterator which returns output time.

        """
        return iter(sorted(self._d))

    def __len__(self) -> int:
        """
        Get number of output time.

        Returns
        -------
        int
            number of output time. It is same as number of TimeRange.

        """
        return len(self._d)

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
        return self._d[end_time]

    @property
    def input_column(self) -> str:
        """
        Get input column name.

        Returns
        -------
        str
            input column name.

        """
        return self._input_column

    @property
    def output_column(self) -> str:
        """
        Get output column name.

        Returns
        -------
        str
            output column name.

        """
        return self._output_column


class ResponseTime:
    """
    Class which calculates response time.

    Parameters
    ----------
    records : RecordsInterface
        records to calculate response time.
    input_column : str
        column name for input time.
    output_column : str
        column name for output time.

    Examples
    --------
    >>> from caret_analyze import Application, Architecture, Lttng
    >>> from caret_analyze.experiment import ResponseTime

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
        input_column: Optional[str] = None,
        output_column: Optional[str] = None
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        records : RecordsInterface
            records to calculate response time.
        input_column : Optional[str], optional
            column name which is input, by default None
            If None, the first column of records is selected.
        output_column : Optional[str], optional
            column name which is output, by default None
            If None, the last column of records is selected.

        """
        input_column = input_column or records.columns[0]
        output_column = output_column or records.columns[-1]
        response_map = ResponseMap(records, input_column, output_column)
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
            - {input_column}
            - {output_column}

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
            - {input_column}_min
            - {input_column}_max
            - {output_column}

        """
        return self._records.to_range_records()

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
        Constructor.

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
            - {input_column}
            - {output_column}

        """
        if len(self._response_map) == 0:
            return self._create_empty_records()

        if all_pattern:
            return self._create_all_pattern_records()

        return self._create_response_records()

    def to_range_records(self) -> RecordsInterface:
        """
        Calculate response time records.

        Returns
        -------
        RecordsInterface
            The best and worst cases are separated into separate columns.
            Columns
            - {input_column}_min
            - {input_column}_max
            - {output_column}

        """
        columns = [
            ColumnValue(f'{self._input_column}_min'),
            ColumnValue(f'{self._input_column}_max'),
            ColumnValue(self._response_map.output_column),
        ]

        records = self._create_empty_records(columns)

        def add_records(output_time, input_time_min, input_time_max):
            records.append(
                RecordFactory.create_instance(
                    {
                        self._response_map.output_column: output_time,
                        f'{self._input_column}_min': input_time_min,
                        f'{self._input_column}_max': input_time_max,
                    }
                )
            )

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
            - {input_column}
            - {output_column}

        """
        columns = [
            f'{self._input_column}',
            self._response_map.output_column,
        ]

        records = self._create_empty_records(columns)

        def add_records(output_time, input_time_min, input_time_max):
            records.append(
                RecordFactory.create_instance(
                    {
                        self._response_map.output_column: output_time,
                        f'{self._input_column}': input_time_max,
                    }
                )
            )

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
            - {input_column}
            - {output_column}

        """
        columns = [
            f'{self._input_column}',
            self._response_map.output_column,
        ]

        records = self._create_empty_records(columns)

        def add_records(output_time, input_time_min, input_time_max):
            records.append(
                RecordFactory.create_instance(
                    {
                        self._response_map.output_column: output_time,
                        f'{self._input_column}': input_time_min,
                    }
                )
            )

        self._create_response_records_core(add_records)

        records.sort_column_order()

        return records

    @property
    def _input_column(self):
        return self._response_map.input_column

    @property
    def _output_column(self):
        return self._response_map.output_column

    def _create_empty_records(
        self,
        columns: Optional[Sequence[ColumnValue]] = None
    ) -> RecordsInterface:
        columns = columns or [ColumnValue(self._input_column), ColumnValue(self._output_column)]
        return RecordsFactory.create_instance(
            None,
            columns
        )

    def _create_all_pattern_records(self) -> RecordsInterface:
        records = self._create_empty_records()

        for output_time in self._response_map.sorted_iter():
            input_time_range = self._response_map.at(output_time)

            records.append(
                RecordFactory.create_instance(
                    {self._input_column: input_time_range.min_value,
                        self._output_column: output_time}
                )
            )

            if input_time_range.min_value == input_time_range.max_value:
                continue

            records.append(
                RecordFactory.create_instance(
                    {self._input_column: input_time_range.max_value,
                        self._output_column: output_time}
                )
            )

        records.sort_column_order()

        return records

    def _create_response_records(self) -> RecordsInterface:
        records = self._create_empty_records()

        def add_records(output_time, input_time_min, input_time_max):
            records.append(
                RecordFactory.create_instance(
                    {self._input_column: input_time_min,
                        self._output_column: output_time}
                )
            )

            records.append(
                RecordFactory.create_instance(
                    {self._input_column: input_time_max,
                        self._output_column: output_time}
                )
            )

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

            callback(output_time, input_min_time, input_max_time)

        records.sort_column_order()

        return records


class ResponseTimeseries:

    def __init__(
        self,
        response_records: ResponseRecords
    ) -> None:
        self._records = response_records

    def to_best_case_timeseries(self):
        records = self._records.to_range_records()
        input_column = records.columns[1]
        output_column = records.columns[2]
        return self._to_timeseries(input_column, output_column)

    def to_worst_case_timeseries(self):
        records = self._records.to_range_records()
        input_column = records.columns[0]
        output_column = records.columns[2]
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
        Constructor.

        Parameters
        ----------
        response_records : ResponseRecords
            records for calculating histogram.

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
