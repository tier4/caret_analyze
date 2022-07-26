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

from typing import Iterator, List, Optional, Tuple

import numpy as np

from .interface import RecordInterface, RecordsInterface
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
        self._min = min(self._min, value)
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

        def update(input_time: int, output_time: int):
            if output_time not in d:
                d[output_time] = TimeRange(input_time, input_time)

            d[output_time].update(input_time)

        for i in range(len(records)):
            data: RecordInterface = records.data[i]

            if input_column not in data.columns or output_column not in data.columns:
                continue

            input_time, output_time = data.get(input_column), data.get(output_column)
            update(input_time, output_time)

            for j in range(i+1, len(records)):
                data_: RecordInterface = records.data[j]
                if input_column not in data_.columns or output_column not in data_.columns:
                    continue

                input_time_, output_time_ = data_.get(input_column), data_.get(output_column)

                update(input_time, output_time_)

                if output_time < input_time_:
                    break

        self._d = d
        self._input_column = input_column
        self._output_column = output_column

    def sorted_iter(self) -> Iterator[int]:
        """
        Get iterator which returns output time in ascending order.

        Yields
        ------
        Iterator[int]
            output time.

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
    >>> arch = Architecture('lttng', '/path/to/ctf')
    >>> lttng = Lttng('humble')
    >>> app = Application(arch, lttng)

    >>> # Select target instance
    >>> node = app.get_node('node_name')
    >>> callback = node.callbacks[0]
    >>> callback.summary.pprint()

    >>> # Calculate response time
    >>> response = ResponseTime(records, records.columns[0], records.columns[1])
    >>> response_records = response.to_response_records()
    >>> response_df = response_records.to_dataframe()

    """

    def __init__(
        self,
        records: RecordsInterface,
        input_column: str,
        output_column: str
    ) -> None:
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
        response_map = ResponseMap(records, input_column, output_column)
        self._records = ResponseRecords(response_map)
        self._histogram = ResponseHistogram(self._records)

    def to_records(self, *, all_pattern=False) -> RecordsInterface:
        """
        Calculate records.

        Parameters
        ----------
        all_pattern : bool, optional
            Get response times with time overlap, by default False. [for debug]

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
        Calculate records.

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

    def to_histogram(
        self,
        binsize_ns: int = 1000000,
        density: bool = False,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get response time histogram.

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
            hist and bin_edges.
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        """
        return self._histogram.to_histogram(binsize_ns, density)


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
            Get response times with time overlap, by default False. [for debug]

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
        Calculate records.

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
            f'{self._input_column}_min',
            f'{self._input_column}_max',
            self._response_map.output_column,
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

    @property
    def _input_column(self):
        return self._response_map.input_column

    @property
    def _output_column(self):
        return self._response_map.output_column

    def _create_empty_records(self, columns: Optional[List[str]] = None) -> RecordsInterface:
        columns_ = columns or [self._input_column, self._output_column]
        return RecordsFactory.create_instance(
            None,
            columns_
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


class ResponseHistogram:
    """Class that calculates response time histogram."""

    def __init__(
        self,
        response_records: ResponseRecords,
    ) -> None:
        """
        Constructor.

        Parameters
        ----------
        response_records : ResponseRecords
            records for calculating histogram.

        """
        self._response_records = response_records

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
            hist and bin_edges.
            ref.  https://numpy.org/doc/stable/reference/generated/numpy.histogram.html

        Raises
        ------
        InvalidRecordsError
            Occurs when the number of response latencies is insufficient.

        """
        records = self._response_records.to_range_records()

        input_min_column = records.columns[0]
        input_max_column = records.columns[1]
        output_column = records.columns[2]

        latency_ns = []
        # Note: need to speed up.

        for record in records:
            output_time = record.get(output_column)
            input_time_min = record.get(input_min_column)
            input_time_max = record.get(input_max_column)
            for input_time in range(input_time_min, input_time_max + 1):
                latency = output_time - input_time
                latency_ns.append(latency)

        if len(latency_ns) == 0:
            raise InvalidRecordsError(
                'Failed to calculate histogram.'
                'There is no amount of data required to calculate histograms.'
            )

        range_min = math.floor(min(latency_ns) / binsize_ns) * binsize_ns
        range_max = math.ceil(max(latency_ns) / binsize_ns) * binsize_ns
        bin_num = math.ceil((range_max - range_min) / binsize_ns)
        return np.histogram(
            latency_ns, bins=bin_num, range=(range_min, range_max), density=density)
