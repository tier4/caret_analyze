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

from .interface import RecordsInterface
from .record_factory import RecordFactory, RecordsFactory


class TimeRange:

    def __init__(
        self,
        min_value: int,
        max_value: int
    ) -> None:
        self._min = min_value
        self._max = max_value

    @property
    def max_value(self) -> int:
        return self._max

    @property
    def min_value(self) -> int:
        return self._min

    def add(self, value: int) -> None:
        self._min = min(self._min, value)
        self._max = max(self._max, value)


class ResponseMap:

    def __init__(
        self,
        records: RecordsInterface,
        input_column: str,
        output_column: str
    ):
        d = {}

        def add(input_time: int, output_time: int):
            if output_time not in d:
                d[output_time] = TimeRange(input_time, input_time)

            d[output_time].add(input_time)

        for i in range(len(records)):
            data = records.data[i]

            if input_column not in data.data or output_column not in data.data:
                continue

            input_time, output_time = data.data[input_column], data.data[output_column]
            add(input_time, output_time)

            for j in range(i+1, len(records)):
                data_ = records.data[j]
                if input_column not in data_.data or output_column not in data_.data:
                    continue

                input_time_, output_time_ = data_.data[input_column], data_.data[output_column]

                add(input_time, output_time_)

                if output_time < input_time_:
                    break

        self._d = d
        self._input_column = input_column
        self._output_column = output_column

    def sorted_iter(self):
        return iter(sorted(self._d))

    def __len__(self) -> int:
        return len(self._d)

    def at(self, end_time: int) -> TimeRange:
        return self._d[end_time]

    @property
    def input_column(self) -> str:
        return self._input_column

    @property
    def output_column(self) -> str:
        return self._output_column


class ResponseTime:

    def __init__(
        self,
        records: RecordsInterface,
        input_column: str,
        output_column: str
    ) -> None:
        response_map = ResponseMap(records, input_column, output_column)
        self._records = ResponseRecords(response_map)

    def to_records(self, *, all_pattern=False) -> RecordsInterface:
        return self._records.to_records(all_pattern)


class ResponseRecords:

    def __init__(
        self,
        response_map: ResponseMap,
    ) -> None:
        self._response_map = response_map

    def to_records(
        self,
        all_pattern: bool
    ) -> RecordsInterface:
        if len(self._response_map) == 0:
            return self._create_empty_records()

        if all_pattern:
            return self._create_all_pattern_records()

        return self._create_response_records()

    @property
    def _input_column(self):
        return self._response_map.input_column

    @property
    def _output_column(self):
        return self._response_map.output_column

    def _create_empty_records(self) -> RecordsInterface:
        return RecordsFactory.create_instance(
            None,
            [self._input_column, self._output_column]
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
