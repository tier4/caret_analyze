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

from typing import List, Tuple

from .column import ColumnValue
from .interface import RecordsInterface
from .record_factory import RecordsFactory


class GroupedRecords:

    def __init__(
        self,
        records: RecordsInterface,
        columns: List[str]
    ) -> None:
        self._columns = records.columns
        self._dict = records.groupby(columns)

    def get(self, *args: int) -> RecordsInterface:
        if not self.has(*args):
            return RecordsFactory.create_instance(None, self.column_values)

        return self._dict[args].clone()

    def has(self, *args: int) -> bool:
        return args in self._dict

    @property
    def column_values(self) -> Tuple[ColumnValue, ...]:
        return self._columns.to_value()

    @property
    def column_names(self) -> Tuple[ColumnValue, ...]:
        return tuple(c.column_names for c in self._columns)
