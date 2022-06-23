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

from .path_base import PathBase
from ..common import Summarizable, Summary
from ..infra.interface import RecordsProvider
from ..record import RecordsInterface
from ..value_objects import VariablePassingStructValue


class VariablePassing(PathBase, Summarizable):

    def __init__(
        self,
        variable_passing: VariablePassingStructValue,
        records_provider: RecordsProvider,
    ) -> None:
        super().__init__()
        self._val = variable_passing
        self._provider = records_provider

    def _to_records_core(self) -> RecordsInterface:
        records = self._provider.variable_passing_records(self._val)
        records.sort(records.columns[0])

        return records

    @property
    def summary(self) -> Summary:
        return self._val.summary

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name

        """
        return self._val.node_name

    @property
    def callback_name_write(self) -> str:
        """
        Get write-side callback name.

        Returns
        -------
        [str]
            write-side callback name.

        """
        return self._val.callback_name_write

    @property
    def callback_name_read(self):
        """
        Get read-side callback name.

        Returns
        -------
        [str]
            read-side callback name.

        """
        return self._val.callback_name_read
