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

from functools import lru_cache


from .callback_records import CallbackRecordsContainer
from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ....record import (
    merge_sequencial,
    RecordsInterface,
)
from ....value_objects import (
    VariablePassingStructValue,
)


class VarPassRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        cb_records: CallbackRecordsContainer,
    ) -> None:
        self._cb_records = cb_records
        self._bridge = bridge

    def get_records(
        self,
        callback: VariablePassingStructValue
    ) -> RecordsInterface:
        return self._get_records(callback).clone()

    @lru_cache
    def _get_records(
        self,
        var_pass: VariablePassingStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        ]
        read_records = self._cb_records.get_records(var_pass.callback_read)
        write_records = self._cb_records.get_records(var_pass.callback_write)

        read_records.columns.drop(
            [
                COLUMN_NAME.CALLBACK_END_TIMESTAMP,
                COLUMN_NAME.TID,
            ], base_name_match=True
        )

        write_records.columns.drop(
            [
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.TID,
            ], base_name_match=True
        )

        write_column = write_records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            base_name_match=True
        )
        read_column = read_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
        )

        merged_records = merge_sequencial(
            left_records=write_records,
            right_records=read_records,
            left_stamp_key=write_column.column_name,
            right_stamp_key=read_column.column_name,
            join_left_key=None,
            join_right_key=None,
            how='left_use_latest',
            progress_label='binding: callback_end and callback_start'
        )

        merged_records.sort(columns[0])
        merged_records.columns.reindex(columns, base_name_match=True)
        return merged_records
