
from email.mime import base
from functools import lru_cache

from caret_analyze.value_objects.callback import CallbackStructValue

from .callback_records import CallbackRecordsContainer
from ..column_names import COLUMN_NAME
from ..bridge import LttngBridge
from ..ros2_tracing.data_model import Ros2DataModel
from ....record import (
    RecordsInterface, merge_sequencial, GroupedRecords, RecordsFactory, UniqueList
)
from ....value_objects import (
    SubscriptionCallbackStructValue,
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
