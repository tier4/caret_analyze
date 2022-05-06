from typing import Union

from functools import lru_cache

from .callback_records import CallbackRecordsContainer
from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng
)
from ....record import RecordsInterface, merge_sequencial, GroupedRecords



class SubscribeRecordsContainer:

    def __init__(
        self,
        data: Ros2DataModel,
        cb_records: CallbackRecordsContainer,
    ) -> None:
        self._cb_records = cb_records

        self._dispatch = GroupedRecords(
            data.dispatch_subscription_callback,
            [COLUMN_NAME.CALLBACK_OBJECT]
        )
        self._intra_dispatch = GroupedRecords(
            data.dispatch_intra_process_subscription_callback,
            [COLUMN_NAME.CALLBACK_OBJECT]
        )

    def get_records(
        self,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        inter_records = self.get_inter_records(callback).clone()
        intra_records = self.get_intra_records(callback).clone()
        inter_records.concat(intra_records)
        column = inter_records.columns.get_by_base_name(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP)
        inter_records.sort(column.column_name)
        return inter_records

    @lru_cache
    def get_inter_records(
        self,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        callback_records = self._cb_records.get_records(callback)
        dispatch_records = self._dispatch.get(callback.callback_object)

        join_columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT
        ]
        dispatch_column = dispatch_records.columns.get_by_base_name(
            COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP
        )
        callback_start = callback_records.columns.get_by_base_name(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        )
        records = merge_sequencial(
            left_records=dispatch_records,
            right_records=callback_records,
            left_stamp_key=dispatch_column.column_name,
            right_stamp_key=callback_start.column_name,
            join_left_key=join_columns,
            join_right_key=join_columns,
            how='left',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )
        records.columns.drop([dispatch_column.column_name])
        return records

    @lru_cache
    def get_intra_records(
        self,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        assert callback.callback_object_intra is not None
        callback_records = self._cb_records.get_intra_records(callback)
        intra_dispatch_records = self._intra_dispatch.get(callback.callback_object_intra)

        join_keys = [
            COLUMN_NAME.PID, COLUMN_NAME.TID, COLUMN_NAME.CALLBACK_OBJECT
        ]
        dispatch_column = intra_dispatch_records.columns.get_by_base_name(
            COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP
        )
        callback_start = callback_records.columns.get_by_base_name(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        )
        records = merge_sequencial(
            left_records=intra_dispatch_records,
            right_records=callback_records,
            left_stamp_key=dispatch_column.column_name,
            right_stamp_key=callback_start.column_name,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left',
            progress_label='binding intra and inter subscribe'
        )

        records.columns.drop([dispatch_column.column_name])
        return records

