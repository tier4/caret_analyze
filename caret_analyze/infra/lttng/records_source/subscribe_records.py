from typing import Union

from functools import lru_cache

from .callback_records import CallbackRecordsContainer
from ..column_names import COLUMN_NAME
from ..bridge import LttngBridge
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng
)
from ....record import RecordsInterface, merge_sequencial, GroupedRecords
from ....value_objects import SubscriptionCallbackStructValue



class SubscribeRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
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
        self._bridge = bridge

    def get_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        inter_records = self.get_inter_records(callback).clone()
        intra_records = self.get_intra_records(callback).clone()
        inter_records.columns.drop([COLUMN_NAME.SOURCE_TIMESTAMP], base_name_match=True)
        inter_records.concat(intra_records)
        column = inter_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
        )
        inter_records.sort(column.column_name)
        return inter_records

    @lru_cache
    def get_inter_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        columns = [
            'pid',
            'tid',
            'callback_object',
            'source_timestamp',
            'message_timestamp',
            'callback_start_timestamp',
            'callback_end_timestamp']
        callback_lttng = self._bridge.get_subscription_callback(callback)

        callback_records = self._cb_records.get_records(callback)
        dispatch_records = self._dispatch.get(callback_lttng.callback_object)
        dispatch_records.columns.drop(['message'], base_name_match=True)

        join_columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT
        ]
        dispatch_column = dispatch_records.columns.get(
            COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            base_name_match=True
        )
        callback_start = callback_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
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
        records.columns.reindex(columns, base_name_match=True)

        columns = records.columns.gets([
            'source_timestamp',
            'message_timestamp',
        ], base_name_match=True)
        for column in columns:
            column.add_prefix(callback.subscribe_topic_name)
        return records

    @lru_cache
    def get_intra_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        columns = [
            'pid', 'tid', 'callback_object',
            'message_timestamp', 'callback_start_timestamp', 'callback_end_timestamp']

        callback_lttng = self._bridge.get_subscription_callback(callback)
        callback_records = self._cb_records.get_intra_records(callback)

        assert callback_lttng.callback_object_intra is not None
        intra_dispatch_records = self._intra_dispatch.get(callback_lttng.callback_object_intra)
        intra_dispatch_records.columns.drop(['message'], base_name_match=True)

        join_keys = [
            COLUMN_NAME.PID, COLUMN_NAME.TID, COLUMN_NAME.CALLBACK_OBJECT
        ]
        dispatch_column = intra_dispatch_records.columns.get(
            COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            base_name_match=True
        )
        callback_start = callback_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
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
        column = records.columns.get(COLUMN_NAME.MESSAGE_TIMESTAMP, base_name_match=True)
        column.add_prefix(callback.subscribe_topic_name)
        records.columns.reindex(columns, base_name_match=True)
        return records
