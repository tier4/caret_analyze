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
from ....record import (
    RecordsInterface, merge_sequencial, GroupedRecords, RecordsFactory, UniqueList
)
from ....value_objects import SubscriptionCallbackStructValue


class SubscribeRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        cb_records: CallbackRecordsContainer,
    ) -> None:
        self._cb_records = cb_records

        self._bridge = bridge

    def get_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        inter_records = self.get_inter_records(callback)
        intra_records = self.get_intra_records(callback)
        inter_records.columns.drop([COLUMN_NAME.SOURCE_TIMESTAMP], base_name_match=True)
        inter_records.concat(intra_records)
        column = inter_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
        )
        inter_records.sort(column.column_name)
        return inter_records

    def get_inter_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        return self._get_inter_records(callback).clone()

    @lru_cache
    def _get_inter_records(
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
        records = self._cb_records.get_inter_records(callback)

        records.columns.reindex(columns, base_name_match=True)

        add_prefix_columns = records.columns.gets([
            'source_timestamp',
            'message_timestamp',
        ], base_name_match=True)
        for add_prefix_column in add_prefix_columns:
            add_prefix_column.add_prefix(callback.subscribe_topic_name)
        return records

    def get_intra_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        return self._get_intra_records(callback).clone()

    @lru_cache
    def _get_intra_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        columns = [
            'pid', 'tid', 'callback_object',
            'callback_start_timestamp', 'callback_end_timestamp', 'message_timestamp',
        ]

        records = self._cb_records.get_intra_records(callback)
        column = records.columns.get(COLUMN_NAME.MESSAGE_TIMESTAMP, base_name_match=True)
        column.add_prefix(callback.subscribe_topic_name)
        records.columns.reindex(columns, base_name_match=True)
        return records
