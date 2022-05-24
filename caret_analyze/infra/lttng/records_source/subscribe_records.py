from tkinter.tix import COLUMN
from typing import Union

from functools import lru_cache

from caret_analyze.record.record import Records

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
from ....value_objects import SubscriptionCallbackStructValue, SubscriptionStructValue


class SubscribeRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        cb_records: CallbackRecordsContainer,
    ) -> None:
        self._cb_records = cb_records

        self._tilde_subscribe = GroupedRecords(
            data.tilde_subscribe,
            [
                COLUMN_NAME.TILDE_SUBSCRIPTION
            ]
        )
        self._bridge = bridge

    def _has_tilde(
        self,
        subscription: SubscriptionStructValue
    ) -> bool:
        sub_cb = self._bridge.get_subscription_callback(
            subscription.callback)
        return sub_cb.tilde_subscription is not None

    def get_tilde_records(
        self,
        subscription: SubscriptionStructValue,
    ) -> RecordsInterface:
        sub_lttng = self._bridge.get_subscription_callback(subscription.callback)
        if sub_lttng.tilde_subscription is None:
            return self._tilde_subscribe.get(0)  # return empty records
        tilde_records = self._tilde_subscribe.get(sub_lttng.tilde_subscription)
        return tilde_records

    def get_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]
        if self._has_tilde(subscription):
            columns.append(COLUMN_NAME.TILDE_MESSAGE_ID)
        columns.extend([
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        ])
        records = self.get_inter_records(subscription)
        intra_records = self.get_intra_records(subscription)
        tilde_records = self.get_tilde_records(subscription)
        records.columns.drop([COLUMN_NAME.SOURCE_TIMESTAMP], base_name_match=True)
        records.concat(intra_records)

        if self._has_tilde(subscription):
            join_keys = [
                COLUMN_NAME.TID
            ]
            left_merge_column = tilde_records.columns.get(
                COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
                base_name_match=True
            )
            right_merge_column = records.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                base_name_match=True
            )
            records = merge_sequencial(
                left_records=records,
                right_records=tilde_records,
                left_stamp_key=right_merge_column.column_name,
                right_stamp_key=left_merge_column.column_name,
                join_left_key=join_keys,
                join_right_key=join_keys,
                how='inner'
            )
            records.columns.drop(
                [
                    COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
                    COLUMN_NAME.TILDE_SUBSCRIPTION,
                ],
                base_name_match=True
            )
            records.columns.get(
                COLUMN_NAME.TILDE_MESSAGE_ID
            ).add_prefix(subscription.topic_name)

        column = records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            base_name_match=True
        )
        records.sort(column.column_name)
        records.columns.reindex(columns, base_name_match=True)
        return records

    def get_inter_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        return self._get_inter_records(subscription).clone()

    @lru_cache
    def _get_inter_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        columns = [
            'pid',
            'tid',
            'source_timestamp',
            'message_timestamp',
            'callback_start_timestamp',
            'callback_end_timestamp']
        records = self._cb_records.get_inter_records(subscription.callback)
        records.columns.drop(
            [COLUMN_NAME.CALLBACK_OBJECT]
        )

        records.columns.reindex(columns, base_name_match=True)

        add_prefix_columns = records.columns.gets([
            'source_timestamp',
            'message_timestamp',
        ], base_name_match=True)
        for add_prefix_column in add_prefix_columns:
            add_prefix_column.add_prefix(subscription.topic_name)
        return records

    def get_intra_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        return self._get_intra_records(subscription).clone()

    @lru_cache
    def _get_intra_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        columns = [
            'pid',
            'tid',
            'callback_start_timestamp',
            'callback_end_timestamp',
            'message_timestamp',
        ]

        records = self._cb_records.get_intra_records(subscription.callback)
        records.columns.drop(
            [COLUMN_NAME.CALLBACK_OBJECT]
        )
        column = records.columns.get(COLUMN_NAME.MESSAGE_TIMESTAMP, base_name_match=True)
        column.add_prefix(subscription.topic_name)
        records.columns.reindex(columns, base_name_match=True)
        return records
