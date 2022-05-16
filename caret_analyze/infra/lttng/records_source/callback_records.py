
from functools import lru_cache

from caret_analyze.value_objects.callback import CallbackStructValue

from ..column_names import COLUMN_NAME
from ..bridge import LttngBridge
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    SubscriptionCallbackValueLttng
)
from ....record import (
    RecordsInterface, merge_sequencial, GroupedRecords, RecordsFactory, UniqueList
)
from ....value_objects import SubscriptionCallbackStructValue


class CallbackRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
    ) -> None:
        self._intra_callback = GroupedRecords(
            data.intra_callback_duration,
            [
                COLUMN_NAME.CALLBACK_OBJECT,
            ]
        )
        self._inter_callback = GroupedRecords(
            data.inter_callback_duration,
            [
                COLUMN_NAME.CALLBACK_OBJECT
            ]
        )
        self._bridge = bridge

    # @property
    # def columns(self) -> Columns:
    #     columns = Columns()
    #     for column_name in self.column_names:
    #         for column in self._callback_start.columns + self._callback_end.columns:
    #             if column.column_name == column_name:
    #                 columns.append(column)
    #                 break
    #     return columns

    # @property
    # def column_names(self) -> List[str]:
    #     return self._columns

    def get_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        return self._get_records(callback).clone()

    @lru_cache
    def _get_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        if isinstance(callback, SubscriptionCallbackStructValue):
            intra_records = self.get_intra_records(callback)
            inter_records = self.get_inter_records(callback)
            inter_records.columns.drop([
                COLUMN_NAME.SOURCE_TIMESTAMP,
            ],
                base_name_match=True
            )

            intra_records.concat(inter_records)

            sort_column = intra_records.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True)
            intra_records.sort(sort_column.column_name)
            return intra_records

        return self.get_inter_records(callback)

    def get_inter_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        return self._get_inter_records(callback).clone()

    @lru_cache
    def _get_inter_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]

        callback_lttng = self._bridge.get_callback(callback)
        records = self._inter_callback.get(callback_lttng.callback_object)

        prefix = callback.callback_name
        records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
        records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)

        records.columns.reindex(columns, base_name_match=True)

        return records

    def get_intra_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        return self._get_intra_records(callback).clone()

    @lru_cache()
    def _get_intra_records(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
        ]
        callback_lttng = self._bridge.get_subscription_callback(callback)
        if callback_lttng.callback_object_intra is None:
            records = RecordsFactory.create_instance(None, self._intra_callback.column_values)
            prefix = callback.callback_name
            records.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
            records.columns.get(
                COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)
            return records

        records = self._intra_callback.get(callback_lttng.callback_object_intra)

        prefix = callback.callback_name
        records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
        records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)

        records.columns.reindex(columns, base_name_match=True)
        return records
