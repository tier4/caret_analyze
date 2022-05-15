
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
        self._callback_start = GroupedRecords(
            data.callback_start,
            [
                COLUMN_NAME.CALLBACK_OBJECT, COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )

        self._callback_end = GroupedRecords(
            data.callback_end,
            [
                COLUMN_NAME.CALLBACK_OBJECT
            ]
        )
        self._columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        ]
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

        callback_lttng = self._bridge.get_callback(callback)
        start_records = self._callback_start.get(callback_lttng.callback_object, 0)
        end_records = self._callback_end.get(callback_lttng.callback_object)

        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT
        ]

        records = merge_sequencial(
            left_records=start_records,
            right_records=end_records,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='inner'
        )
        records.columns.drop([COLUMN_NAME.IS_INTRA_PROCESS], base_name_match=True)

        prefix = callback.callback_name
        records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
        records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)

        records.columns.reindex(self._columns, base_name_match=True)

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
        callback_lttng = self._bridge.get_subscription_callback(callback)
        if callback_lttng.callback_object_intra is None:
            columns = UniqueList(
                self._callback_start.column_values + self._callback_end.column_values
            ).as_list()
            records = RecordsFactory.create_instance(None, columns)
            records.columns.drop([COLUMN_NAME.IS_INTRA_PROCESS], base_name_match=True)
            prefix = callback.callback_name
            records.columns.get(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
            records.columns.get(
                COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)
            return records

        start_records = self._callback_start.get(callback_lttng.callback_object_intra, 1)
        end_records = self._callback_end.get(callback_lttng.callback_object_intra)

        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.CALLBACK_OBJECT
        ]

        records = merge_sequencial(
            left_records=start_records,
            right_records=end_records,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='inner'
        )

        records.columns.drop([COLUMN_NAME.IS_INTRA_PROCESS], base_name_match=True)

        prefix = callback.callback_name
        records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True).add_prefix(prefix)
        records.columns.get(
            COLUMN_NAME.CALLBACK_END_TIMESTAMP, base_name_match=True).add_prefix(prefix)

        records.columns.reindex(self._columns, base_name_match=True)
        return records
