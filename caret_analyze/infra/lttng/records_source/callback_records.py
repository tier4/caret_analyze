
from typing import Union

from functools import lru_cache

from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    TimerCallbackValueLttng,
    SubscriptionCallbackValueLttng
)
from ....record import (
    RecordsInterface, merge_sequencial, GroupedRecords, RecordsFactory, UniqueList
)


class CallbackRecordsContainer:

    def __init__(
        self,
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
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        ]

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

    @lru_cache
    def get_records(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ) -> RecordsInterface:
        if isinstance(callback, SubscriptionCallbackValueLttng):
            intra_records = self.get_intra_records(callback).clone()
            inter_records = self.get_inter_records(callback).clone()
            intra_records.concat(inter_records)

            sort_column = intra_records.columns.get_by_base_name(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP)
            intra_records.sort(sort_column.column_name)
            return intra_records

        return self.get_inter_records(callback).clone()

    @lru_cache
    def get_inter_records(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ) -> RecordsInterface:

        records = self._get_inter_records(callback)
        records.columns.drop([COLUMN_NAME.IS_INTRA_PROCESS, COLUMN_NAME.CALLBACK_OBJECT])

        records.columns.reindex(self._columns)

        prefix = callback.callback_id
        records.columns.get_by_base_name(COLUMN_NAME.CALLBACK_START_TIMESTAMP).add_prefix(prefix)
        records.columns.get_by_base_name(COLUMN_NAME.CALLBACK_END_TIMESTAMP).add_prefix(prefix)

        return records

    def _get_inter_records(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ) -> RecordsInterface:
        start_records = self._callback_start.get(callback.callback_object, 0)
        end_records = self._callback_end.get(callback.callback_object)

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

        return records

    @lru_cache
    def get_intra_records(
        self,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        records = self._get_intra_records(callback)

        records.columns.drop([COLUMN_NAME.IS_INTRA_PROCESS, COLUMN_NAME.CALLBACK_OBJECT])

        records.columns.reindex(self._columns)

        prefix = callback.callback_id
        records.columns.get_by_base_name(COLUMN_NAME.CALLBACK_START_TIMESTAMP).add_prefix(prefix)
        records.columns.get_by_base_name(COLUMN_NAME.CALLBACK_END_TIMESTAMP).add_prefix(prefix)

        return records

    def _get_intra_records(
        self,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        if callback.callback_object_intra is None:
            columns = UniqueList(
                self._callback_start.column_values + self._callback_end.column_values
            ).as_list()
            return RecordsFactory.create_instance(None, columns)

        start_records = self._callback_start.get(callback.callback_object_intra, 1)
        end_records = self._callback_end.get(callback.callback_object_intra)

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

        return records
