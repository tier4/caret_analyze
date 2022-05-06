
from typing import Union, Sequence

from .callback_records import CallbackRecordsContainer
from .publish_records import PublishRecordsContainer

from ..column_names import COLUMN_NAME
from ..value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
)
from ....record import RecordsInterface, merge_sequencial, ColumnAttribute


class NodeUseLatestMessageRecordsContainer:

    def __init__(
        self,
        cb_records: CallbackRecordsContainer,
        pub_records: PublishRecordsContainer,
    ) -> None:
        self._cb_records = cb_records
        self._pub_records = pub_records

    def get_use_latest_message_records(
        self,
        callback: Union[SubscriptionCallbackValueLttng, TimerCallbackValueLttng],
        publisher: PublisherValueLttng,
    ) -> RecordsInterface:

        publish_records = self._pub_records.get_records(publisher)
        publish_column = publish_records.columns.get_by_attrs([
            ColumnAttribute.NODE_IO,
            ColumnAttribute.SEND_MSG,
            ColumnAttribute.SYSTEM_TIME,
        ], 'head')
        callback_records = self._cb_records.get_records(callback)

        callback_column = callback_records.columns.get_by_base_name(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        )
        records = merge_sequencial(
            left_records=callback_records,
            right_records=publish_records,
            left_stamp_key=callback_column.column_name,
            right_stamp_key=publish_column.column_name,
            join_left_key=['pid'],
            join_right_key=['pid'],
            how='left'
        )

        # if self._node_path.subscription is not None:
        #     node_in_records = self._provider.subscribe_records(self._node_path.subscription)
        # elif self._node_path.tf_frame_buffer is not None:
        #     node_in_records = self._provider.tf_lookup_records(self._node_path.tf_frame_buffer)

        # if self._node_path.publisher is not None:
        #     node_out_records = self._provider.publish_records(self._node_path.publisher)
        # elif self._node_path.tf_frame_broadcaster is not None and \
        #         self._node_path.tf_frame_broadcaster.publisher is not None:
        #     node_out_records = self._provider.publish_records(
        #         self._node_path.tf_frame_broadcaster.publisher)
        # else:
        #     raise UnsupportedNodeRecordsError('node_path.publisher is None')

        # column_names = [
        #     node_in_records.columns[0].column_name,
        #     f'{self._node_path.publish_topic_name}/rclcpp_publish_timestamp',
        # ]
        # node_io_records = merge_sequencial(
        #     left_records=node_in_records,
        #     right_records=node_out_records,
        #     left_stamp_key=node_in_records.columns[0].column_name,
        #     right_stamp_key=node_out_records.columns[0].column_name,
        #     join_left_key=None,
        #     join_right_key=None,
        #     how='left_use_latest',
        #     progress_label='binding use_latest_message.'
        # )

        # drop_columns = list(set(node_io_records.column_names) - set(column_names))
        # node_io_records.drop_columns(drop_columns)
        # node_io_records.reindex(column_names)
        # return node_io_records

        return records

    def get_callback_chain(
        self,
        callbacks: Sequence[Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]],
        publisher: PublisherValueLttng,
    ) -> RecordsInterface:

        records = self._cb_records.get_records(callbacks[0])

        for callback in callbacks[1:]:
            records_ = self._cb_records.get_records(callback)
            end_column = records.columns.get_by_base_name(
                COLUMN_NAME.CALLBACK_END_TIMESTAMP, take='tail'
            )
            start_column = records_.columns.get_by_base_name(
                COLUMN_NAME.CALLBACK_START_TIMESTAMP, take='head'
            )

            records = merge_sequencial(
                left_records=records,
                right_records=records_,
                left_stamp_key=end_column.column_name,
                right_stamp_key=start_column.column_name,
                join_left_key=[COLUMN_NAME.PID],
                join_right_key=[COLUMN_NAME.PID],
                how='left'
            )

        column_callback_start_time = records.columns.get_by_base_name(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, take='tail')

        publish_records = self._pub_records.get_records(publisher)
        column_publish_time = publish_records.columns.get_by_base_name(
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, take='head')

        records = merge_sequencial(
            left_records=records,
            right_records=publish_records,
            join_left_key=None,
            join_right_key=None,
            left_stamp_key=column_callback_start_time.column_name,
            right_stamp_key=column_publish_time.column_name,
            how='left',
            progress_label='binding: callback_start and publish',
        )

        return records
