from functools import lru_cache
from typing import Optional

from caret_analyze.infra.lttng.bridge import LttngBridge

from ..column_names import COLUMN_NAME
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    IntraProcessBufferValueLttng
)
from ....record import RecordsInterface, merge_sequencial, GroupedRecords
from ....value_objects import IntraProcessBufferStructValue


class IpcBufferRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
    ) -> None:
        self._enqueue_records = GroupedRecords(
            data.ring_buffer_enqueue,
            [
                COLUMN_NAME.BUFFER
            ]
        )
        self._dequque_records = GroupedRecords(
            data.ring_buffer_dequeue,
            [
                COLUMN_NAME.BUFFER
            ]
        )
        self._bridge = bridge

    def get_records(
        self,
        buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        return self._get_records(buffer).clone()

    @lru_cache
    def _get_records(
        self,
        buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        buffer_lttng = self._bridge.get_ipc_buffer(buffer)
        columns = [
            COLUMN_NAME.PID,
            'enqueue_tid',
            COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            COLUMN_NAME.BUFFER,
            'index',
            'queued_msg_size',
            'is_full',
            'dequeue_tid',
            COLUMN_NAME.BUFFER_DEQUEUE_TIMESTAMP,
            'dequeued_msg_size'
        ]
        enqueue_records = self._enqueue_records.get(buffer_lttng.buffer)
        dequeue_records = self._dequque_records.get(buffer_lttng.buffer)

        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.BUFFER,
            'index'
        ]
        enqueue_records.columns.rename({
            'size': 'queued_msg_size',
            'tid': 'enqueue_tid',
        })

        dequeue_records.columns.rename({
            'size': 'dequeued_msg_size',
            'tid': 'dequeue_tid',
        })
        records = merge_sequencial(
            left_records=enqueue_records,
            right_records=dequeue_records,
            left_stamp_key=COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.BUFFER_DEQUEUE_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
            how='left'
        )
        records.columns.reindex(columns)
        columns_ = records.columns.gets([
            COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
            COLUMN_NAME.BUFFER_DEQUEUE_TIMESTAMP,
        ], base_name_match=True)
        for column in columns_:
            column.add_prefix(buffer.topic_name)
        return records
