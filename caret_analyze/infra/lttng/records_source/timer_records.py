# Copyright 2021 Research Institute of Systems Planning, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import lru_cache
from typing import List, Sequence


from .callback_records import CallbackRecordsContainer
from ..bridge import LttngBridge
from ..column_names import COLUMN_NAME
from ..events_factory import EventsFactory
from ..lttng_info import LttngInfo
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    TimerCallbackValueLttng,
    TimerControl,
    TimerInit,
)
from ....common import Util
from ....record import (
    ColumnValue,
    GroupedRecords,
    merge_sequencial,
    RecordFactory,
    RecordsFactory,
    RecordsInterface,
)
from ....value_objects import TimerStructValue


class TimerRecordsContainer:

    def __init__(
        self,
        bridge: LttngBridge,
        data: Ros2DataModel,
        info: LttngInfo,
        cb_records: CallbackRecordsContainer
    ) -> None:
        self._cb_records = cb_records
        self._bridge = bridge
        self._info = info
        self._timer_init = GroupedRecords(
            data.rcl_timer_init,
            [
                COLUMN_NAME.TIMER_HANDLE,
            ]
        )

    def get_records(
        self,
        timer: TimerStructValue
    ) -> RecordsInterface:
        return self._get_records(timer).clone()

    def _create_timer_events_factory(
        self,
        timer_callback: TimerCallbackValueLttng,
    ) -> EventsFactory:
        class TimerEventsFactory(EventsFactory):

            def __init__(self, ctrls: Sequence[TimerControl]) -> None:
                self._ctrls = ctrls

            def create(self, until_ns: int) -> RecordsInterface:

                columns = [
                    ColumnValue(COLUMN_NAME.TIMER_EVENT_TIMESTAMP),
                ]

                records = RecordsFactory.create_instance(None, columns)
                for ctrl in self._ctrls:

                    if isinstance(ctrl, TimerInit):
                        ctrl._timestamp
                        timer_timestamp = ctrl._timestamp
                        while timer_timestamp < until_ns:
                            record_dict = {
                                COLUMN_NAME.TIMER_EVENT_TIMESTAMP: timer_timestamp,
                            }
                            record = RecordFactory.create_instance(record_dict)
                            records.append(record)
                            timer_timestamp = timer_timestamp+ctrl.period_ns

                return records

        timer_ctrls = self._create_timer_controls(timer_callback)

        filtered_timer_ctrls = Util.filter_items(
            lambda x: x.timer_handle == timer_callback.timer_handle, timer_ctrls)

        return TimerEventsFactory(filtered_timer_ctrls)

    def _create_timer_controls(
        self,
        timer_callback: TimerCallbackValueLttng
    ) -> Sequence[TimerControl]:
        ctrls: List[TimerControl] = []

        init_records = self._timer_init.get(timer_callback.timer_handle)
        for i in range(len(init_records)):
            timer_handle = init_records.iget(i, 'timer_handle')
            timestamp = init_records.iget(i, 'timestamp')
            period = init_records.iget(i, 'period')
            assert timer_handle is not None
            assert timestamp is not None
            assert period is not None
            ctrl = TimerInit(timer_handle, timestamp, period)
            ctrls.append(ctrl)

        return ctrls

    @lru_cache
    def _get_records(
        self,
        timer: TimerStructValue
    ) -> RecordsInterface:
        columns = [
            COLUMN_NAME.PID,
            COLUMN_NAME.TID,
            COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        ]

        assert timer.callback is not None
        callback_records = self._cb_records.get_records(timer.callback)
        cb = self._bridge.get_timer_callback(timer.callback)
        factory = self._create_timer_events_factory(cb)
        last_record = callback_records.data[-1]
        cb_start_column = callback_records.columns.get(
            COLUMN_NAME.CALLBACK_START_TIMESTAMP, base_name_match=True)
        last_callback_start = last_record.get(cb_start_column.column_name)

        timer_events = factory.create(last_callback_start)

        for column in timer_events.columns:
            column.add_prefix(timer.callback_name)

        event_column = timer_events.columns.get(
            COLUMN_NAME.TIMER_EVENT_TIMESTAMP, base_name_match=True)

        timer_records = merge_sequencial(
            left_records=timer_events,
            right_records=callback_records,
            left_stamp_key=event_column.column_name,
            right_stamp_key=cb_start_column.column_name,
            join_left_key=None,
            join_right_key=None,
            how='left'
        )
        timer_records.columns.reindex(columns, base_name_match=True)
        return timer_records
