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

from functools import cached_property

from typing import Dict, List, Sequence

from .column_names import COLUMN_NAME
from .events_factory import EventsFactory
from .lttng_info import LttngInfo
from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import TimerCallbackValueLttng, TimerControl, TimerInit
from ...common import Util
from ...record import (merge, merge_sequential,
                       merge_sequential_for_addr_track,
                       RecordFactory,
                       RecordsFactory,
                       RecordsInterface)
from ...record.column import Columns, ColumnValue


class RecordsSource():

    def __init__(
        self,
        data: Ros2DataModel,
        info: LttngInfo
    ) -> None:
        self._data = data
        self._preprocess(self._data)
        self._info = info

    @staticmethod
    def _preprocess(data: Ros2DataModel):
        data.rclcpp_publish_instances.rename_columns(
            {COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP}
        )

    @cached_property
    def _grouped_callback_start(self) -> Dict[int, RecordsInterface]:
        records = self._data.callback_start_instances.clone()
        group: Dict[int, RecordsInterface] = {}
        for k, v in records.groupby([COLUMN_NAME.IS_INTRA_PROCESS]).items():
            assert len(k) == 1
            group[k[0]] = v
        return group

    @cached_property
    def inter_proc_comm_records(self) -> RecordsInterface:
        """
        Compose inter process communication records.

        Used tracepoints
        - rclcpp_publish
        - dds_bind_addr_to_addr
        - dds_bind_addr_to_stamp
        - rcl_publish (Optional)
        - dds_write (Optional)
        - dispatch_subscription_callback
        - callback_start

        Returns
        -------
        RecordsInterface
            columns
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp (Optional)
            - dds_write_timestamp (Optional)
            - message_timestamp
            - source_timestamp

        """
        publish = self._data.rclcpp_publish_instances

        publish = merge_sequential_for_addr_track(
            source_records=publish,
            copy_records=self._data.dds_bind_addr_to_addr,
            sink_records=self._data.dds_bind_addr_to_stamp,
            source_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ADDR_FROM,
            copy_to_key=COLUMN_NAME.ADDR_TO,
            sink_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            sink_from_key=COLUMN_NAME.ADDR,
            columns=[
                COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.SOURCE_TIMESTAMP,
            ],
            progress_label='binding: message_addr and rclcpp_publish',
        )

        rcl_publish_records = self._data.rcl_publish_instances
        rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
        if len(rcl_publish_records) > 0:
            publish = merge_sequential(
                left_records=publish,
                right_records=rcl_publish_records,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                join_left_key=COLUMN_NAME.MESSAGE,
                join_right_key=COLUMN_NAME.MESSAGE,
                how='left',
                columns=[
                    COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.PUBLISHER_HANDLE,
                    COLUMN_NAME.MESSAGE,
                    COLUMN_NAME.MESSAGE_TIMESTAMP,
                ],
                progress_label='binding: rclcpp_publish and rcl_publish',
            )

        dds_write = self._data.dds_write_instances
        if len(dds_write) > 0:
            publish = merge_sequential(
                left_records=publish,
                right_records=dds_write,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                join_left_key=COLUMN_NAME.MESSAGE,
                join_right_key=COLUMN_NAME.MESSAGE,
                columns=Columns.from_str(
                    publish.columns + dds_write.columns
                ).column_names,
                how='left',
                progress_label='binding: rcl_publish and dds_write',
            )

        # When both intra_publish and inter_publish are used, value mismatch occurs when merging.
        # In order to merge at the latency time,
        # align the time to intra_publish if intra_process communication is used.
        intra_publish = self._data.rclcpp_intra_publish_instances.clone()
        intra_publish.drop_columns([
            COLUMN_NAME.MESSAGE, COLUMN_NAME.MESSAGE_TIMESTAMP
        ])
        publish = merge_sequential(
            left_records=intra_publish,
            right_records=publish,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.PUBLISHER_HANDLE,
            join_right_key=COLUMN_NAME.PUBLISHER_HANDLE,
            columns=Columns.from_str(
                intra_publish.columns + publish.columns
            ).column_names,
            how='right'
        )
        rclcpp_publish = [None] * len(publish.data)
        for i, record in enumerate(publish.data):
            if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.data:
                rclcpp_publish[i] = record.data[COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP]
            else:
                rclcpp_publish[i] = record.data[COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP]
        publish.append_column(
            ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP), rclcpp_publish)
        publish.drop_columns([
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP
        ])

        callback_start_instances = self.inter_callback_records
        subscription = self._data.dispatch_subscription_callback_instances

        subscription = merge_sequential(
            left_records=subscription,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns.from_str(
                subscription.columns + callback_start_instances.columns
            ).column_names,
            how='left',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )

        # communication = merge(
        #     publish,
        #     self._data.on_data_available_instances,
        #     join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
        #     join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
        #     columns=publish.columns,
        #     how='left',
        #     progress_label='binding: source_timestamp and on_data_available',
        # )

        communication = merge(
            publish,
            subscription,
            join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            columns=Columns.from_str(
                publish.columns + subscription.columns
            ).column_names,
            how='left',
            progress_label='binding: source_timestamp and callback_start',
        )

        communication.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS,
                COLUMN_NAME.ADDR,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
                COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            ],
        )

        return communication

    @cached_property
    def publish_records(self) -> RecordsInterface:
        """
        Compose publish records.

        Returns
        -------
        RecordsInterface
            Columns
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp (Optional)
            - dds_write_timestamp (Optional)
            - message_timestamp
            - source_timestamp

        """
        inter_proc_publish = self._data.rclcpp_publish_instances

        rcl_publish_records = self._data.rcl_publish_instances
        rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
        if len(rcl_publish_records) > 0:
            inter_proc_publish = merge_sequential(
                left_records=inter_proc_publish,
                right_records=rcl_publish_records,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                join_left_key='tid',
                join_right_key='tid',
                how='left',
                columns=[
                    COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.PUBLISHER_HANDLE,
                    COLUMN_NAME.MESSAGE,
                    'tid',
                    COLUMN_NAME.MESSAGE_TIMESTAMP,
                ],
                progress_label='binding: rclcpp_publish and rcl_publish',
            )

        dds_write = self._data.dds_write_instances
        if len(dds_write) > 0:
            inter_proc_publish = merge_sequential(
                left_records=inter_proc_publish,
                right_records=dds_write,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                join_left_key='tid',
                join_right_key='tid',
                columns=Columns.from_str(
                    inter_proc_publish.columns + dds_write.columns
                ).column_names,
                how='left',
                progress_label='binding: rclcpp_publish and dds_write',
            )

        inter_proc_publish = merge_sequential(
            left_records=inter_proc_publish,
            right_records=self._data.dds_bind_addr_to_stamp,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            join_left_key='tid',
            join_right_key='tid',
            columns=Columns.from_str(
                inter_proc_publish.columns + self._data.dds_bind_addr_to_stamp.columns
            ).column_names,
            how='left',
            progress_label='binding: rclcpp_publish and source_timestamp',
        )

        inter_proc_publish.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS,
                COLUMN_NAME.ADDR,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
                COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            ],
        )

        intra_proc_publish = self._data.rclcpp_intra_publish_instances.clone()
        intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])
        # intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])

        # When publishing to both intra-process and inter-process,
        # intra-process communication is done first.
        # On the other hand,
        # there are cases where only one or the other is done, so we use outer join.
        # https://github.com/ros2/rclcpp/blob/galactic/rclcpp/include/rclcpp/publisher.hpp#L203
        publish = merge_sequential(
            left_records=intra_proc_publish,
            right_records=inter_proc_publish,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.PUBLISHER_HANDLE,
            join_right_key=COLUMN_NAME.PUBLISHER_HANDLE,
            columns=Columns.from_str(
                inter_proc_publish.columns + intra_proc_publish.columns
            ).column_names,
            how='outer',
            progress_label='binding intra_publish and inter_publish'
        )

        publish_stamps = []
        maxsize = 2**64-1
        for record in publish.data:
            rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
            if COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP in record.columns:
                rclcpp_publish = record.get(COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)
            if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.columns:
                rclcpp_intra_publish = record.get(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP)
            inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
            publish_stamps.append(inter_intra_publish)

        publish.append_column(ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP), publish_stamps)

        columns = []
        columns.append(COLUMN_NAME.PUBLISHER_HANDLE)
        columns.append(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in publish.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in publish.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

        drop_columns = set(publish.columns) - set(columns)
        publish.drop_columns(list(drop_columns))
        publish.reindex(columns)

        return publish

    def create_timer_events_factory(
        self,
        timer_callback: TimerCallbackValueLttng
    ) -> EventsFactory:
        """
        Create timer events factory.

        Parameters
        ----------
        timer_callback : TimerCallbackValueLttng
            target callback to create timer events.

        Returns
        -------
        EventsFactory

        """
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

        timer_ctrls = self._info.get_timer_controls()

        filtered_timer_ctrls = Util.filter_items(
            lambda x: x.timer_handle == timer_callback.timer_handle, timer_ctrls)

        return TimerEventsFactory(filtered_timer_ctrls)

    @cached_property
    def tilde_publish_records(self) -> RecordsInterface:
        """
        Compose tilde publish records.

        Returns
        -------
        RecordsInterface
            columns:
            - tilde_publish_timestamp
            - tilde_publisher
            - tilde_message_id
            - tilde_subscription

        """
        records = self._data.tilde_publish
        records.rename_columns({'publisher': 'tilde_publisher'})

        subscription: List[int] = []
        for record in records:
            subscription_id = record.get('subscription_id')
            subscription.append(self._info.tilde_sub_id_map[subscription_id])

        records.append_column(ColumnValue('tilde_subscription'), subscription)
        records.drop_columns(['subscription_id'])
        return records

    @cached_property
    def tilde_subscribe_records(self) -> RecordsInterface:
        """
        Compose tilde subscribe records.

        Returns
        -------
        RecordsInterface
            columns:
            - tilde_subscribe_timestamp
            - tilde_subscription
            - tilde_message_id

        """
        records = self._data.tilde_subscribe
        records.rename_columns({'subscription': 'tilde_subscription'})
        return records

    @cached_property
    def intra_callback_records(self) -> RecordsInterface:
        intra_proc_subscribe = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('callback_start_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process'),
            ]
        )
        if 1 in self._grouped_callback_start:
            intra_callback_start = self._grouped_callback_start[1].clone()
            intra_proc_subscribe.concat(intra_callback_start)
        return intra_proc_subscribe

    @cached_property
    def inter_callback_records(self) -> RecordsInterface:
        intra_proc_subscribe = RecordsFactory.create_instance(
            None,
            [
                ColumnValue('callback_start_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process')
            ]
        )
        if 0 in self._grouped_callback_start:
            intra_callback_start = self._grouped_callback_start[0].clone()
            intra_proc_subscribe.concat(intra_callback_start)
        return intra_proc_subscribe

    @cached_property
    def subscribe_records(self) -> RecordsInterface:
        callback_start_instances = self.inter_callback_records
        inter_proc_subscribe = self._data.dispatch_subscription_callback_instances

        inter_proc_subscribe = merge_sequential(
            left_records=inter_proc_subscribe,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns.from_str(
                inter_proc_subscribe.columns + callback_start_instances.columns
            ).column_names,
            how='left',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )

        intra_proc_subscribe = self.intra_callback_records

        subscribe = merge_sequential(
            left_records=inter_proc_subscribe,
            right_records=intra_proc_subscribe,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns.from_str(
                inter_proc_subscribe.columns + intra_proc_subscribe.columns
            ).column_names,
            how='outer',
            progress_label='binding intra and inter subscribe'
        )

        return subscribe

    @cached_property
    def intra_proc_comm_records(self) -> RecordsInterface:
        """
        Compose intra process communication records.

        Used tracepoints
        - dispatch_intra_process_subscription_callback
        - rclcpp_publish
        - message_construct
        - callback_start

        Returns
        -------
        RecordsInterface
            columns:
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - message_timestamp

        """
        sink_records = self._data.dispatch_intra_process_subscription_callback_instances
        intra_publish_records = merge_sequential_for_addr_track(
            source_records=self._data.rclcpp_intra_publish_instances,
            copy_records=self._data.message_construct_instances,
            sink_records=sink_records,
            source_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.MESSAGE_CONSTRUCT_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ORIGINAL_MESSAGE,
            copy_to_key=COLUMN_NAME.CONSTRUCTED_MESSAGE,
            sink_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            sink_from_key=COLUMN_NAME.MESSAGE,
            columns=[
                'tid',
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.PUBLISHER_HANDLE,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
            ],
            progress_label='binding: publish_timestamp and message_addr',
        )

        # note: Incorrect latency is calculated when intra_publish of ros-rclcpp is included.
        intra_publish_records = merge(
            left_records=intra_publish_records,
            right_records=sink_records,
            join_left_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_right_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            columns=intra_publish_records.columns + [COLUMN_NAME.MESSAGE],
            how='inner'
        )

        intra_publish_records = merge_sequential(
            left_records=intra_publish_records,
            right_records=sink_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE,
            join_right_key=COLUMN_NAME.MESSAGE,
            columns=intra_publish_records.columns,
            how='left_use_latest'
        )

        callback_start_instances = self.intra_callback_records

        intra_records = merge_sequential(
            left_records=intra_publish_records,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns.from_str(
                intra_publish_records.columns +
                [
                    COLUMN_NAME.CALLBACK_START_TIMESTAMP, COLUMN_NAME.IS_INTRA_PROCESS,
                ]
            ).column_names,
            how='left',
            progress_label='binding: dispatch_subscription and callback_start',
        )

        intra_records.drop_columns(
            [
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )
        intra_records.rename_columns(
            {
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP
            }
        )

        return intra_records

    @cached_property
    def callback_records(self) -> RecordsInterface:
        """
        Compose callback records.

        Used tracepoints
        - callback_start
        - callback_end

        Returns
        -------
        RecordsInterface
            columns:
            - callback_start_timestamp
            - callback_end_timestamp
            - callback_object

        """
        records: RecordsInterface

        records = merge_sequential(
            left_records=self._data.callback_start_instances,
            right_records=self._data.callback_end_instances,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=[
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.CALLBACK_END_TIMESTAMP,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.IS_INTRA_PROCESS,
            ],
            how='inner',
            progress_label='binding: callback_start and callback_end'
        )

        records.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS
            ]
        )

        return records

    @cached_property
    def system_and_sim_times(self) -> RecordsInterface:
        return self._data.sim_time
