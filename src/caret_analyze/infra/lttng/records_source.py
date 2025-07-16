# Copyright 2021 TIER IV, Inc.
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

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property

from .column_names import COLUMN_NAME
from .events_factory import EventsFactory
from .lttng_info import LttngInfo
from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import TimerCallbackValueLttng, TimerControl, TimerInit
from ...common import Util
from ...record import (Columns,
                       ColumnValue,
                       merge, merge_sequential,
                       merge_sequential_for_addr_track,
                       RecordsFactory,
                       RecordsInterface)


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
    def _grouped_callback_start(self) -> dict[int, RecordsInterface]:
        records = self._data.callback_start_instances.clone()
        group: dict[int, RecordsInterface] = {}
        for k, v in records.groupby([COLUMN_NAME.IS_INTRA_PROCESS]).items():
            assert len(k) == 1
            group[k[0]] = v
        return group

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
        inter_proc_publish = self._data.rclcpp_publish_instances.clone()
        rcl_publish_records = self._data.rcl_publish_instances.clone()
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
            Created timer events factory.

        """
        class TimerEventsFactory(EventsFactory):

            def __init__(self, controls: Sequence[TimerControl]) -> None:
                self._controls = controls

            def create(self, until_ns: int) -> RecordsInterface:

                columns = [
                    ColumnValue(COLUMN_NAME.TIMER_EVENT_TIMESTAMP),
                ]

                records = RecordsFactory.create_instance(None, columns=columns)
                for control in self._controls:

                    if isinstance(control, TimerInit):
                        control.timestamp
                        timer_timestamp = control.timestamp
                        while timer_timestamp < until_ns:
                            record = {
                                COLUMN_NAME.TIMER_EVENT_TIMESTAMP: timer_timestamp,
                            }
                            records.append(record)
                            timer_timestamp = timer_timestamp+control.period_ns

                return records

        timer_controls = self._info.get_timer_controls()

        filtered_timer_controls = Util.filter_items(
            lambda x: x.timer_handle == timer_callback.timer_handle, timer_controls)

        return TimerEventsFactory(filtered_timer_controls)

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
        records = self._data.tilde_publish.clone()
        records.rename_columns({'publisher': 'tilde_publisher'})

        subscription: list[int] = []
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
        """
        Compose intra callback records.

        Returns
        -------
        RecordsInterface
            columns:

            - callback_start_timestamp
            - callback_object
            - is_intra_process

        """
        intra_proc_subscribe = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('callback_start_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process'),
            ]
        )
        if 1 in self._grouped_callback_start:
            intra_callback_start = self._grouped_callback_start[1].clone()
            intra_callback_start.drop_columns([COLUMN_NAME.TID])
            intra_proc_subscribe.concat(intra_callback_start)
        return intra_proc_subscribe

    @cached_property
    def inter_callback_records(self) -> RecordsInterface:
        """
        Compose inter callback records.

        Returns
        -------
        RecordsInterface
            columns:

            - tid
            - callback_start_timestamp
            - callback_object
            - is_intra_process

        """
        inter_proc_subscribe = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue('tid'),
                ColumnValue('callback_start_timestamp'),
                ColumnValue('callback_object'),
                ColumnValue('is_intra_process')
            ]
        )
        if 0 in self._grouped_callback_start:
            inter_callback_start = self._grouped_callback_start[0].clone()
            inter_proc_subscribe.concat(inter_callback_start)
        return inter_proc_subscribe

    @cached_property
    def subscribe_records(self) -> RecordsInterface:
        """
        Compose subscribe records.

        Returns
        -------
        RecordsInterface
            columns:

            - callback_start_timestamp
            - callback_object
            - is_intra_process
            - source_timestamp

        """
        callback_start_instances = self.inter_callback_records
        dispatch_sub_records = self._data.dispatch_subscription_callback_instances

        dispatch_sub_records = merge_sequential(
            left_records=dispatch_sub_records,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            columns=Columns.from_str(
                dispatch_sub_records.columns + callback_start_instances.columns
            ).column_names,
            how='left',
        )
        dispatch_sub_records.drop_columns(
            [
                COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
                COLUMN_NAME.TID,
                COLUMN_NAME.MESSAGE,
            ]
        )

        rmw_sub_records = self._data.rmw_take_instances.clone()
        rmw_sub_records.drop_columns([COLUMN_NAME.RMW_SUBSCRIPTION_HANDLE])
        rmw_sub_records = merge_sequential(
            left_records=rmw_sub_records,
            right_records=callback_start_instances,
            left_stamp_key=COLUMN_NAME.RMW_TAKE_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.TID,
            join_right_key=COLUMN_NAME.TID,
            columns=Columns.from_str(
                rmw_sub_records.columns + callback_start_instances.columns
            ).column_names,
            how='left',
        )
        rmw_sub_records.drop_columns(
            [
                COLUMN_NAME.TID,
                COLUMN_NAME.MESSAGE,
            ]
        )

        inter_proc_subscribe = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
                ColumnValue(COLUMN_NAME.IS_INTRA_PROCESS),
                ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
                ColumnValue(COLUMN_NAME.RMW_TAKE_TIMESTAMP),
            ]
        )
        inter_proc_subscribe.concat(dispatch_sub_records)
        inter_proc_subscribe.concat(rmw_sub_records)

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
        )

        return subscribe

    @cached_property
    def rmw_take_records(self) -> RecordsInterface:
        """
        Compose rmw_take records.

        Returns
        -------
        RecordsInterface
            columns:

            - tid
            - rmw_take_timestamp
            - rmw_subscription_handle
            - message
            - source_timestamp

        """
        rmw_take_records = self._data.rmw_take_instances.clone()
        return rmw_take_records

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

            (in the case of humble)

            columns:

            - tid
            - publisher_handle
            - callback_object
            - rclcpp_publish_timestamp
            - message_timestamp
            - callback_start_timestamp


            (in the case of iron and after)

            columns:

            - tid
            - publisher_handle
            - callback_object
            - rclcpp_publish_timestamp
            - callback_start_timestamp

        """
        if self._info.get_distribution()[0] >= 'i':
            return self.intra_proc_comm_records_iron

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
                COLUMN_NAME.TID,
                COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
                COLUMN_NAME.PUBLISHER_HANDLE,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
            ],
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
    def intra_proc_comm_records_iron(self) -> RecordsInterface:
        """
        Compose intra process communication records.

        Used tracepoints
        - rclcpp_intra_publish
        - rclcpp_ring_buffer_enqueue
        - rclcpp_ring_buffer_dequeue
        - callback_start

        Returns
        -------
        RecordsInterface
            columns:

            - tid
            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp

        """
        intra_pub = self._data.rclcpp_intra_publish_instances.clone()
        enq = self._data.rclcpp_ring_buffer_enqueue_instances.clone()
        deq = self._data.rclcpp_ring_buffer_dequeue_instances.clone()
        callback_start = self._data.callback_start_instances.clone()

        pub_records = merge_sequential(
            left_records=intra_pub,
            right_records=enq,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_RING_BUFFER_ENQUEUE_TIMESTAMP,
            join_left_key=COLUMN_NAME.TID,
            join_right_key=COLUMN_NAME.TID,
            columns=[
                COLUMN_NAME.TID,
                COLUMN_NAME.INDEX,
                COLUMN_NAME.BUFFER,
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                COLUMN_NAME.PUBLISHER_HANDLE,
                COLUMN_NAME.RCLCPP_RING_BUFFER_ENQUEUE_TIMESTAMP
            ],
            how='left_use_latest'
        )

        sub_records = merge_sequential(
            left_records=deq,
            right_records=callback_start,
            left_stamp_key=COLUMN_NAME.RCLCPP_RING_BUFFER_DEQUEUE_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.TID,
            join_right_key=COLUMN_NAME.TID,
            columns=[
                COLUMN_NAME.TID,
                COLUMN_NAME.INDEX,
                COLUMN_NAME.BUFFER,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.RCLCPP_RING_BUFFER_DEQUEUE_TIMESTAMP
            ],
            how='inner'
        )

        grouped_pub_records = pub_records.groupby(['buffer'])
        grouped_sub_records = sub_records.groupby(['buffer'])
        intra_records = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue(COLUMN_NAME.TID),
                ColumnValue(COLUMN_NAME.PUBLISHER_HANDLE),
                ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
                ColumnValue(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
            ]
        )

        for key in grouped_pub_records:
            if key in grouped_sub_records:
                pub = grouped_pub_records[key]
                sub = grouped_sub_records[key]
                intermediate_records = merge_sequential(
                    left_records=pub,
                    right_records=sub,
                    left_stamp_key=COLUMN_NAME.RCLCPP_RING_BUFFER_ENQUEUE_TIMESTAMP,
                    right_stamp_key=COLUMN_NAME.RCLCPP_RING_BUFFER_DEQUEUE_TIMESTAMP,
                    join_left_key='index',
                    join_right_key='index',
                    columns=[
                        COLUMN_NAME.TID,
                        COLUMN_NAME.PUBLISHER_HANDLE,
                        COLUMN_NAME.CALLBACK_OBJECT,
                        COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                        COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                    ],
                    how='left_use_latest'
                )
                intra_records.concat(intermediate_records)

        intra_records.drop_columns(
            [
                COLUMN_NAME.MESSAGE,
                COLUMN_NAME.MESSAGE_TIMESTAMP,
                COLUMN_NAME.RCLCPP_RING_BUFFER_ENQUEUE_TIMESTAMP,
                COLUMN_NAME.RCLCPP_RING_BUFFER_DEQUEUE_TIMESTAMP,
                COLUMN_NAME.BUFFER,
                COLUMN_NAME.INDEX,
                COLUMN_NAME.SIZE,
                COLUMN_NAME.OVERWRITTEN
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
        )

        records.drop_columns(
            [
                COLUMN_NAME.IS_INTRA_PROCESS,
                COLUMN_NAME.TID
            ]
        )

        return records

    @cached_property
    def path_beginning_records(self) -> RecordsInterface:
        """
        Compose callback records.

        Used to evaluate the beginning node of a path.

        Used tracepoints
        - callback_start
        - rclcpp_publish

        Returns
        -------
        RecordsInterface
            columns:

            - callback_start_timestamp
            - rclcpp_publish_timestamp
            - callback_object
            - publisher_handle

        """
        records_inter: RecordsInterface
        records_intra: RecordsInterface
        rclcpp_publish_records = self._data.rclcpp_publish_instances.clone()
        rcl_publish_records = self._data.rcl_publish_instances.clone()

        # Get the publish_handle from rcl_publish and add it to the rclcpp_publish record,
        # in case rclcpp_publish does not have a publisher_handle.
        if len(rcl_publish_records) != 0:
            rclcpp_publish_records = merge_sequential(
                left_records=rclcpp_publish_records,
                right_records=rcl_publish_records,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                join_left_key=COLUMN_NAME.MESSAGE,
                join_right_key=COLUMN_NAME.MESSAGE,
                columns=Columns.from_str(
                    rclcpp_publish_records.columns + [COLUMN_NAME.PUBLISHER_HANDLE]).column_names,
                how='left',
            )

        records_inter = merge_sequential(
            left_records=self._data.callback_start_instances,
            right_records=rclcpp_publish_records,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.TID,
            join_right_key=COLUMN_NAME.TID,
            columns=[
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.PUBLISHER_HANDLE
            ],
            how='left_use_latest',
        )
        records_inter.rename_columns(
            {COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}
        )

        records_intra = merge_sequential(
            left_records=self._data.callback_start_instances,
            right_records=self._data.rclcpp_intra_publish_instances,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.TID,
            join_right_key=COLUMN_NAME.TID,
            columns=[
                COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
                COLUMN_NAME.CALLBACK_OBJECT,
                COLUMN_NAME.PUBLISHER_HANDLE
            ],
            how='left_use_latest',
        )
        records_intra.rename_columns(
            {COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}
        )

        records_inter.concat(records_intra)

        return records_inter

    @cached_property
    def system_and_sim_times(self) -> RecordsInterface:
        return self._data.sim_time
