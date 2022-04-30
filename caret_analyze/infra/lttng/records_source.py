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

from functools import cached_property, lru_cache

from typing import Dict, List, Sequence, Tuple

from caret_analyze.record.column import ColumnMapper

from .column_names import COLUMN_NAME
from .events_factory import EventsFactory
from .lttng_info import LttngInfo
from .ros2_tracing.data_model import Ros2DataModel
from .value_objects import TimerCallbackValueLttng, TimerControl, TimerInit
from ...common import Util
from ...record import (Column, ColumnAttribute, merge, merge_sequencial,
                       merge_sequencial_for_addr_track,
                       RecordFactory,
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
        data.rclcpp_publish.rename_columns(
            {COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP}
        )

    @lru_cache
    def _grouped_callback_start(self) -> Tuple[List[Column], Dict[int, RecordsInterface]]:
        records = self._data.callback_start.clone()
        group: Dict[int, RecordsInterface] = {}
        for k, v in records.groupby([COLUMN_NAME.IS_INTRA_PROCESS]).items():
            assert len(k) == 1
            group[k[0]] = v
        return records.columns, group

    def ipc_buffer_records(self) -> RecordsInterface:
        columns = [
            'pid', 'enqueue_tid', 'enqueue_timestamp', 'buffer', 'message',
            'queued_msg_size', 'is_full', 'dequeue_tid', 'dequeue_timestamp',
            'dequeued_msg_size'
        ]

        join_keys = [
            COLUMN_NAME.PID,
            COLUMN_NAME.BUFFER,
            COLUMN_NAME.MESSAGE
        ]
        enqueue_records = self._data.ring_buffer_enqueue.clone()
        enqueue_records.rename_columns({
            'size': 'queued_msg_size',
            'tid': 'enqueue_tid',
        })

        dequeue_records = self._data.ring_buffer_dequeue.clone()
        dequeue_records.rename_columns({
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

        records.reindex(columns)
        return records

    def intra_publish_records(self) -> RecordsInterface:
        # join_keys = [COLUMN_NAME.PID, COLUMN_NAME.TID]
        # records = merge_sequencial(
        #     left_records=self._data.rclcpp_intra_publish.clone(),
        #     right_records=self._data.ring_buffer_enqueue.clone(),
        #     left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
        #     right_stamp_key=COLUMN_NAME.BUFFER_ENQUEUE_TIMESTAMP,
        #     join_left_key=join_keys,
        #     join_right_key=join_keys,
        #     how='left_use_latest'
        # )

        return self._data.rclcpp_intra_publish.clone()


    # @cached_property
    # def inter_proc_comm_records(self) -> RecordsInterface:
    #     """
    #     Compose inter process communication records.

    #     Used tracepoints
    #     - rclcpp_publish
    #     - dds_bind_addr_to_addr
    #     - dds_bind_addr_to_stamp
    #     - rcl_publish (Optional)
    #     - dds_write (Optional)
    #     - dispatch_subscription_callback
    #     - callback_start

    #     Returns
    #     -------
    #     RecordsInterface
    #         columns
    #         - callback_object
    #         - callback_start_timestamp
    #         - publisher_handle
    #         - rclcpp_publish_timestamp
    #         - rcl_publish_timestamp (Optional)
    #         - dds_write_timestamp (Optional)
    #         - message_timestamp
    #         - source_timestamp

    #     """
    #     publish = self._data.rclcpp_publish

    #     publish = merge_sequencial_for_addr_track(
    #         source_records=publish,
    #         copy_records=self._data.dds_bind_addr_to_addr,
    #         sink_records=self._data.dds_bind_addr_to_stamp,
    #         source_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         source_key=COLUMN_NAME.MESSAGE,
    #         copy_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_ADDR_TIMESTAMP,
    #         copy_from_key=COLUMN_NAME.ADDR_FROM,
    #         copy_to_key=COLUMN_NAME.ADDR_TO,
    #         sink_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #         sink_from_key=COLUMN_NAME.ADDR,
    #         progress_label='binding: message_addr and rclcpp_publish',
    #     )

    #     rcl_publish_records = self._data.rcl_publish
    #     rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
    #     if len(rcl_publish_records) > 0:
    #         publish = merge_sequencial(
    #             left_records=publish,
    #             right_records=rcl_publish_records,
    #             left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #             right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
    #             join_left_key=COLUMN_NAME.MESSAGE,
    #             join_right_key=COLUMN_NAME.MESSAGE,
    #             how='left',
    #             progress_label='binding: rclcpp_publish and rcl_publish',
    #         )

    #     dds_write = self._data.dds_write
    #     if len(dds_write) > 0:
    #         publish = merge_sequencial(
    #             left_records=publish,
    #             right_records=dds_write,
    #             left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #             right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
    #             join_left_key=COLUMN_NAME.MESSAGE,
    #             join_right_key=COLUMN_NAME.MESSAGE,
    #             how='left',
    #             progress_label='binding: rcl_publish and dds_write',
    #         )

    #     # When both intra_publish and inter_publish are used, value mismatch occurs when merging.
    #     # In order to merge at the latency time,
    #     # align the time to intra_publish if intra_process communication is used.
    #     intra_publish = self._data.rclcpp_intra_publish.clone()
    #     intra_publish.drop_columns([
    #         COLUMN_NAME.MESSAGE, COLUMN_NAME.MESSAGE_TIMESTAMP
    #     ])
    #     publish = merge_sequencial(
    #         left_records=intra_publish,
    #         right_records=publish,
    #         left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
    #         join_left_key=COLUMN_NAME.PUBLISHER_HANDLE,
    #         join_right_key=COLUMN_NAME.PUBLISHER_HANDLE,
    #         how='right'
    #     )
    #     rclcpp_publish: List[int] = []
    #     for record in publish.data:
    #         if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.data:
    #             rclcpp_publish.append(record.data[COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP])
    #         else:
    #             rclcpp_publish.append(
    #                 record.data[COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP])

    #     publish.append_column(
    #         Column(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP, [ColumnAttribute.SYSTEM_TIME]),
    #         rclcpp_publish)
    #     publish.drop_columns([
    #         COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
    #         COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP
    #     ])

    #     callback_start= self.inter_callback_records
    #     subscription = self._data.dispatch_subscription_callback

    #     subscription = merge_sequencial(
    #         left_records=subscription,
    #         right_records=callback_start,
    #         left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
    #         right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
    #         join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
    #         join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
    #         how='left',
    #         progress_label='binding: dispatch_subscription_callback and callback_start',
    #     )

    #     # communication = merge(
    #     #     publish,
    #     #     self._data.on_data_available,
    #     #     join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #     #     join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #     #     columns=publish.columns,
    #     #     how='left',
    #     #     progress_label='binding: source_timestamp and on_data_available',
    #     # )

    #     communication = merge(
    #         publish,
    #         subscription,
    #         join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #         join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
    #         how='left',
    #         progress_label='binding: source_timestamp and callback_start',
    #     )

    #     communication.drop_columns(
    #         [
    #             COLUMN_NAME.IS_INTRA_PROCESS,
    #             COLUMN_NAME.ADDR,
    #             COLUMN_NAME.MESSAGE,
    #             COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
    #             COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
    #         ],
    #     )

    #     return communication

    @cached_property
    def inter_publish_records(self) -> RecordsInterface:
        intra_proc_publish = self._data.rclcpp_intra_publish.clone()
        intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])
        # intra_proc_publish.drop_columns([COLUMN_NAME.MESSAGE])

        # When publishing to both intra-process and inter-process,
        # intra-process communication is done first.
        # On the other hand,
        # there are cases where only one or the other is done, so we use outer join.
        # https://github.com/ros2/rclcpp/blob/galactic/rclcpp/include/rclcpp/publisher.hpp#L203
        publish = merge_sequencial(
            left_records=intra_proc_publish,
            right_records=inter_proc_publish,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.PUBLISHER_HANDLE,
            join_right_key=COLUMN_NAME.PUBLISHER_HANDLE,
            how='outer',
            progress_label='binding intra_publish and inter_publish'
        )


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
        publish_records = self._data.rclcpp_publish

        rcl_publish_records = self._data.rcl_publish
        rcl_publish_records.drop_columns([COLUMN_NAME.PUBLISHER_HANDLE])
        if len(rcl_publish_records) > 0:
            publish_records = merge_sequencial(
                left_records=publish_records,
                right_records=rcl_publish_records,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                join_left_key='tid',
                join_right_key='tid',
                how='left',
                progress_label='binding: rclcpp_publish and rcl_publish',
            )

        dds_write = self._data.dds_write
        if len(dds_write) > 0:
            publish_records = merge_sequencial(
                left_records=publish_records,
                right_records=dds_write,
                left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.DDS_WRITE_TIMESTAMP,
                join_left_key='tid',
                join_right_key='tid',
                how='left',
                progress_label='binding: rclcppp_publish and dds_write',
            )

        publish_records = merge_sequencial(
            left_records=publish_records,
            right_records=self._data.dds_bind_addr_to_stamp,
            left_stamp_key=COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DDS_BIND_ADDR_TO_STAMP_TIMESTAMP,
            join_left_key='tid',
            join_right_key='tid',
            how='left',
            progress_label='binding: rclcppp_publish and source_timestamp',
        )

        # publish_stamps = []
        # maxsize = 2**64-1
        # for record in publish.data:
        #     rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
        #     if COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP in record.columns:
        #         rclcpp_publish = record.get(COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)
        #     if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.columns:
        #         rclcpp_intra_publish = record.get(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP)
        #     inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
        #     publish_stamps.append(inter_intra_publish)

        # publish.append_column(
        #     Column(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        #            [ColumnAttribute.SYSTEM_TIME]),
        #     publish_stamps)

        columns = []
        columns.append(COLUMN_NAME.PUBLISHER_HANDLE)
        columns.append(COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in publish_records.column_names:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in publish_records.column_names:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

        drop_columns = list(set(publish_records.column_names) - set(columns))
        publish_records.drop_columns(drop_columns)
        publish_records.reindex(columns)

        return publish_records

    @lru_cache
    def send_transform_records(
        self,
        frame_mapper: ColumnMapper
    ) -> RecordsInterface:
        records = self._data.send_transform

        frame_ids: List[int] = []
        child_frame_ids: List[int] = []
        for record in records:
            to_frame_id = self._info.get_tf_broadcaster_frame_compact_map(
                record.get('broadcaster'))
            frame_id = to_frame_id[record.get('frame_id_compact')]
            frame_ids.append(frame_mapper.get_key(frame_id))
            child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
            child_frame_ids.append(frame_mapper.get_key(child_frame_id))
        records.append_column(
            Column('frame_id', mapper=frame_mapper),
            frame_ids
        )
        records.append_column(
            Column('child_frame_id', mapper=frame_mapper),
            child_frame_ids
        )

        records.drop_columns(['frame_id_compact', 'child_frame_id_compact'])

        return records

    @lru_cache
    def lookup_transform_records(
        self,
        frame_mapper: ColumnMapper
    ) -> RecordsInterface:
        lookup_start = self._data.tf_lookup_transform_start
        lookup_end = self._data.tf_lookup_transform_end
        records = merge_sequencial(
            left_records=lookup_start,
            right_records=lookup_end,
            join_left_key='tf_buffer_core',
            join_right_key='tf_buffer_core',
            left_stamp_key='lookup_transform_start_timestamp',
            right_stamp_key='lookup_transform_end_timestamp',
            how='left'
        )

        frame_ids = []
        child_frame_ids = []
        for record in records:
            to_frame_id = self._info.get_tf_buffer_frame_compact_map(
                record.get('tf_buffer_core'))
            frame_id = to_frame_id[record.get('frame_id_compact')]
            frame_ids.append(frame_mapper.get_key(frame_id))
            child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
            child_frame_ids.append(frame_mapper.get_key(child_frame_id))
        records.append_column(
            Column('frame_id', mapper=frame_mapper),
            frame_ids
        )
        records.append_column(
            Column('child_frame_id', mapper=frame_mapper),
            child_frame_ids
        )

        records.drop_columns(['frame_id_compact', 'child_frame_id_compact'])
        return records
        # df = .to_dataframe()
        # end_df = self._data.tf_lookup_transform_end.to_dataframe()

        # cache_df = self._data.buffer_cache.reset_index()
        # mapper = self._info.get_tf_buffer_frame_mapper()
        # closest_df = self._data.tf_find_closest.to_dataframe()
        # set_df = self._data.tf_set_transform.to_dataframe()
        # TODO: 座標ごと、broadcaster毎で別々につくる。

        # raise NotImplementedError('')
        # return self._data.send_transform

    @lru_cache
    def set_transform_records(
        self,
        frame_mapper: ColumnMapper
    ) -> RecordsInterface:
        cb_start = self._data.callback_start.clone()
        set_tf = self._data.tf_set_transform.clone()
        cb_end = self._data.callback_end.clone()

        records = set_tf
        frame_ids = []
        child_frame_ids = []
        for record in records:
            to_frame_id = self._info.get_tf_buffer_frame_compact_map(
                record.get('tf_buffer_core'))
            frame_id = to_frame_id[record.get('frame_id_compact')]
            frame_ids.append(frame_mapper.get_key(frame_id))
            child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
            child_frame_ids.append(frame_mapper.get_key(child_frame_id))
        records.append_column(
            Column('frame_id', mapper=frame_mapper),
            frame_ids
        )
        records.append_column(
            Column('child_frame_id', mapper=frame_mapper),
            child_frame_ids
        )
        records.drop_columns(['frame_id_compact', 'child_frame_id_compact'])

        records = merge_sequencial(
            left_records=cb_start,
            right_records=set_tf,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key='set_transform_timestamp',
            join_left_key='tid',
            join_right_key='tid',
            how='right',
        )
        records = merge_sequencial(
            left_records=records,
            right_records=cb_end,
            left_stamp_key='set_transform_timestamp',
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key='tid',
            join_right_key='tid',
            how='left',
        )
        records.drop_columns([
            'tid',
            'callback_object',
            'is_intra_process',
        ])
        return records

    @lru_cache
    def find_closest_records(
        self,
        frame_mapper: ColumnMapper
    ) -> RecordsInterface:
        records = self._data.tf_find_closest.clone()

        frame_ids: List[int] = []
        child_frame_ids: List[int] = []
        for record in records:
            to_frame_id = self._info.get_tf_buffer_frame_compact_map(
                record.get('tf_buffer_core'))
            frame_id = to_frame_id[record.get('frame_id_compact')]
            frame_ids.append(frame_mapper.get_key(frame_id))
            child_frame_id = to_frame_id[record.get('child_frame_id_compact')]
            child_frame_ids.append(frame_mapper.get_key(child_frame_id))
        records.append_column(Column('frame_id', mapper=frame_mapper),
                              frame_ids
                              )
        records.append_column(
            Column('child_frame_id', mapper=frame_mapper),
            child_frame_ids
        )
        records.drop_columns(['frame_id_compact', 'child_frame_id_compact'])

        return records

    def create_timer_events_factory(
        self,
        timer_callback: TimerCallbackValueLttng
    ) -> EventsFactory:
        """
        Create tiemr events factory.

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
                    Column(COLUMN_NAME.TIMER_EVENT_TIMESTAMP),
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

        records.append_column(Column('tilde_subscription'), subscription)
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
        columns, group = self._grouped_callback_start()
        intra_proc_subscribe = RecordsFactory.create_instance(None, columns)
        if 1 in group:
            intra_callback_start = group[1].clone()
            intra_proc_subscribe.concat(intra_callback_start)
        return intra_proc_subscribe

    @cached_property
    def inter_callback_records(self) -> RecordsInterface:
        columns, group = self._grouped_callback_start()
        intra_proc_subscribe = RecordsFactory.create_instance(None, columns)
        if 0 in group:
            intra_callback_start = group[0].clone()
            intra_proc_subscribe.concat(intra_callback_start)
        return intra_proc_subscribe

    @cached_property
    def subscribe_records(self) -> RecordsInterface:
        callback_start = self.inter_callback_records
        inter_proc_subscrube = self._data.dispatch_subscription_callback

        inter_proc_subscrube = merge_sequencial(
            left_records=inter_proc_subscrube,
            right_records=callback_start,
            left_stamp_key=COLUMN_NAME.DISPATCH_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            how='left',
            progress_label='binding: dispatch_subscription_callback and callback_start',
        )

        intra_proc_subscribe = self.intra_callback_records

        subscribe = merge_sequencial(
            left_records=inter_proc_subscrube,
            right_records=intra_proc_subscribe,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            how='outer',
            progress_label='binding intra and inter subscribe'
        )

        return subscribe

    @cached_property
    def intra_subscribe_records(self) -> RecordsInterface:
        intra_proc_subscrube = self._data.dispatch_intra_process_subscription_callback

        subscribe = merge_sequencial(
            left_records=intra_proc_subscrube,
            right_records=self.intra_callback_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=[COLUMN_NAME.PID, COLUMN_NAME.TID, COLUMN_NAME.CALLBACK_OBJECT],
            join_right_key=[COLUMN_NAME.PID, COLUMN_NAME.TID, COLUMN_NAME.CALLBACK_OBJECT],
            how='left',
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
        sink_records = self._data.dispatch_intra_process_subscription_callback
        intra_publish_records = merge_sequencial_for_addr_track(
            source_records=self._data.rclcpp_intra_publish,
            copy_records=self._data.message_construct,
            sink_records=sink_records,
            source_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            source_key=COLUMN_NAME.MESSAGE,
            copy_stamp_key=COLUMN_NAME.MESSAGE_CONSTRUCT_TIMESTAMP,
            copy_from_key=COLUMN_NAME.ORIGINAL_MESSAGE,
            copy_to_key=COLUMN_NAME.CONSTRUCTED_MESSAGE,
            sink_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            sink_from_key=COLUMN_NAME.MESSAGE,
            progress_label='bindig: publish_timestamp and message_addr',
        )

        # note: Incorrect latency is calculated when intra_publish of ros-rclcpp is included.
        intra_publish_records = merge(
            left_records=intra_publish_records,
            right_records=sink_records,
            join_left_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_right_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            how='inner'
        )

        intra_publish_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=sink_records,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE,
            join_right_key=COLUMN_NAME.MESSAGE,
            how='left_use_latest'
        )

        callback_start= self.intra_callback_records

        intra_records = merge_sequencial(
            left_records=intra_publish_records,
            right_records=callback_start,
            left_stamp_key=COLUMN_NAME.DISPATCH_INTRA_PROCESS_SUBSCRIPTION_CALLBACK_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            join_left_key=COLUMN_NAME.CALLBACK_OBJECT,
            join_right_key=COLUMN_NAME.CALLBACK_OBJECT,
            how='left',
            progress_label='bindig: dispath_subsription and callback_start',
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

        join_keys = [COLUMN_NAME.PID, COLUMN_NAME.TID, COLUMN_NAME.CALLBACK_OBJECT]
        records = merge_sequencial(
            left_records=self._data.callback_start,
            right_records=self._data.callback_end,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.CALLBACK_END_TIMESTAMP,
            join_left_key=join_keys,
            join_right_key=join_keys,
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
