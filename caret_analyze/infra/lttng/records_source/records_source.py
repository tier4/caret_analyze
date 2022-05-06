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

from typing import List, Sequence, Union, Collection
from caret_analyze.infra.lttng.value_objects.transform import TransformBufferValueLttng

from caret_analyze.record.column import ColumnMapper
from caret_analyze.value_objects.transform import TransformValue

from .callback_records import CallbackRecordsContainer
from .publish_records import PublishRecordsContainer
from .subscribe_records import SubscribeRecordsContainer
from .ipc_buffer_records import IpcBufferRecordsContainer
from .comm_records import CommRecordsContainer
from .node_records import NodeUseLatestMessageRecordsContainer
from .transform import (
    TransformLookupContainer, TransformSendRecordsContainer, TransformSetRecordsContainer,
    TransformCommRecordsContainer
)

from ..column_names import COLUMN_NAME
from ..events_factory import EventsFactory
from ..lttng_info import LttngInfo
from ..ros2_tracing.data_model import Ros2DataModel
from ..value_objects import (
    TimerCallbackValueLttng, TimerControl, TimerInit,
    PublisherValueLttng, SubscriptionCallbackValueLttng,
    IntraProcessBufferValueLttng,
    TransformBroadcasterValueLttng,
)
from ....common import Util
from ....record import (
    Column,
    merge_sequencial,
    RecordFactory,
    RecordsFactory,
    RecordsInterface,
)

from ....value_objects import (TransformValue)


class RecordsSource():

    def __init__(
        self,
        data: Ros2DataModel,
        info: LttngInfo
    ) -> None:
        self._data = data
        self._preprocess(self._data)
        self._info = info
        self._cb_records = CallbackRecordsContainer(data)
        self._sub_records = SubscribeRecordsContainer(data, self._cb_records)
        self._pub_records = PublishRecordsContainer(data)
        self._ipc_buffer_records = IpcBufferRecordsContainer(data)
        self._comm_records = CommRecordsContainer(
            data, self._sub_records, self._ipc_buffer_records, self._pub_records)
        self._node_records = NodeUseLatestMessageRecordsContainer(
            self._cb_records, self._pub_records)
        self._tf_send_records = TransformSendRecordsContainer(data, info, self._pub_records)
        self._tf_set_records = TransformSetRecordsContainer(data, info, self._cb_records)
        self._tf_lookup_records = TransformLookupContainer(data, info)
        self._tf_comm_records = TransformCommRecordsContainer(
            data, info, self._tf_send_records, self._tf_set_records, self._tf_lookup_records
        )

    @staticmethod
    def _preprocess(data: Ros2DataModel):
        data.rclcpp_publish.columns.rename(
            {COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP: COLUMN_NAME.RCLCPP_INTER_PUBLISH_TIMESTAMP}
        )

    def ipc_buffer_records(
        self,
        buffer: IntraProcessBufferValueLttng
    ) -> RecordsInterface:
        return self._ipc_buffer_records.get_records(buffer)

    def intra_publish_records(
        self,
        publisher: PublisherValueLttng
    ) -> RecordsInterface:
        return self._pub_records.get_intra_records(publisher)

    def inter_proc_comm_records(
        self,
        publisher: PublisherValueLttng,
        callback: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        return self._comm_records.get_inter_records(publisher, callback)

    def inter_publish_records(
        self,
        publisher: PublisherValueLttng
    ) -> RecordsInterface:
        return self._pub_records.get_inter_records(publisher)

    def publish_records(
        self,
        publisher: PublisherValueLttng
    ) -> RecordsInterface:
        return self._pub_records.get_records(publisher)

    def subscribe_records(
        self,
        subscription: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        return self._sub_records.get_records(subscription)

    def intra_subscribe_records(
        self,
        subscription: SubscriptionCallbackValueLttng
    ) -> RecordsInterface:
        return self._sub_records.get_intra_records(subscription)

    def send_transform_records(
        self,
        frame_mapper: ColumnMapper,
        tf_broadcaster: TransformBroadcasterValueLttng,
        transform: TransformValue,
    ) -> RecordsInterface:
        return self._tf_send_records.get_records(tf_broadcaster, transform)

    def lookup_transform_records(
        self,
        frame_mapper: ColumnMapper,
        tf_buffer: TransformBufferValueLttng,
        transform: TransformValue
    ) -> RecordsInterface:
        records = self._tf_lookup_records.get_records(tf_buffer, transform)
        records.columns.drop(['tf_buffer_core'])
        return records

    def set_transform_records(
        self,
        frame_mapper: ColumnMapper,
        tf_buffer: TransformBufferValueLttng,
        transform: TransformValue,
    ) -> RecordsInterface:
        return self._tf_set_records.get_records(tf_buffer, transform)

    def get_inter_proc_tf_comm_records(
        self,
        broadcaster: TransformBroadcasterValueLttng,
        listen_transform: TransformValue,
        buffer: TransformBufferValueLttng,
        lookup_transform: TransformValue,
    ) -> RecordsInterface:
        return self._tf_comm_records.get_inter_records(
            broadcaster, listen_transform, buffer, lookup_transform)

    def find_closest_records(
        self,
        frame_mapper: ColumnMapper,
        tf_buffer: TransformBufferValueLttng,
    ) -> RecordsInterface:
        raise NotImplementedError('')
        records = self._data.tf_buffer_find_closest.clone()

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

    def tilde_publish_records(
        self,
        tilde_publisher
    ) -> RecordsInterface:
        raise NotImplementedError('')
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

    def timer_callback(
        self,
        callback: TimerCallbackValueLttng,
    ) -> RecordsInterface:
        timer_events_factory = self._lttng.create_timer_events_factory(timer_lttng_cb)
        callback_records = self.callback_records(timer.callback)
        last_record = callback_records.data[-1]
        last_callback_start = last_record.get(callback_records.column_names[0])
        timer_events = timer_events_factory.create(last_callback_start)
        timer_records = merge_sequencial(
            left_records=timer_events,
            right_records=callback_records,
            left_stamp_key=COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            right_stamp_key=callback_records.column_names[0],
            join_left_key=None,
            join_right_key=None,
            how='left'
        )

    def node_use_latest_message(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng],
        publisher: PublisherValueLttng
    ) -> RecordsInterface:
        return self._node_records.get_use_latest_message_records(callback, publisher)

    def node_callback_chain(
        self,
        callbacks: Collection[Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]],
        publisher: PublisherValueLttng,
    ) -> RecordsInterface:
        return self._node_records.get_callback_chain(callbacks, publisher)

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
        raise NotImplementedError('')
        records = self._data.tilde_subscribe
        records.rename_columns({'subscription': 'tilde_subscription'})
        return records


    def intra_proc_comm_records(
        self,
        publisher: PublisherValueLttng,
        buffer: IntraProcessBufferValueLttng,
        subscription: SubscriptionCallbackValueLttng,
    ) -> RecordsInterface:
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
        return self._comm_records.get_intra_records(publisher, buffer, subscription)

    def get_comm_records(
        self,
        publisher: PublisherValueLttng,
        buffer: IntraProcessBufferValueLttng,
        subscription: SubscriptionCallbackValueLttng,
    ) -> RecordsInterface:
        return self._comm_records.get_records(publisher, buffer, subscription)

    def callback_records(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ) -> RecordsInterface:
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
        return self._cb_records.get_records(callback)

    @cached_property
    def system_and_sim_times(self) -> RecordsInterface:
        return self._data.sim_time





    def callback_records(
        self,
        callback: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ) -> RecordsInterface:
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
        return self._cb_records.get_records(callback)

    @cached_property
    def system_and_sim_times(self) -> RecordsInterface:
        return self._data.sim_time




