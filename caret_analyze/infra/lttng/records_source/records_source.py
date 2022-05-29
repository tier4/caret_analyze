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

from caret_analyze.value_objects.subscription import SubscriptionStructValue

from .callback_records import CallbackRecordsContainer
from .comm_records import CommRecordsContainer
from .ipc_buffer_records import IpcBufferRecordsContainer
from .node_records import NodeRecordsContainer
from .publish_records import PublishRecordsContainer
from .subscribe_records import SubscribeRecordsContainer
from .timer_records import TimerRecordsContainer
from .transform import (
    TransformCommRecordsContainer,
    TransformLookupContainer,
    TransformSendRecordsContainer,
    TransformSetRecordsContainer,
)
from .var_pass_records import VarPassRecordsContainer
from ..bridge import LttngBridge
from ..lttng_info import LttngInfo
from ..ros2_tracing.data_model import Ros2DataModel
from ....record import (
    RecordsInterface,
)
from ....value_objects import (
    CallbackStructValue,
    CommunicationStructValue,
    IntraProcessBufferStructValue,
    NodePathStructValue,
    PublisherStructValue,
    TimerStructValue,
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)


class RecordsSource():

    def __init__(
        self,
        data: Ros2DataModel,
        bridge: LttngBridge,
        info: LttngInfo,
    ) -> None:
        self._data = data
        self._info = info
        self._cb_records = CallbackRecordsContainer(bridge, data)
        self._sub_records = SubscribeRecordsContainer(bridge, data, self._cb_records)
        self._pub_records = PublishRecordsContainer(bridge, data)
        self._ipc_buffer_records = IpcBufferRecordsContainer(bridge, data)
        self._comm_records = CommRecordsContainer(
            bridge, data, self._sub_records, self._ipc_buffer_records, self._pub_records)
        self._tf_send_records = TransformSendRecordsContainer(
            bridge, data, info, self._pub_records)
        self._tf_set_records = TransformSetRecordsContainer(bridge, data, info, self._cb_records)
        self._tf_lookup_records = TransformLookupContainer(bridge, data, info)
        self._tf_comm_records = TransformCommRecordsContainer(
            bridge, data, info, self._tf_send_records,
            self._tf_set_records, self._tf_lookup_records
        )
        self._timer_records = TimerRecordsContainer(bridge, data, info, self._cb_records)
        self._var_pass_records = VarPassRecordsContainer(bridge, self._cb_records)
        self._node_records = NodeRecordsContainer(
            bridge, self._cb_records, self._sub_records, self._pub_records,
            self._tf_lookup_records, self._tf_send_records)

    def ipc_buffer_records(
        self,
        buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        return self._ipc_buffer_records.get_records(buffer)

    def intra_publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        return self._pub_records.get_intra_records(publisher)

    def inter_proc_comm_records(
        self,
        comm: CommunicationStructValue
    ) -> RecordsInterface:
        return self._comm_records.get_inter_records(comm)

    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        return self._pub_records.get_records(publisher)

    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        return self._sub_records.get_records(subscription)

    def send_transform_records(
        self,
        tf_broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:
        return self._tf_send_records.get_records(tf_broadcaster)

    def lookup_transform_records(
        self,
        tf_buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        records = self._tf_lookup_records.get_records(tf_buffer)
        records.columns.drop(['tf_buffer_core'])
        return records

    def get_inter_proc_tf_comm_records(
        self,
        comm: TransformCommunicationStructValue
    ) -> RecordsInterface:
        return self._tf_comm_records.get_inter_records(comm)

    def get_var_pass_records(
        self,
        var_pass: VariablePassingStructValue,
    ) -> RecordsInterface:
        return self._var_pass_records.get_records(var_pass)

    def get_timer_records(
        self,
        timer: TimerStructValue,
    ) -> RecordsInterface:
        return self._timer_records.get_records(timer)

    def get_node_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:
        return self._node_records.get_records(node_path_val)

    def intra_proc_comm_records(
        self,
        communication: CommunicationStructValue
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
        return self._comm_records.get_intra_records(communication)

    def callback_records(
        self,
        callback: CallbackStructValue,
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
        return self._data.sim_time.clone()
