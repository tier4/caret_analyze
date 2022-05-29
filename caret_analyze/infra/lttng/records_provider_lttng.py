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

from logging import getLogger
from typing import Optional, Union

from .lttng import Lttng
from ...common import ClockConverter
from ...infra.interface import RuntimeDataProvider
from ...record import (
    RecordsFactory,
    RecordsInterface)
from ...value_objects import (
    CallbackStructValue,
    CommunicationStructValue,
    IntraProcessBufferStructValue,
    NodePathStructValue,
    PublisherStructValue,
    Qos,
    SubscriptionStructValue,
    TimerStructValue,
    TransformCommunicationStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    VariablePassingStructValue,
)

logger = getLogger(__name__)


class RecordsProviderLttng(RuntimeDataProvider):
    """
    Records are processed and measurement results are calculated.

    In addition to merging, filtering and other operations are performed here.

    """

    def __init__(
        self,
        lttng: Lttng,
    ) -> None:
        self._lttng = lttng

    def is_intra_process_communication(
        self,
        publisher: PublisherStructValue,
        subscription: SubscriptionStructValue,
    ) -> Optional[bool]:
        return self._lttng.is_intra_process_communication(publisher, subscription)

    def communication_records(
        self,
        comm_val: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Provide communication records.

        Parameters
        ----------
        comm_info : CommunicationStructInfo
            communicadtion info.

        Returns
        -------
        RecordsInterface
            Columns:
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_publish_timestamp (Optional)
            - [callback_name]/callback_start_timestamp

        """
        if self.is_intra_process_communication(comm_val.publisher, comm_val.subscription):
            return self._lttng.get_intra_proc_comm_records(comm_val)
        return self._lttng.get_inter_proc_comm_records(comm_val)

    def node_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:
        if node_path_val.message_context is None:
            # dummy record
            msg = 'message context is None. return dummy record. '
            msg += f'node_name: {node_path_val.node_name}'
            logger.info(msg)
            assert False
            return RecordsFactory.create_instance()

        return self._lttng.get_node_records(node_path_val)

    def callback_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        """
        Return callback duration records.

        Parameters
        ----------
        callback_val : CallbackStructValue
            target callback value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [callback_name]/callback_end_timestamp

        """
        return self._lttng.get_callback_records(callback)

    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_start_timestamp
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        Raises
        ------
        InvalidArgumentError

        """
        return self._lttng.get_subscribe_records(subscription)

    def ipc_buffer_records(
        self,
        ipc_buffer: IntraProcessBufferStructValue
    ) -> RecordsInterface:
        return self._lttng.get_ipc_buffer_records(ipc_buffer)

    def tf_broadcast_records(
        self,
        broadcaster: TransformFrameBroadcasterStructValue,
    ) -> RecordsInterface:
        """
        Compose transform broadcast records.

        Parameters
        ----------
        broadcaster : TransformBroadcasterStructValue
            target bradcaster
        transform : Optional[TransformValue]
            target transform

        Returns
        -------
        RecordsInterface
            Columns
            - frame_id
            - child_frame_id
            - same as publlish records

        """
        return self._lttng.get_send_transform(broadcaster)

    def tf_communication_records(
        self,
        communication: TransformCommunicationStructValue
    ) -> RecordsInterface:
        return self._lttng.get_inter_proc_tf_comm_records(communication)

    def tf_lookup_records(
        self,
        frame_buffer: TransformFrameBufferStructValue,
    ) -> RecordsInterface:
        return self._lttng.get_lookup_transform(frame_buffer)

    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publish : PublisherStructValue
            target publisher

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rclcpp_intra_publish_timestamp (Optional)
            - [topic_name]/rclcpp_inter_publish_timestamp (Optional)
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp (Optional)
            ---
            - [topic_name]/tilde_publish_timestamp (Optional)
            - [topic_name]/tilde_message_id (Optional)

        """
        return self._lttng.get_publish_records(publisher)

    def timer_records(self, timer: TimerStructValue) -> RecordsInterface:
        """
        Return timer records.

        Parameters
        ----------
        timer : TimerStructValue
            [description]

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/timer_event
            - [callback_name]/callback_start
            - [callback_name]/callback_end

        """
        return self._lttng.get_timer_callback(timer)

    def get_rmw_implementation(self) -> Optional[str]:
        return self._lttng.get_rmw_impl()

    def get_qos(
        self,
        pub_sub: Union[PublisherStructValue, SubscriptionStructValue]
    ) -> Qos:
        if isinstance(pub_sub, SubscriptionStructValue):
            return self._lttng.get_subscription_qos(pub_sub)
        elif isinstance(pub_sub, PublisherStructValue):
            return self._lttng.get_publisher_qos(pub_sub)

        raise NotImplementedError('')

    def get_sim_time_converter(self) -> ClockConverter:
        return self._lttng.get_sim_time_converter()

    def variable_passing_records(
        self,
        variable_passing_info: VariablePassingStructValue
    ) -> RecordsInterface:
        """
        Return variable passing records.

        Parameters
        ----------
        variable_passing_info : VariablePassingStructInfo
            target variable passing info.

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/callback_end_timestamp
            - [callback_name]/callback_start_timestamp

        """
        return self._lttng.get_var_pass_records(variable_passing_info)

    def _verify_trace_points(
        self,
        node_name: str,
        trace_points: str
    ) -> bool:
        df = self._lttng.get_count(['node_name', 'trace_point'])
        df = df.reset_index()
        node_df = df.loc[df['node_name'] == node_name]
        trace_point_df = node_df[node_df['trace_point'] == trace_points]
        if trace_point_df['size'].empty:
            return False
        elif trace_point_df['size'].item() <= 0:
            return False
        else:
            return True

    def verify_communication(
        self,
        communication: CommunicationStructValue,
    ) -> bool:
        is_intra_proc = self.is_intra_process_communication(
            communication.publisher, communication.subscription)

        if is_intra_proc is True:
            pub_node = communication.publish_node.node_name
            sub_node = communication.subscribe_node.node_name
            pub_result = self._verify_trace_points(
                pub_node, 'ros2:rclcpp_intra_publish')
            sub_result = self._verify_trace_points(
                sub_node,
                'ros2:dispatch_intra_process_subscription_callback'
            )

        elif is_intra_proc is False:
            pub_result = True
            sub_node = communication.subscribe_node.node_name
            sub_result = self._verify_trace_points(
                sub_node,
                'ros2:dispatch_subscription_callback'
            )

        if not pub_result:
            logger.warning(
                f"'caret/rclcpp' may not be used in publisher of '{pub_node}'.")
            return False
        if not sub_result:
            logger.warning(
                f"'caret/rclcpp' may not be used in subscriber of '{sub_node}'.")
            return False
        return True
