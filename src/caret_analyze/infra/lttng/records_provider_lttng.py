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
from logging import getLogger

from .bridge import LttngBridge
from .column_names import COLUMN_NAME
from .lttng import Lttng
from .ros2_tracing.data_model_service import DataModelService
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ..interface import RuntimeDataProvider
from ...common import ClockConverter, Util
from ...exceptions import (InvalidArgumentError,
                           UnsupportedNodeRecordsError,
                           UnsupportedTypeError)
from ...record import (Columns, ColumnValue,
                       merge, merge_sequential,
                       RecordsFactory, RecordsInterface)
from ...value_objects import (CallbackChain,
                              CallbackStructValue,
                              CommunicationStructValue,
                              InheritUniqueStamp,
                              MessageContext,
                              MessageContextType,
                              NodePathStructValue,
                              PublisherStructValue,
                              Qos,
                              SubscriptionCallbackStructValue,
                              SubscriptionStructValue,
                              Tilde,
                              TimerCallbackStructValue,
                              TimerStructValue,
                              UseLatestMessage,
                              VariablePassingStructValue)

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
        self._source = FilteredRecordsSource(lttng)
        self._helper = RecordsProviderLttngHelper(lttng)
        self._srv = DataModelService(lttng.data)

    def communication_records(
        self,
        comm_val: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Provide communication records.

        Parameters
        ----------
        comm_val : CommunicationStructValue
            communication value.

        Returns
        -------
        RecordsInterface
            Columns

            If inter-proc communication

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_publish_timestamp (Optional)
            - [topic_name]/source_timestamp
            - [callback_name]/callback_start_timestamp

            If intra-proc communication

            - [topic_name]/rclcpp_publish_timestamp
            - [callback_name]/callback_start_timestamp

        """
        assert comm_val.subscribe_callback_name is not None

        if self.is_intra_process_communication(comm_val):
            return self._compose_intra_proc_comm_records(comm_val)

        return self._compose_inter_proc_comm_records(comm_val)

    def communication_take_records(
        self,
        comm_val: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Provide communication take records.

        Parameters
        ----------
        comm_val : CommunicationStructValue
            communication value.

        Returns
        -------
        RecordsInterface
            Columns

            inter-proc communication only

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/source_timestamp
            - [node_name]/rmw_take_timestamp

        """
        assert comm_val.subscribe_callback_name is not None
        return self._compose_inter_proc_take_comm_records(comm_val)

    def node_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:
        """
        Provide node records.

        Parameters
        ----------
        node_path_val : NodePathStructValue
            Node path value.

        Returns
        -------
        RecordsInterface
            Record corresponding to specified message context type.

        Raises
        ------
        UnsupportedNodeRecordsError
            Occurs when message context type is unknown.

        """
        if node_path_val.message_context is None:
            # dummy record
            msg = 'message context is None. return dummy record. '
            msg += f'node_name: {node_path_val.node_name}'
            logger.info(msg)
            return RecordsFactory.create_instance()

        if node_path_val.message_context_type == MessageContextType.CALLBACK_CHAIN:
            return NodeRecordsCallbackChain(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.INHERIT_UNIQUE_STAMP:
            return NodeRecordsInheritUniqueTimestamp(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.USE_LATEST_MESSAGE:
            return NodeRecordsUseLatestMessage(self, node_path_val).to_records()

        if node_path_val.message_context_type == MessageContextType.TILDE:
            return NodeRecordsTilde(self, node_path_val).to_records()

        raise UnsupportedNodeRecordsError(
            'Unknown message context. '
            f'message_context = {node_path_val.message_context.context_type.type_name}'
        )

    def callback_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        """
        Return callback duration records.

        Parameters
        ----------
        callback : CallbackStructValue
            Target callback value.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_start_timestamp
            - [callback_name]/callback_end_timestamp

        """
        callback_objects = self._helper.get_callback_objects(callback)
        callback_records = self._source.callback_records(*callback_objects)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP
        ]
        self._format(callback_records, columns)
        self._rename_column(callback_records, callback.callback_name, None, None)
        callback_records.drop_columns([COLUMN_NAME.CALLBACK_OBJECT])

        return callback_records

    def subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_start_timestamp
            - [topic_name]/source_timestamp

        Raises
        ------
        InvalidArgumentError
            Occurs when callback value were not exist.

        """
        callback = subscription.callback
        assert callback is not None

        tilde_subscription = self._helper.get_tilde_subscription(callback)

        if tilde_subscription is None:
            return self._subscribe_records(subscription)

        return self._subscribe_records_with_tilde(subscription)

    def subscription_take_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        This method is implemented for nodes which receive messages
        by 'take' method instead of subscription callbacks.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/source_timestamp
            - [node_name]/rmw_take_timestamp

        Raises
        ------
        InvalidArgumentError
            Occurs when callback value were not exist.

        """
        callback = subscription.callback
        if callback is not None:
            callback_objects = self._helper.get_subscription_callback_objects(callback)

            try:
                rmw_handle =\
                    self._srv.get_rmw_subscription_handle_from_callback_object(callback_objects[0])
            except InvalidArgumentError:
                rmw_handle = None

        # get rmw_records, which relates to callback_object
        rmw_records: RecordsInterface
        if rmw_handle is not None and rmw_handle in self._source._grouped_rmw_take_records:
            rmw_records = self._source._grouped_rmw_take_records[rmw_handle].clone()
        else:
            rmw_records = RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.TID),
                    ColumnValue(COLUMN_NAME.RMW_TAKE_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.RMW_SUBSCRIPTION_HANDLE),
                    ColumnValue(COLUMN_NAME.MESSAGE),
                    ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP)
                    ]
                )

        # drop columns
        columns = rmw_records.columns
        drop_columns = list(
            set(columns) - {COLUMN_NAME.SOURCE_TIMESTAMP, COLUMN_NAME.RMW_TAKE_TIMESTAMP}
            )
        rmw_records.drop_columns(drop_columns)

        # reindex
        rmw_records.reindex([COLUMN_NAME.SOURCE_TIMESTAMP, COLUMN_NAME.RMW_TAKE_TIMESTAMP])

        # add prefix to columns; e.g. [topic_name]/source_timestamp
        self._rename_column(
            rmw_records,
            None if callback is None else callback.callback_name,
            subscription.topic_name,
            None
        )

        return rmw_records

    def _subscribe_records(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_start_timestamp
            - [topic_name]/source_timestamp

        Raises
        ------
        InvalidArgumentError
            Occurs when callback value were not exist.

        """
        callback = subscription.callback
        if callback is None:
            raise InvalidArgumentError(
                'callback_value is None. '
                f'node_name: {subscription.node_name}'
                f'callback_name: {subscription.callback_name}'
                f'topic_name: {subscription.topic_name}'
            )

        callback_objects = self._helper.get_subscription_callback_objects(callback)
        sub_records = self._source.sub_records(*callback_objects)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
        ]
        self._format(sub_records, columns)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            None
        )

        return sub_records

    def _subscribe_records_with_tilde(
        self,
        subscription: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_start_timestamp
            - [topic_name]/source_timestamp
            - [topic_name]/tilde_subscribe_timestamp
            - [topic_name]/tilde_message_id

        Raises
        ------
        InvalidArgumentError
            Occurs when callback value were not exist.

        """
        callback = subscription.callback
        if callback is None:
            raise InvalidArgumentError(
                'callback_value is None. '
                f'node_name: {subscription.node_name}'
                f'callback_name: {subscription.callback_name}'
                f'topic_name: {subscription.topic_name}'
            )

        callback_objects = self._helper.get_subscription_callback_objects(callback)
        sub_records = self._source.sub_records(*callback_objects)

        tilde_subscription = self._helper.get_tilde_subscription(callback)

        if tilde_subscription is not None:
            tilde_records = self._source.tilde_subscribe_records(tilde_subscription)

            sub_records = merge_sequential(
                left_records=sub_records,
                right_records=tilde_records,
                left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
                right_stamp_key=COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
                join_left_key=None,
                join_right_key=None,
                how='left',
                columns=Columns.from_str(
                    sub_records.columns + tilde_records.columns
                ).column_names,
            )

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
            COLUMN_NAME.TILDE_MESSAGE_ID,
        ]
        self._format(sub_records, columns)

        self._rename_column(
            sub_records,
            callback.callback_name,
            subscription.topic_name,
            None
        )

        return sub_records

    def _publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        """
        publisher_handles = self._helper.get_publisher_handles(publisher)
        pub_records = self._source.publish_records(publisher_handles)

        columns = []
        columns.append(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)

        self._format(pub_records, columns)
        self._rename_column(pub_records, None, publisher.topic_name, None)

        return pub_records

    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        RecordsInterface
            (in the case of tilde publisher)

            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp
            - [topic_name]/tilde_publish_timestamp
            - [topic_name]/tilde_message_id

            (for cases other than tilde publisher)

            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp

        """
        tilde_publishers = self._helper.get_tilde_publishers(publisher)
        if len(tilde_publishers) == 0:
            return self._publish_records(publisher)

        return self._publish_records_with_tilde(publisher)

    def _publish_records_with_tilde(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp
            - [topic_name]/tilde_publish_timestamp
            - [topic_name]/tilde_message_id

        """
        publisher_handles = self._helper.get_publisher_handles(publisher)
        pub_records = self._source.publish_records(publisher_handles)

        tilde_publishers = self._helper.get_tilde_publishers(publisher)
        tilde_records = self._source.tilde_publish_records(tilde_publishers)

        pub_records = merge_sequential(
            left_records=tilde_records,
            right_records=pub_records,
            left_stamp_key='tilde_publish_timestamp',
            right_stamp_key='rclcpp_publish_timestamp',
            join_left_key=None,
            join_right_key=None,
            columns=Columns.from_str(
                tilde_records.columns + pub_records.columns
            ).column_names,
            how='right',
        )

        columns = [
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in pub_records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns.append(COLUMN_NAME.MESSAGE_TIMESTAMP)
        columns.append(COLUMN_NAME.SOURCE_TIMESTAMP)
        columns.append(COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP)
        columns.append(COLUMN_NAME.TILDE_MESSAGE_ID)

        self._format(pub_records, columns)
        self._rename_column(pub_records, None, publisher.topic_name, None)

        return pub_records

    def timer_records(self, timer: TimerStructValue) -> RecordsInterface:
        """
        Return timer records.

        Parameters
        ----------
        timer : TimerStructValue
            Target timer.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/timer_event
            - [callback_name]/callback_start
            - [callback_name]/callback_end

        """
        assert timer.callback is not None
        timer_lttng_cb = self._helper.get_lttng_timer(timer.callback)

        timer_events_factory = self._lttng.create_timer_events_factory(timer_lttng_cb)
        callback_records = self.callback_records(timer.callback)
        if len(callback_records) == 0:
            records = RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.TIMER_EVENT_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.CALLBACK_END_TIMESTAMP),
                ]
            )
            self._rename_column(records, timer.callback_name, None, None)
            return records
        last_record = callback_records.data[-1]
        last_callback_start = last_record.get(callback_records.columns[0])
        timer_events = timer_events_factory.create(last_callback_start)
        timer_records = merge_sequential(
            left_records=timer_events,
            right_records=callback_records,
            left_stamp_key=COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            right_stamp_key=callback_records.columns[0],
            join_left_key=None,
            join_right_key=None,
            columns=Columns.from_str(
                timer_events.columns + callback_records.columns
            ).column_names,
            how='left'
        )

        columns = [
            COLUMN_NAME.TIMER_EVENT_TIMESTAMP,
            callback_records.columns[0],
            callback_records.columns[1],
        ]

        self._format(timer_records, columns)
        self._rename_column(timer_records, timer.callback_name, None, None)

        return timer_records

    def tilde_records(
        self,
        subscription: SubscriptionStructValue,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return tilde records.

        Parameters
        ----------
        subscription : SubscriptionStructValue
            Target subscription value.
        publisher : PublisherStructValue
            Target publisher value.

        Returns
        -------
        RecordsInterface
            Columns

            - tilde_subscribe_timestamp
            - tilde_publish_timestamp

        """
        assert subscription.callback is not None

        publisher_addresses = self._helper.get_tilde_publishers(publisher)
        subscription_addr = self._helper.get_tilde_subscription(subscription.callback)

        assert len(publisher_addresses) > 0
        assert subscription_addr is not None

        pub_records = self._source.tilde_publish_records(publisher_addresses)
        sub_records = self._source.tilde_subscribe_records(subscription_addr)

        records = merge(
            left_records=sub_records,
            right_records=pub_records,
            join_left_key=COLUMN_NAME.TILDE_MESSAGE_ID,
            join_right_key=COLUMN_NAME.TILDE_MESSAGE_ID,
            columns=Columns.from_str(
                sub_records.columns + pub_records.columns
            ).column_names,
            how='left',
        )

        columns = [
            COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP,
            COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP
        ]
        self._format(records, columns)

        self._rename_column_tilde(records, subscription.topic_name, publisher.topic_name)

        return records

    def get_rmw_implementation(self) -> str:
        """
        Get rmw implementation.

        Returns
        -------
        str
            Rmw implementation.

        """
        return self._lttng.get_rmw_impl()

    def get_qos(
        self,
        pub_sub: PublisherStructValue | SubscriptionStructValue
    ) -> Qos:
        """
        Get qos.

        Parameters
        ----------
        pub_sub : PublisherStructValue | SubscriptionStructValue
            Target subscription or publisher.

        Returns
        -------
        Qos
            Subscription qos or publisher qos.

        Raises
        ------
        InvalidArgumentError
            Occurs when callback were not exist.

        """
        if isinstance(pub_sub, SubscriptionStructValue):
            sub_cb = pub_sub.callback
            if sub_cb is None:
                raise InvalidArgumentError('Failed to get callback information.'
                                           'pub.callback is None')
            sub_cb_lttng = self._helper.get_lttng_subscription(sub_cb)
            return self._lttng.get_subscription_qos(sub_cb_lttng)

        pubs_lttng = self._helper.get_lttng_publishers(pub_sub)
        if len(pubs_lttng) == 0:
            raise InvalidArgumentError('No publisher matching the criteria was found.')
        if len(pubs_lttng) > 1:
            logger.warning(
                'Multiple publishers matching your criteria were found.'
                'The value of the first publisher qos will be returned.')

        return self._lttng.get_publisher_qos(pubs_lttng[0])

    def get_sim_time_converter(self, min_ns, max_ns) -> ClockConverter:
        """
        Get sim time converter.

        Parameters
        ----------
        min_ns : float
            Minimum timestamp value of the data
                used to create the system time to sim_time converter.
        max_ns : float
            Maximum timestamp value of the data
                used to create the system time to sim_time converter.

        Returns
        -------
        ClockConverter
            Object that converts timestamps from system time to sim_time.

        Raises
        ------
        InvalidArgumentError
            Failed to load sim_time.

        """
        return self._lttng.get_sim_time_converter(min_ns, max_ns)

    def variable_passing_records(
        self,
        variable_passing_info: VariablePassingStructValue
    ) -> RecordsInterface:
        """
        Return variable passing records.

        Parameters
        ----------
        variable_passing_info : VariablePassingStructValue
            Target variable passing info.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_end_timestamp
            - [callback_name]/callback_start_timestamp

        """
        read_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_read)
        write_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_write)

        read_records.drop_columns([read_records.columns[-1]])  # callback end
        write_records.drop_columns([write_records.columns[0]])  # callback_start

        columns = [
            write_records.columns[0],
            read_records.columns[0],
        ]

        merged_records = merge_sequential(
            left_records=write_records,
            right_records=read_records,
            left_stamp_key=columns[0],
            right_stamp_key=columns[1],
            join_left_key=None,
            join_right_key=None,
            columns=columns,
            how='left_use_latest',
        )

        merged_records.sort(columns[0])
        self._format(merged_records, columns)
        return merged_records

    def is_intra_process_communication(
        self,
        communication_value: CommunicationStructValue
    ) -> bool | None:
        """
        If inter-proc communication.

        Parameters
        ----------
        communication_value : CommunicationStructValue
            Communication value.

        Returns
        -------
        bool | None
            Intra process communication records count.

        """
        intra_record = self._compose_intra_proc_comm_records(communication_value)
        return len(intra_record) > 0

    def path_beginning_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return records from callback_start to publish.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        RecordsInterface
            Columns

            - [node_name]/callback_start_timestamp
            - [topic_name]/rclcpp_publish_timestamp

        """
        publisher_handles = self._helper.get_publisher_handles(publisher)
        records = self._source.path_beginning_records(publisher_handles)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        self._format(records, columns)
        self._rename_column(records, None, publisher.topic_name, publisher.node_name)
        return records

    def path_end_records(
        self,
        callback: CallbackStructValue
    ) -> RecordsInterface:
        """
        Return records from callback_start to callback_end.

        Parameters
        ----------
        callback : CallbackStructValue
            Target callback.

        Returns
        -------
        RecordsInterface
            Columns

            - [callback_name]/callback_start_timestamp
            - [callback_name]/callback_end_timestamp

        """
        callback_objects = self._helper.get_callback_objects(callback)
        records = self._source.callback_records(*callback_objects)

        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_END_TIMESTAMP,
        ]
        self._format(records, columns)
        self._rename_column(records, callback.callback_name, None, None)
        return records

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
        """
        Verify communication.

        Parameters
        ----------
        communication : CommunicationStructValue
            Communication value.

        Returns
        -------
        bool
            True if the trace points required to constitute a communication record are included
            in the trace data, false otherwise.

        """
        is_intra_proc = self.is_intra_process_communication(communication)
        if is_intra_proc is True:
            pub_node = communication.publish_node.node_name
            sub_node = communication.subscribe_node.node_name
            pub_result = self._verify_trace_points(pub_node, 'ros2:rclcpp_intra_publish')
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
            is_contains_rmw_take = self._verify_trace_points(
                sub_node,
                'ros2:rmw_take'
            )

        if not pub_result:
            logger.warning(f"'caret/rclcpp' may not be used in publisher of '{pub_node}'.")
            return False
        if not sub_result and not is_contains_rmw_take:
            logger.warning(f"'caret/rclcpp' may not be used in subscriber of '{sub_node}'.")
            return False
        return True

    def _compose_intra_proc_comm_records(
        self,
        comm_info: CommunicationStructValue,
    ) -> RecordsInterface:
        """
        Compose intra process communication records.

        Parameters
        ----------
        comm_info : CommunicationStructValue
            Target communication info.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [callback_name]/callback_start_timestamp

        """
        publisher = comm_info.publisher
        subscription_cb = comm_info.subscribe_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object_intra = self._helper.get_subscription_callback_object_intra(
            subscription_cb)

        records = self._source.intra_comm_records(publisher_handles, callback_object_intra)

        columns = [
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        ]
        self._format(records, columns)

        self._rename_column(records, comm_info.subscribe_callback_name, comm_info.topic_name, None)

        return records

    def _compose_inter_proc_comm_records(
        self,
        comm_value: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Composer inter process communication records.

        Parameters
        ----------
        comm_value : CommunicationStructValue
            Target communication value.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/source_timestamp
            - [callback_name]/callback_start_timestamp

        """
        publisher = comm_value.publisher
        subscription_cb = comm_value.subscribe_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object = self._helper.get_subscription_callback_object_inter(subscription_cb)

        records = self._source.inter_comm_records(publisher_handles, callback_object)

        columns = [COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns += [
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        ]

        self._format(records, columns)

        self._rename_column(records, comm_value.subscribe_callback_name,
                            comm_value.topic_name, None)

        return records

    def _compose_inter_proc_take_comm_records(
        self,
        comm_value: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Composer inter process communication records.

        Parameters
        ----------
        comm_value : CommunicationStructValue
            Target communication value.

        Returns
        -------
        RecordsInterface
            Columns

            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp (Optional)
            - [topic_name]/dds_write_timestamp (Optional)
            - [topic_name]/source_timestamp
            - [node_name]/rmw_take_timestamp

        """
        publisher = comm_value.publisher
        subscription_cb = comm_value.subscribe_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)

        callback = comm_value.subscription.callback
        rmw_handle = None
        if callback is not None:
            callback_objects = self._helper.get_subscription_callback_objects(callback)

            rmw_handle =\
                self._srv.get_rmw_subscription_handle_from_callback_object(callback_objects[0])
        else:
            raise InvalidArgumentError('comm_value subscription has no callbacks')

        if rmw_handle is not None:
            records = self._source.inter_take_comm_records(publisher_handles, rmw_handle)
        else:
            raise InvalidArgumentError(
                'Failed to get rmw subscription handle from callback object.'
                f'callback_object: {callback_objects[0]}'
            )

        columns = [COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in records.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns += [
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.RMW_TAKE_TIMESTAMP,
        ]

        self._format(records, columns)

        self._rename_column(records, comm_value.subscribe_callback_name,
                            comm_value.topic_name, comm_value.subscribe_node_name)
        return records

    @staticmethod
    def _format(records: RecordsInterface, columns: list[str]):
        drop = list(set(records.columns) - set(columns))
        records.drop_columns(drop)
        records.reindex(columns)

    @staticmethod
    def _rename_column(
        records: RecordsInterface,
        callback_name: str | None,
        topic_name: str | None,
        node_name: str | None
    ) -> None:
        rename_dict = {}

        if COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'

        if COLUMN_NAME.TIMER_EVENT_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TIMER_EVENT_TIMESTAMP] = \
                f'{callback_name}/{COLUMN_NAME.TIMER_EVENT_TIMESTAMP}'

        if COLUMN_NAME.CALLBACK_START_TIMESTAMP in records.columns:
            if callback_name is None:
                rename_dict[COLUMN_NAME.CALLBACK_START_TIMESTAMP] = \
                    f'{node_name}/callback_x/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'
            else:
                rename_dict[COLUMN_NAME.CALLBACK_START_TIMESTAMP] = \
                    f'{callback_name}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'

        if COLUMN_NAME.CALLBACK_END_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.CALLBACK_END_TIMESTAMP] = \
                f'{callback_name}/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'

        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.RCL_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}'

        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.DDS_WRITE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}'

        if COLUMN_NAME.MESSAGE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.MESSAGE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'

        if COLUMN_NAME.SOURCE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.SOURCE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}'

        if COLUMN_NAME.RMW_TAKE_TIMESTAMP in records.columns:
            if node_name:
                rename_dict[COLUMN_NAME.RMW_TAKE_TIMESTAMP] =\
                    f'{node_name}/{COLUMN_NAME.RMW_TAKE_TIMESTAMP}'
            else:
                rename_dict[COLUMN_NAME.RMW_TAKE_TIMESTAMP] =\
                    f'{topic_name}/{COLUMN_NAME.RMW_TAKE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_MESSAGE_ID in records.columns:
            rename_dict[COLUMN_NAME.TILDE_MESSAGE_ID] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_MESSAGE_ID}'

        if COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP] = \
                f'{topic_name}/{COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP}'

        records.rename_columns(rename_dict)

    @staticmethod
    def _rename_column_tilde(
        records: RecordsInterface,
        topic_name_sub: str,
        topic_name_pub: str
    ) -> None:
        rename_dict = {}

        if COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP] = \
                f'{topic_name_sub}/{COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP}'

        if COLUMN_NAME.TILDE_MESSAGE_ID in records.columns:
            rename_dict[COLUMN_NAME.TILDE_MESSAGE_ID] = \
                f'{topic_name_sub}/{COLUMN_NAME.TILDE_MESSAGE_ID}'

        if COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in records.columns:
            rename_dict[COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP] = \
                f'{topic_name_pub}/{COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP}'

        records.rename_columns(rename_dict)


class RecordsProviderLttngHelper:
    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        self._bridge = LttngBridge(lttng)

    def get_callback_objects(
        self,
        callback: CallbackStructValue
    ) -> tuple[int, int | None]:
        """
        Get callback objects.

        Parameters
        ----------
        callback : CallbackStructValue
            Target callback.

        Returns
        -------
        tuple[int, int | None]
            Isinstance of TimerCallbackStructValue or SubscriptionCallbackStructValue.

        Raises
        ------
        UnsupportedTypeError
            Argument callback object is not supported.

        """
        if isinstance(callback, TimerCallbackStructValue):
            return self.get_timer_callback_object(callback), None

        if isinstance(callback, SubscriptionCallbackStructValue):
            obj = self.get_subscription_callback_object_inter(callback)
            obj_intra = self.get_subscription_callback_object_intra(callback)
            if obj_intra is not None:
                return obj, obj_intra
            return obj, None

        msg = 'Failed to get callback object. '
        msg += f'{callback.callback_type.type_name} is not supported.'
        raise UnsupportedTypeError(msg)

    def get_timer_callback_object(
        self,
        callback: TimerCallbackStructValue
    ) -> int:
        """
        Get timer callback objects.

        Parameters
        ----------
        callback : TimerCallbackStructValue
            Target callback.

        Returns
        -------
        int
            Timer callback object.

        """
        callback_lttng = self._bridge.get_timer_callback(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_objects(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> tuple[int, int | None]:
        """
        Get subscription callback objects.

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Target callback.

        Returns
        -------
        tuple[int, int | None]
            Subscription callback object.

        """
        return self.get_callback_objects(callback)

    def get_subscription_callback_object_inter(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> int:
        """
        Get subscription callback objects.

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Target callback.

        Returns
        -------
        int
            Subscription callback object.

        """
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_object_intra(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> int | None:
        """
        Get intra subscription callback objects.

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Target callback.

        Returns
        -------
        int | None
            Intra subscription callback object.

        """
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.callback_object_intra

    def get_tilde_subscription(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> int | None:
        """
        Get tilde subscription callback objects.

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Target callback.

        Returns
        -------
        int | None
            Tilde subscription callback object.

        """
        callback_lttng = self._bridge.get_subscription_callback(callback)
        return callback_lttng.tilde_subscription

    def get_publisher_handles(
        self,
        publisher: PublisherStructValue
    ) -> list[int]:
        """
        Get publisher callback objects.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        list[int]
            Publisher handles.

        """
        publisher_lttng = self._bridge.get_publishers(publisher)
        return [pub_info.publisher_handle
                for pub_info
                in publisher_lttng]

    def get_tilde_publishers(
        self,
        publisher_info: PublisherStructValue
    ) -> list[int]:
        """
        Get tilde publisher.

        Parameters
        ----------
        publisher_info : PublisherStructValue
            Target publisher info.

        Returns
        -------
        list[int]
            Tilde publisher.

        """
        publisher_lttng = self._bridge.get_publishers(publisher_info)
        publisher = [pub_info.tilde_publisher
                     for pub_info
                     in publisher_lttng
                     if pub_info.tilde_publisher is not None]
        return publisher

    def get_lttng_publishers(
        self,
        publisher: PublisherStructValue
    ) -> list[PublisherValueLttng]:
        """
        Get lttng publishers.

        Parameters
        ----------
        publisher : PublisherStructValue
            Target publisher.

        Returns
        -------
        list[PublisherValueLttng]
            Publisher values of Lttng.

        """
        return self._bridge.get_publishers(publisher)

    def get_lttng_subscription(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> SubscriptionCallbackValueLttng:
        """
        Get lttng subscription.

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Target subscription callback.

        Returns
        -------
        SubscriptionCallbackValueLttng
            Subscription callback values of Lttng.

        """
        return self._bridge.get_subscription_callback(callback)

    def get_lttng_timer(
        self,
        callback: TimerCallbackStructValue
    ) -> TimerCallbackValueLttng:
        """
        Get lttng timer.

        Parameters
        ----------
        callback : TimerCallbackStructValue
            Target timer callback.

        Returns
        -------
        TimerCallbackValueLttng
            Timer callback values of Lttng.

        """
        return self._bridge.get_timer_callback(callback)


class NodeRecordsCallbackChain:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:

        self._provider = provider
        self._validate(node_path)
        self._val = node_path

    def to_records(self):
        assert self._val.child is not None

        chain_info = self._val.child

        if isinstance(chain_info[0], CallbackStructValue):
            cb_info = chain_info[0]
            records = self._provider.callback_records(cb_info)
        else:
            var_pass_info = chain_info[0]
            records = self._provider.variable_passing_records(var_pass_info)

        for chain_element in chain_info[1:]:
            if isinstance(chain_element, CallbackStructValue):
                records_ = self._provider.callback_records(chain_element)
                join_key = records_.columns[0]
                records = merge(
                    left_records=records,
                    right_records=records_,
                    join_left_key=join_key,
                    join_right_key=join_key,
                    columns=Columns.from_str(
                        records.columns + records_.columns
                    ).column_names,
                    how='left',
                )
                continue

            if isinstance(chain_element, VariablePassingStructValue):
                records_ = self._provider.variable_passing_records(chain_element)
                # self._rename_var_pass_records(records_, chain_element)
                join_key = records_.columns[0]
                records = merge(
                    left_records=records,
                    right_records=records_,
                    join_left_key=join_key,
                    join_right_key=join_key,
                    columns=Columns.from_str(
                        records.columns + records_.columns
                    ).column_names,
                    how='left',
                )
                continue

        last_element = chain_info[-1]
        if isinstance(last_element, CallbackStructValue) \
                and self._val.publisher is not None:
            last_callback_end_name = Util.filter_items(
                lambda x: COLUMN_NAME.CALLBACK_END_TIMESTAMP in x, records.columns)[-1]
            records.drop_columns([last_callback_end_name])
            last_callback_start_name = Util.filter_items(
                lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, records.columns)[-1]

            publish_records = self._provider.publish_records(self._val.publisher)
            publish_column = publish_records.columns[0]
            columns = records.columns + [publish_column]
            records = merge_sequential(
                left_records=records,
                right_records=publish_records,
                join_left_key=None,
                join_right_key=None,
                left_stamp_key=last_callback_start_name,
                right_stamp_key=publish_column,
                columns=Columns.from_str(
                    records.columns + publish_records.columns
                ).column_names,
                how='left',
            )
            records.drop_columns(list(set(records.columns) - set(columns)))
            records.reindex(columns)
        return records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('')

        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('callback values is None.')

        if not isinstance(node_path.message_context, CallbackChain):
            msg = 'node_path.message context is not CallbackChain'
            raise UnsupportedNodeRecordsError(msg)

        head_callback = node_path.callbacks[0]
        tail_callback = node_path.callbacks[-1]

        if node_path.publish_topic_name is not None and \
                tail_callback.publish_topics is not None:
            is_valid = False
            for topic in tail_callback.publish_topics:
                if node_path.publish_topic_name == topic.topic_name and \
                   node_path.publisher_construction_order == topic.construction_order:
                    is_valid = True
            if is_valid is not True:
                raise UnsupportedNodeRecordsError('')

        if node_path.subscribe_topic_name is not None:
            if node_path.subscribe_topic_name != head_callback.subscribe_topic_name or \
               node_path.subscription_construction_order != head_callback.construction_order:
                raise UnsupportedNodeRecordsError('')


class NodeRecordsInheritUniqueTimestamp:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, InheritUniqueStamp):
            msg = 'node_path.message context is not InheritUniqueStamp'
            raise UnsupportedNodeRecordsError(msg)

        self._provider = provider
        self._context: InheritUniqueStamp = node_path.message_context
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        assert self._node_path.subscription is not None and self._node_path.publisher is not None

        sub_records = self._provider.subscribe_records(self._node_path.subscription)
        pub_records = self._provider.publish_records(self._node_path.publisher)

        columns = [
            sub_records.columns[0],
            pub_records.columns[0],
        ]

        join_left_key = f'{self._node_path.subscribe_topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'
        join_right_key = f'{self._node_path.publish_topic_name}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'
        pub_sub_records = merge_sequential(
            left_records=sub_records,
            right_records=pub_records,
            left_stamp_key=sub_records.columns[0],
            right_stamp_key=pub_records.columns[0],
            join_left_key=join_left_key,
            join_right_key=join_right_key,
            columns=Columns.from_str(
                sub_records.columns + pub_records.columns
            ).column_names,
            how='left_use_latest',
        )

        drop_columns = list(set(pub_sub_records.columns) - set(columns))
        pub_sub_records.drop_columns(drop_columns)
        pub_sub_records.reindex(columns)
        return pub_sub_records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: InheritUniqueStamp,
    ) -> None:
        def is_valid() -> bool:
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False

            return True

        if is_valid():
            return None

        msg = f'InheritUniqueStamp cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class NodeRecordsUseLatestMessage:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, UseLatestMessage):
            raise UnsupportedNodeRecordsError('node_path.message context is not UseLatestMessage')

        self._provider = provider
        self._context: UseLatestMessage = node_path.message_context

        try:
            self._validate(node_path, self._context)
        except UnsupportedNodeRecordsError as e:
            raise UnsupportedNodeRecordsError(e.message)
        self._node_path = node_path

    def to_records(self):
        assert self._node_path.subscription is not None and self._node_path.publisher is not None

        sub_records = self._provider.subscribe_records(self._node_path.subscription)

        # If explicitly take message by user, there are cases that source_timestamp is 0.
        def fill_source_timestamp_with_latest_timestamp(records):
            source_columns = [s for s in records.columns if COLUMN_NAME.SOURCE_TIMESTAMP in s]
            if len(source_columns) != 1:
                return records
            source_column = source_columns[0]

            columns = []
            for column in records.columns:
                columns += [ColumnValue(column)]

            records_data = []
            latest_timestamp = 0

            for record in records.data:
                source_timestamp = record.data[source_column]
                if source_timestamp == 0:
                    record_dict = record.data
                    record_dict[source_column] = latest_timestamp
                    records_data.append(record_dict)
                else:
                    latest_timestamp = source_timestamp
                    records_data.append(record.data)

            new_records = RecordsFactory.create_instance(
                records_data,
                columns=columns
            )
            return new_records

        is_take_node = len(sub_records) == 0
        if is_take_node:
            sub_records = self._provider.subscription_take_records(self._node_path.subscription)
            # source_timestamp of rmw_take is 0 when no message is taken.
            # We replace this 0 with the source_timestamp of preceding record because
            # typical node which 'take' messages (instead of using subscription callback)
            # use last message when no message is taken.
            sub_records = fill_source_timestamp_with_latest_timestamp(sub_records)
        pub_records = self._provider.publish_records(self._node_path.publisher)

        columns = [
            *sub_records.columns,
            f'{self._node_path.publish_topic_name}/rclcpp_publish_timestamp',
        ]
        left_key = sub_records.columns[0]

        # Set left_key to rmw_take timestamp
        # if sub_records are obtained by RecordsProviderLttng.subscription_take_records()
        if is_take_node:
            for column in sub_records.columns:
                if column.endswith(COLUMN_NAME.RMW_TAKE_TIMESTAMP):
                    columns.remove(column)
                    left_key = column
                    break

        pub_sub_records = merge_sequential(
            left_records=sub_records,
            right_records=pub_records,
            left_stamp_key=left_key,
            right_stamp_key=pub_records.columns[0],
            join_left_key=None,
            join_right_key=None,
            columns=Columns.from_str(
                sub_records.columns + pub_records.columns
            ).column_names,
            how='left_use_latest',
        )

        drop_columns = list(set(pub_sub_records.columns) - set(columns))
        pub_sub_records.drop_columns(drop_columns)
        pub_sub_records.reindex(columns)
        return pub_sub_records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: UseLatestMessage,
    ) -> None:
        def is_valid() -> bool:
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False
            if context.publisher_construction_order != node_path.publisher_construction_order:
                return False
            if context.subscription_construction_order != \
               node_path.subscription_construction_order:
                return False

            return True

        if is_valid():
            return None

        msg = f'UseLatest cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class NodeRecordsTilde:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.message_context is None:
            raise UnsupportedNodeRecordsError('node_path.message context is None')
        if not isinstance(node_path.message_context, Tilde):
            raise UnsupportedNodeRecordsError('node_path.message context is not UseLatestMessage')

        self._provider = provider
        self._context: MessageContext = node_path.message_context
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        assert self._node_path.subscription is not None and self._node_path.publisher is not None

        tilde_records = self._provider.tilde_records(
            self._node_path.subscription, self._node_path.publisher)
        sub_records = self._provider.subscribe_records(self._node_path.subscription)
        pub_records = self._provider.publish_records(self._node_path.publisher)

        left_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, sub_records.columns)
        right_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP in x, sub_records.columns)

        records = merge(
            left_records=sub_records,
            right_records=tilde_records,
            join_left_key=right_stamp_key,
            join_right_key=right_stamp_key,
            columns=Columns.from_str(
                sub_records.columns + tilde_records.columns
            ).column_names,
            how='left',
        )

        left_stamp_key = Util.find_one(
            lambda x: COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP in x, records.columns)

        records = merge(
            left_records=records,
            right_records=pub_records,
            join_left_key=left_stamp_key,
            join_right_key=left_stamp_key,
            columns=Columns.from_str(
                records.columns + pub_records.columns
            ).column_names,
            how='left',
        )

        columns = [
            Util.find_one(lambda x: COLUMN_NAME.CALLBACK_START_TIMESTAMP in x, records.columns),
            Util.find_one(lambda x: COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in x, records.columns),
        ]

        drop_columns = list(set(records.columns) - set(columns))
        records.drop_columns(drop_columns)
        records.reindex(columns)
        return records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
        context: MessageContext,
    ) -> None:
        def is_valid() -> bool:
            if not isinstance(context, Tilde):
                return False
            if context.publisher_topic_name != node_path.publish_topic_name:
                return False
            if context.subscription_topic_name != node_path.subscribe_topic_name:
                return False

            return True

        if is_valid():
            return None

        msg = f'UseLatest cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)


class FilteredRecordsSource:

    def __init__(self, lttng: Lttng):
        self._lttng = lttng

    def tilde_subscribe_records(
        self,
        tilde_subscription: int
    ) -> RecordsInterface:
        """
        Compose filtered tilde subscribe records.

        Parameters
        ----------
        tilde_subscription : int
            TILDE subscription

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_tilde_subscribe_records()
            records.filter_if(
                lambda x: x.get('tilde_subscription') == tilde_subscription
            )
            records.drop_columns(['tilde_subscription])

            Columns

            - [topic_name]/tilde_subscribe_timestamp
            - [topic_name]/tilde_subscription
            - [topic_name]/tilde_message_id

        """
        grouped_records = self._grouped_tilde_sub_records
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.TILDE_SUBSCRIBE_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.TILDE_SUBSCRIPTION),
                    ColumnValue(COLUMN_NAME.TILDE_MESSAGE_ID),
                ]
            )
        sample_records = list(grouped_records.values())[0]
        column_values = Columns.from_str(sample_records.columns).to_value()
        sub_records = RecordsFactory.create_instance(None, columns=column_values)

        if tilde_subscription is not None and tilde_subscription in grouped_records:
            sub_records_ = grouped_records[tilde_subscription].clone()
            sub_records.concat(sub_records_)

        sub_records.drop_columns([COLUMN_NAME.TILDE_SUBSCRIPTION])
        return sub_records

    def sub_records(
        self,
        inter_callback_object: int,
        intra_callback_object: int | None
    ) -> RecordsInterface:
        """
        Compose filtered subscribe records.

        Parameters
        ----------
        inter_callback_object : int
            inter callback object
        intra_callback_object : int | None
            intra callback object

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_subscribe_records()
            records.filter_if(
                lambda x: x.get('callback_object') in [
                    inter_callback_object, intra_callback_object
                ]
            )

            columns:

            - callback_start_timestamp
            - callback_object
            - is_intra_process
            - source_timestamp

        """
        grouped_records = self._grouped_sub_records
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
                    ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
                ]
            )
        sample_records = list(grouped_records.values())[0]
        column_values = Columns.from_str(sample_records.columns).to_value()
        sub_records = RecordsFactory.create_instance(None, columns=column_values)

        if inter_callback_object in grouped_records:
            sub_records.concat(grouped_records[inter_callback_object].clone())

        if intra_callback_object is not None and intra_callback_object in grouped_records:
            intra_sub_records = grouped_records[intra_callback_object].clone()
            sub_records.concat(intra_sub_records)
            sub_records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        return sub_records

    def inter_comm_records(
        self,
        publisher_handles: list[int],
        callback_object: int
    ) -> RecordsInterface:
        """
        Compose filtered inter communication records.

        Parameters
        ----------
        publisher_handles : list[int]
            publisher handles
        callback_object : int
            callback object

        Returns
        -------
        RecordsInterface
            columns:

            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp (Optional)
            - dds_write_timestamp (Optional)
            - message_timestamp
            - source_timestamp
            - rmw_take_timestamp (Optional)

        """
        pub_records = self.publish_records(publisher_handles)
        sub_records = self.sub_records(callback_object, None)

        merged = merge(
            left_records=pub_records,
            right_records=sub_records,
            join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            columns=Columns.from_str(
                pub_records.columns + sub_records.columns
            ).column_names,
            how='left'
        )

        columns = [
            COLUMN_NAME.CALLBACK_OBJECT,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in merged.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in merged.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns += [
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP
        ]
        if COLUMN_NAME.RMW_TAKE_TIMESTAMP in merged.columns:
            columns.append(COLUMN_NAME.RMW_TAKE_TIMESTAMP)
        drop = list(set(merged.columns) - set(columns))
        merged.drop_columns(drop)
        merged.reindex(columns)

        # NOTE: After merge, the dropped data are aligned at the end
        # regardless of the time of publish.
        merged.sort(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        return merged

    def inter_take_comm_records(
        self,
        publisher_handles: list[int],
        rmw_handle: int
    ) -> RecordsInterface:
        """
        Compose filtered inter communication records.

        Parameters
        ----------
        publisher_handles : list[int]
            publisher handles
        rmw_handle : int
            rmw_take subscription handle

        Returns
        -------
        RecordsInterface
            columns:

            - publisher_handle
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp (Optional)
            - dds_write_timestamp (Optional)
            - message_timestamp
            - source_timestamp
            - rmw_take_timestamp

        """
        pub_records = self.publish_records(publisher_handles)
        rmw_records = self._grouped_rmw_take_records[rmw_handle].clone()
        rmw_records.drop_columns([
            'rmw_subscription_handle'])

        merged = merge(
            left_records=pub_records,
            right_records=rmw_records,
            join_left_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            join_right_key=COLUMN_NAME.SOURCE_TIMESTAMP,
            columns=Columns.from_str(
                pub_records.columns + rmw_records.columns
            ).column_names,
            how='left'
        )

        columns = [
            COLUMN_NAME.PUBLISHER_HANDLE,
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
        ]
        if COLUMN_NAME.RCL_PUBLISH_TIMESTAMP in merged.columns:
            columns.append(COLUMN_NAME.RCL_PUBLISH_TIMESTAMP)
        if COLUMN_NAME.DDS_WRITE_TIMESTAMP in merged.columns:
            columns.append(COLUMN_NAME.DDS_WRITE_TIMESTAMP)
        columns += [
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.RMW_TAKE_TIMESTAMP,
        ]
        drop = list(set(merged.columns) - set(columns))
        merged.drop_columns(drop)
        merged.reindex(columns)

        # NOTE: After merge, the dropped data are aligned at the end
        # regardless of the time of publish.
        merged.sort(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        return merged

    def intra_comm_records(
        self,
        publisher_handles: list[int],
        intra_callback_object: int | None
    ) -> RecordsInterface:
        """
        Compose filtered intra communication records.

        Parameters
        ----------
        publisher_handles : list[int]
            publisher handles
        intra_callback_object : int | None
            intra callback object

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_intra_proc_comm_records()
            records.filter_if(
                lambda x: x.get('callback_object') == callback_object and
                          x.get('publisher_handle') in publisher_handles
            )

            columns:

            - callback_object
            - callback_start_timestamp
            - publisher_handle
            - rclcpp_publish_timestamp
            - message_timestamp


        """
        grouped_records = self._grouped_intra_comm_records

        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
                    ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.PUBLISHER_HANDLE),
                    ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.MESSAGE_TIMESTAMP),
                ]
            )

        sample_records = list(grouped_records.values())[0]
        column_values = Columns.from_str(sample_records.columns).to_value()
        records = RecordsFactory.create_instance(None, columns=column_values)

        if intra_callback_object is not None:
            for publisher_handle in publisher_handles:
                key = (intra_callback_object, publisher_handle)
                if key in grouped_records:
                    records_ = grouped_records[key].clone()
                    records.concat(records_)
        records.sort(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        return records

    def publish_records(
        self,
        publisher_handles: list[int],
    ) -> RecordsInterface:
        """
        Compose publish records.

        Parameters
        ----------
        publisher_handles : list[int]
            publisher handles

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_publish_records()
            records.filter_if(
                lambda x: x.get('publisher_handle') in publisher_handles
                ]
            )

        Columns

        - publisher_handle
        - rclcpp_publish_timestamp
        - rcl_publish_timestamp (Optional)
        - dds_write_timestamp (Optional)
        - message_timestamp
        - source_timestamp

        """
        grouped_records = self._grouped_publish_records
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.PUBLISHER_HANDLE),
                    ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.MESSAGE_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
                ]
            )
        # NOTE: There is concern that publisher_handles has only one publisher_handle.
        if not set(publisher_handles) & set(grouped_records.keys()):
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.PUBLISHER_HANDLE),
                    ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.MESSAGE_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
                ]
            )
        sample_records = grouped_records[publisher_handles[0]]
        column_values = Columns.from_str(sample_records.columns).to_value()
        pub_records = RecordsFactory.create_instance(None, columns=column_values)

        for publisher_handle in publisher_handles:
            if publisher_handle in grouped_records:
                inter_pub_records = grouped_records[publisher_handle].clone()
                pub_records.concat(inter_pub_records)

        pub_records.sort(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)

        return pub_records

    def tilde_publish_records(
        self,
        tilde_publishers: Sequence[int]
    ) -> RecordsInterface:
        """
        Compose tilde publish records.

        Parameters
        ----------
        tilde_publishers : Sequence[int]
            TILDE publishers

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_tilde_publish_records()
            records.filter_if(
                lambda x: x.get('tilde_publisher') in tilde_publishers
            )

        """
        grouped_records = self._grouped_tilde_pub_records
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.TILDE_PUBLISH_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.TILDE_PUBLISHER),
                    ColumnValue(COLUMN_NAME.TILDE_MESSAGE_ID),
                    ColumnValue(COLUMN_NAME.TILDE_SUBSCRIPTION),
                ]
            )
        sample_records = list(grouped_records.values())[0]
        column_values = Columns.from_str(sample_records.columns).to_value()
        tilde_records = RecordsFactory.create_instance(None, columns=column_values)

        for tilde_publisher in tilde_publishers:
            if tilde_publisher in grouped_records:
                tilde_records_ = grouped_records[tilde_publisher].clone()
                tilde_records.concat(tilde_records_)

        tilde_records.drop_columns([COLUMN_NAME.TILDE_PUBLISHER])
        return tilde_records

    def _expand_key_tuple(
        self,
        group: dict[tuple[int, ...], RecordsInterface]
    ) -> dict[int, RecordsInterface]:
        group_: dict[int, RecordsInterface] = {}
        for key in group.keys():
            assert len(key) == 1
            group_[key[0]] = group[key]
        return group_

    def callback_records(
        self,
        inter_callback_object: int,
        intra_callback_object: int | None
    ) -> RecordsInterface:
        """
        Compose callback records.

        Parameters
        ----------
        inter_callback_object : int
            inter callback object
        intra_callback_object : int | None
            intra callback object

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_callback_records()
            records.filter_if(
                lambda x: x.['callback_object] in [inter_callback_object, intra_callback_object]
            )

        columns:

        - callback_start_timestamp
        - callback_end_timestamp
        - callback_object

        """
        records = self._grouped_callback_records
        callback_records = RecordsFactory.create_instance(
            None,
            columns=[
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_END_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
            ]
        )

        if inter_callback_object in records:
            inter_callback_records = records[inter_callback_object].clone()
            callback_records.concat(inter_callback_records)

        if intra_callback_object is not None and intra_callback_object in records:
            intra_callback_records = records[intra_callback_object].clone()
            callback_records.concat(intra_callback_records)
            callback_records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        return callback_records

    def path_beginning_records(
        self,
        publisher_handles: list[int]
    ) -> RecordsInterface:
        """
        Compose callback_start to publish records.

        Parameters
        ----------
        publisher_handles : list[int]
            publisher handles

        Returns
        -------
        RecordsInterface
            Equivalent to the following process.
            records = lttng.compose_path_beginning_records()
            records.filter_if(
                lambda x: x.get('publisher_handle') in publisher_handles
            )
        Columns
        - callback_start_timestamp
        - rclcpp_publish_timestamp
        - callback_handle
        - publisher_handle

        """
        grouped_records = self._path_beginning_records
        if len(grouped_records) == 0:
            return RecordsFactory.create_instance(
                None,
                columns=[
                    ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                    ColumnValue(COLUMN_NAME.CALLBACK_OBJECT),
                    ColumnValue(COLUMN_NAME.PUBLISHER_HANDLE)
                ]
            )
        sample_records = list(grouped_records.values())[0]
        column_values = Columns.from_str(sample_records.columns).to_value()
        records = RecordsFactory.create_instance(None, columns=column_values)

        for publisher_handle in publisher_handles:
            if publisher_handle in grouped_records:
                records.concat(grouped_records[publisher_handle].clone())

        records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        return records

    @cached_property
    def _grouped_callback_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_callback_records()
        group = records.groupby([COLUMN_NAME.CALLBACK_OBJECT])
        return self._expand_key_tuple(group)

    @cached_property
    def _grouped_intra_comm_records(self) -> dict[tuple[int, ...], RecordsInterface]:
        records = self._lttng.compose_intra_proc_comm_records()
        return records.groupby([COLUMN_NAME.CALLBACK_OBJECT, COLUMN_NAME.PUBLISHER_HANDLE])

    @cached_property
    def _grouped_publish_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_publish_records()
        group = records.groupby([COLUMN_NAME.PUBLISHER_HANDLE])

        # Compare records.columns with record.columns, and drop mismatched columns.
        # When node has GenericPublisher, some trace event are not output.
        for records_key in group:
            # sample_record_columns is sample value because record.data[N] has same columns.
            sample_records_columns = set(group[records_key].columns)
            sample_record_columns: set = group[records_key].data[0].columns
            mismatched_columns = sample_records_columns - sample_record_columns
            if mismatched_columns:
                optional_columns = {
                    COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
                    COLUMN_NAME.DDS_WRITE_TIMESTAMP
                    }
                drop_columns = list(optional_columns & mismatched_columns)
                group[records_key].drop_columns(drop_columns)
        return self._expand_key_tuple(group)

    @cached_property
    def _grouped_sub_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_subscribe_records()
        group = records.groupby([COLUMN_NAME.CALLBACK_OBJECT])
        return self._expand_key_tuple(group)

    @cached_property
    def _grouped_rmw_take_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_rmw_take_records()
        group = records.groupby([COLUMN_NAME.RMW_SUBSCRIPTION_HANDLE])
        return self._expand_key_tuple(group)

    @cached_property
    def _grouped_tilde_pub_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_tilde_publish_records()
        group = records.groupby([COLUMN_NAME.TILDE_PUBLISHER])
        return self._expand_key_tuple(group)

    @cached_property
    def _grouped_tilde_sub_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_tilde_subscribe_records()
        group = records.groupby([COLUMN_NAME.TILDE_SUBSCRIPTION])
        return self._expand_key_tuple(group)

    @cached_property
    def _path_beginning_records(self) -> dict[int, RecordsInterface]:
        records = self._lttng.compose_path_beginning_records()
        group = records.groupby([COLUMN_NAME.PUBLISHER_HANDLE])
        return self._expand_key_tuple(group)
