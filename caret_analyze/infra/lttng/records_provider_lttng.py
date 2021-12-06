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

from typing import List, Optional, Union

from ...exceptions import InvalidArgumentError, UnsupportedTypeError, UnsupportedNodeRecordsError
from ...infra.lttng.column_names import COLUMN_NAME
from ...record.interface import (RecordInterface, RecordsInterface)
from ...record.record import merge_sequencial, merge
from ...record.record_factory import RecordsFactory
from ...infra.interface import RuntimeDataProvider
from ...infra.infra_helper import InfraHelper
from ...value_objects import (CallbackStructValue,
                              SubscriptionCallbackStructValue,
                              TimerCallbackStructValue,
                              CommunicationStructValue,
                              NodePathStructValue,
                              PublisherStructValue,
                              SubscriptionStructValue,
                              VariablePassingStructValue,
                              CallbackChain,
                              InheritUniqueStamp,
                              UseLatestMessage,
                              Qos)
from .lttng import Lttng
from ...common import Columns
from logging import getLogger

logger = getLogger(__name__)


class RecordsProviderLttng(RuntimeDataProvider):
    """Recordsを加工して、測定結果を算出する。
    マージに加えて、フィルタリングなどはここで行う。

    Parameters
    ----------
    RecordsContainer : [type]
        [description]ol
    """

    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        self._lttng = lttng
        self._helper = RecordsProviderLttngHelper(lttng)
        self._callback_records_cache: Optional[RecordsInterface] = None
        self._inter_comm_records_cache: Optional[RecordsInterface] = None
        self._intra_comm_records_cache: Optional[RecordsInterface] = None

    def communication_records(
        self,
        comm_val: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Provide communication records

        Parameters
        ----------
        comm_info : CommunicationStructInfo
            communicadtion info.

        Returns
        -------
        RecordsInterface
            Columns [inter process communication case]:
            - [topic_name]/rclcpp_publish_timestamp
            - [topic_name]/rcl_publish_timestamp
            - [topic_name]/dds_publish_timestamp
            - [callback_name]/callback_start_timestamp

            Columns [inhtra process communication case]:
            - [topic_name]/rclcpp_intra_publish_timestamp
            - [topic_name]/message_timestamp
            - [callback_name]/callback_start_timestamp

        """
        if self.is_intra_process_communication(comm_val):
            records = self._compose_intra_proc_comm_records(comm_val)
            records.rename_columns({
                COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP:
                    InfraHelper.pub_to_column(
                        comm_val.publisher, COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                # COLUMN_NAME.MESSAGE_TIMESTAMP:
                #     InfraHelper.pub_to_column(
                #         comm_val.publisher_value, COLUMN_NAME.MESSAGE_TIMESTAMP),
                COLUMN_NAME.CALLBACK_START_TIMESTAMP:
                    InfraHelper.cb_to_column(
                        comm_val.subscription.callback, COLUMN_NAME.CALLBACK_START_TIMESTAMP)
            })
            return records
        else:
            records = self._compose_inter_proc_comm_records(comm_val)
            records.rename_columns({
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP:
                    InfraHelper.pub_to_column(
                        comm_val.publisher, COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
                COLUMN_NAME.RCL_PUBLISH_TIMESTAMP:
                    InfraHelper.pub_to_column(
                        comm_val.publisher, COLUMN_NAME.RCL_PUBLISH_TIMESTAMP),
                COLUMN_NAME.DDS_WRITE_TIMESTAMP:
                    InfraHelper.pub_to_column(
                        comm_val.publisher, COLUMN_NAME.DDS_WRITE_TIMESTAMP),
                # COLUMN_NAME.MESSAGE_TIMESTAMP:
                #     InfraHelper.pub_to_column(
                #         comm_val.publisher_value, COLUMN_NAME.MESSAGE_TIMESTAMP),
                COLUMN_NAME.CALLBACK_START_TIMESTAMP:
                    InfraHelper.cb_to_column(
                        comm_val.subscription.callback, COLUMN_NAME.CALLBACK_START_TIMESTAMP)
            })
            return records

    def node_records(
        self,
        node_path_val: NodePathStructValue,
    ) -> RecordsInterface:
        if node_path_val.child is None and node_path_val.message_context is None:
            # dummy record
            msg = 'message context and node_path.child are None. return dummy record. '
            msg += f'node_name: {node_path_val.node_name}'
            logger.info(msg)
            return RecordsFactory.create_instance()

        try:
            return NodeRecordsCallbackChain(self, node_path_val).to_records()
        except UnsupportedNodeRecordsError as e:
            logger.info(f'Skip callback_chain. {e}')

        try:
            return NodeRecordsInheritUniqueTimestamp(self, node_path_val).to_records()
        except UnsupportedNodeRecordsError as e:
            logger.info(f'Skip inherit_unique_timestamp. {e}')

        try:
            return NodeRecordsUseLatestMessage(self, node_path_val).to_records()
        except UnsupportedNodeRecordsError as e:
            logger.info(f'Skip use_latest_message. {e}')

        raise UnsupportedNodeRecordsError('Failed to calculate node latency.')

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
        if self._callback_records_cache is None:
            self._callback_records_cache = self._lttng.compose_callback_records()

        callback_objects = self._helper.get_callback_objects(callback)

        def is_target_record(record: RecordInterface):
            return record.get('callback_object') in callback_objects

        callback_records = self._callback_records_cache.clone()
        callback_records.filter_if(is_target_record)

        runtime_info_columns = [
            COLUMN_NAME.CALLBACK_OBJECT,
        ]
        callback_records.drop_columns(runtime_info_columns)
        callback_records.rename_columns(
            {
                COLUMN_NAME.CALLBACK_START_TIMESTAMP:
                    InfraHelper.cb_to_column(callback, COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                COLUMN_NAME.CALLBACK_END_TIMESTAMP:
                    InfraHelper.cb_to_column(callback, COLUMN_NAME.CALLBACK_END_TIMESTAMP),
            }
        )
        return callback_records

    def subscription_records(
        self,
        subscription_value: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/message_timestamp
            - [topic_name]/source_timestamp
            - [callback_name]/callback_start_timestamp

        Raises
        ------
        InvalidArgumentError
        """
        callback = subscription_value.callback
        if callback is None:
            msg = 'callback_value is None. '
            msg += f'node_name: {subscription_value.node_name}'
            msg += f'callback_name: {subscription_value.callback_name}'
            msg += f'topic_name: {subscription_value.topic_name}'
            raise InvalidArgumentError(msg)

        callback_object = self._helper.get_subscription_callback_object(callback)

        def is_target_record(record: RecordInterface):
            return record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object

        inter_sub_records = self._compose_inter_sub_callback_records()
        inter_sub_records.filter_if(is_target_record)

        columns = [
            InfraHelper.cb_to_column(callback, COLUMN_NAME.CALLBACK_START_TIMESTAMP),
            InfraHelper.sub_to_column(subscription_value, COLUMN_NAME.MESSAGE_TIMESTAMP),
            InfraHelper.sub_to_column(subscription_value, COLUMN_NAME.SOURCE_TIMESTAMP)
        ]
        inter_sub_records.rename_columns(
            {
                COLUMN_NAME.CALLBACK_START_TIMESTAMP: columns[0],
                COLUMN_NAME.MESSAGE_TIMESTAMP: columns[1],
                COLUMN_NAME.SOURCE_TIMESTAMP: columns[2],
            }
        )
        drop_columns = list(set(inter_sub_records.columns) - set(columns))
        inter_sub_records.drop_columns(drop_columns)
        inter_sub_records.reindex(columns)

        return inter_sub_records

    def intra_subscription_records(
        self,
        subscription_value: SubscriptionStructValue
    ) -> RecordsInterface:
        """
        Provide subscription records

        Parameters
        ----------
        subscription_value : SubscriptionStructValue
            Target subscription value.

        Returns
        -------
        RecordsInterface
            Columns
            - message_timestamp
            - callback_start_timestamp

        Raises
        ------
        InvalidArgumentError

        """
        callback_value = subscription_value.callback
        if callback_value is None:
            msg = 'callback_value is None. '
            msg += f'node_name: {subscription_value.node_name}'
            msg += f'callback_name: {subscription_value.callback_name}'
            msg += f'topic_name: {subscription_value.topic_name}'
            raise InvalidArgumentError(msg)

        callback_object_intra = self._helper.get_subscription_callback_object_intra(callback_value)

        def is_target_record_intra(record: RecordInterface):
            return record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object_intra

        intra_sub_records = self._compose_inter_sub_callback_records()
        intra_sub_records.filter_if(is_target_record_intra)

        columns = [
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        ]
        drop_columns = list(set(intra_sub_records.columns) - set(columns))
        intra_sub_records.drop_columns(drop_columns)
        intra_sub_records.reindex(columns)
        return intra_sub_records

    def publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return publish records

        Parameters
        ----------
        publish : PublisherStructValue
            target publisher

        Returns
        -------
        RecordsInterface
            Columns
            - [callback_name]/rclcpp_publish_timestamp
            - [topic_name]/message_timestamp

        """
        if self._inter_comm_records_cache is None:
            self._inter_comm_records_cache = self._lttng.compose_inter_proc_comm_records()

        publisher_handles = self._helper.get_publisher_handles(publisher)

        def is_target(record: RecordInterface):
            return record.get('publisher_handle') in publisher_handles

        records = self._inter_comm_records_cache.clone()
        records.filter_if(is_target)

        columns = [
            InfraHelper.pub_to_column(publisher, COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP),
            InfraHelper.pub_to_column(publisher, COLUMN_NAME.RCL_PUBLISH_TIMESTAMP),
            InfraHelper.pub_to_column(publisher, COLUMN_NAME.DDS_WRITE_TIMESTAMP),
            InfraHelper.pub_to_column( publisher, COLUMN_NAME.MESSAGE_TIMESTAMP),
        ]
        records.rename_columns({
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP: columns[0],
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP: columns[1],
            COLUMN_NAME.DDS_WRITE_TIMESTAMP: columns[2],
            COLUMN_NAME.MESSAGE_TIMESTAMP: columns[3],
        })
        drop_columns = list(set(records.columns) - set(columns))
        records.drop_columns(drop_columns)  # type: ignore
        records.reindex(columns)
        return records

    def intra_publish_records(
        self,
        publisher: PublisherStructValue
    ) -> RecordsInterface:
        """
        Return intra publish records

        Parameters
        ----------
        publish_value : PublisherStructValue
            target publisher value

        Returns
        -------
        RecordsInterface
            Columns
            - [topic_name]/rclcpp_intra_publish_timestamp
            - [topic_name]/message_timestamp

        """
        if self._intra_comm_records_cache is None:
            self._intra_comm_records_cache = self._lttng.compose_intra_proc_comm_records()

        publisher_handles = self._helper.get_publisher_handles(publisher)

        def is_target(record: RecordInterface):
            return record.get('publisher_handle') in publisher_handles

        records_intra = self._intra_comm_records_cache.clone()
        records_intra.filter_if(is_target)

        columns = [
            InfraHelper.pub_to_column(publisher, COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP),
            InfraHelper.pub_to_column(publisher, COLUMN_NAME.MESSAGE_TIMESTAMP),
        ]

        records_intra.rename_columns(
            {
                records_intra.columns[0]: columns[0],
                records_intra.columns[1]: columns[1],
            }
        )
        drop_columns = list(set(records_intra.columns)-set(columns))
        records_intra.drop_columns(drop_columns)
        records_intra.reindex(columns)
        return records_intra

    def get_rmw_implementation(self) -> str:
        return self._lttng.get_rmw_impl()

    def get_qos(
        self,
        pub_sub_value: Union[PublisherStructValue, SubscriptionStructValue]
    ) -> Qos:
        raise NotImplementedError()

    def _compose_inter_proc_comm_records(
        self,
        comm_value: CommunicationStructValue
    ) -> RecordsInterface:
        """
        Composer intar process communication records.

        Parameters
        ----------
        comm_value : CommunicationStructValue
            target communication value.

        Returns
        -------
        RecordsInterface
            Columns
            - rclcpp_publish_timestamp
            - rcl_publish_timestamp
            - dds_write_timestamp
            - callback_start_timestamp

        """
        if self._inter_comm_records_cache is None:
            self._inter_comm_records_cache = self._lttng.compose_inter_proc_comm_records()

        publisher = comm_value.publisher
        subscription_cb = comm_value.subscribe_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object = self._helper.get_subscription_callback_object(subscription_cb)

        def is_target(record: RecordInterface):
            if COLUMN_NAME.CALLBACK_OBJECT not in record.columns:
                return False
            if COLUMN_NAME.PUBLISHER_HANDLE not in record.columns:
                return False
            return record.get('publisher_handle') in publisher_handles and \
                record.get('callback_object') == callback_object

        records = self._inter_comm_records_cache.clone()
        records.filter_if(is_target)

        columns = [
            COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            COLUMN_NAME.RCL_PUBLISH_TIMESTAMP,
            COLUMN_NAME.DDS_WRITE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP
        ]
        drop_columns = list(set(records.columns) - set(columns))
        records.drop_columns(drop_columns)
        records.reindex(columns)
        return records

    def _compose_intra_sub_callback_records(
        self,
        subscription_callback_value: SubscriptionCallbackStructValue
    ) -> RecordsInterface:
        """
        Compose intra process subscription callback records.

        Parameters
        ----------
        subscription_callback_value : SubscriptionCallbackStructValue
            target subscription callback value

        Returns
        -------
        RecordsInterface
            Columns
            - source_timestamp
            - message_timestamp
            - callback_start_timestamp
        """
        if self._intra_comm_records_cache is None:
            self._intra_comm_records_cache = self._lttng.compose_intra_proc_comm_records()
        self._intra_comm_records_cache.clone()
        raise NotImplementedError('')

    def _compose_inter_sub_callback_records(
        self,
    ) -> RecordsInterface:
        """
        Compose inter process subscription callback records.

        Parameters
        ----------
        subscription_callback_info : SubscriptionCallbackStructInfo
            target subscription callback info

        Returns
        -------
        RecordsInterface
            Columns
            - source_timestamp
            - message_timestamp
            - callback_start_timestamp
            - callback_object
        """
        if self._inter_comm_records_cache is None:
            self._inter_comm_records_cache = self._lttng.compose_inter_proc_comm_records()

        records = self._inter_comm_records_cache.clone()

        columns = [
            COLUMN_NAME.SOURCE_TIMESTAMP,
            COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            COLUMN_NAME.CALLBACK_OBJECT,
        ]

        drop_columns = list(set(records.columns) - set(columns))
        records.drop_columns(drop_columns)
        self._ensure_column_values(records, columns)
        records.reindex(columns)
        return records

    def _ensure_column_values(
        self,
        records: RecordsInterface,
        columns: List[str]
    ) -> None:
        def has_values(record: RecordInterface):
            return set(columns) == record.columns

        records.filter_if(has_values)

    def _compose_intra_proc_comm_records(
        self,
        comm_info: CommunicationStructValue,
    ) -> RecordsInterface:
        """
        Compose intra process communication records.

        Parameters
        ----------
        comm_info : CommunicationStructInfo
            Target communication info.

        Returns
        -------
        RecordsInterface
            Columns
            - rclcpp_intra_publish_timestamp
            - callback_start_timestamp

        """
        if self._intra_comm_records_cache is None:
            self._intra_comm_records_cache = self._lttng.compose_intra_proc_comm_records()

        publisher = comm_info.publisher
        subscription_cb = comm_info.subscribe_callback

        assert subscription_cb is not None
        assert isinstance(subscription_cb, SubscriptionCallbackStructValue)

        publisher_handles = self._helper.get_publisher_handles(publisher)
        callback_object_intra = \
            self._helper.get_subscription_callback_object_intra(
                subscription_cb)

        def is_target(record: RecordInterface):
            return record.get(COLUMN_NAME.PUBLISHER_HANDLE) in publisher_handles and \
                record.get(COLUMN_NAME.CALLBACK_OBJECT) == callback_object_intra

        records = self._intra_comm_records_cache.clone()
        records.filter_if(is_target)

        columns = [
            COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            # COLUMN_NAME.MESSAGE_TIMESTAMP,
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
        ]
        runtime_info_columns = list(set(records.columns) - set(columns))
        records.drop_columns(runtime_info_columns)

        self._ensure_column_values(records, columns)
        records.reindex(columns)
        return records

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
        read_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_value_read)
        write_records: RecordsInterface = self.callback_records(
            variable_passing_info.callback_value_write)

        read_records.drop_columns([read_records.columns[-1]])  # callback end
        write_records.drop_columns([write_records.columns[0]])  # callback_start

        columns = [
            write_records.columns[0],
            read_records.columns[0],
        ]

        merged_records = merge_sequencial(
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
        self._ensure_column_values(merged_records, columns)
        merged_records.reindex(columns)
        return merged_records

    def is_intra_process_communication(
        self,
        communication_value: CommunicationStructValue
    ) -> Optional[bool]:
        intra_record = self._compose_intra_proc_comm_records(communication_value)
        return len(intra_record) > 0


class RecordsProviderLttngHelper:
    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        from .bridge import LttngBridge
        self._bridge = LttngBridge(lttng)

    def get_callback_objects(
        self,
        callback: CallbackStructValue
    ) -> List[int]:
        if isinstance(callback, TimerCallbackStructValue):
            return [self.get_timer_callback_object(callback)]

        if isinstance(callback, SubscriptionCallbackStructValue):
            obj = self.get_subscription_callback_object(callback)
            obj_intra = self.get_subscription_callback_object_intra(callback)
            if obj_intra is not None:
                return [obj, obj_intra]
            return [obj]

        msg = 'Failed to get callback object. '
        msg += f'{callback.callback_type.type_name} is not supported.'
        raise UnsupportedTypeError(msg)

    def get_timer_callback_object(
        self,
        callback: TimerCallbackStructValue
    ) -> int:
        callback_lttng = self._bridge.get_timer_callback_value(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_object(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> int:
        callback_lttng = self._bridge.get_subscription_callback_value(callback)
        return callback_lttng.callback_object

    def get_subscription_callback_object_intra(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> Optional[int]:
        callback_lttng = self._bridge.get_subscription_callback_value(callback)
        return callback_lttng.callback_object_intra

    def get_publisher_handles(
        self,
        publisher_info: PublisherStructValue
    ) -> List[int]:
        publisher_lttng = self._bridge.get_publisher_values(publisher_info)
        return [pub_info.publisher_handle
                for pub_info
                in publisher_lttng]


class NodeRecordsCallbackChain:
    def __init__(
        self,
        provider: RecordsProviderLttng,
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('callback values is None.')
        if not isinstance(node_path.message_context, CallbackChain):
            msg = 'node_path.message context is not InheritUniqueStamp'
            raise UnsupportedNodeRecordsError(msg)

        self._provider = provider
        self._validate(node_path)
        self._info = node_path

    @staticmethod
    def _rename_callback_records(
        records: RecordsInterface,
        callback_info: CallbackStructValue
    ) -> None:
        records.rename_columns(
            {
                COLUMN_NAME.CALLBACK_START_TIMESTAMP:
                    InfraHelper.cb_to_column(callback_info, COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                COLUMN_NAME.CALLBACK_END_TIMESTAMP:
                    InfraHelper.cb_to_column(callback_info, COLUMN_NAME.CALLBACK_END_TIMESTAMP),
            }
        )

    @staticmethod
    def _rename_var_pass_records(
        records: RecordsInterface,
        var_pass: VariablePassingStructValue
    ) -> None:
        callback_read = var_pass.callback_value_read
        callback_write = var_pass.callback_value_write
        records.rename_columns(
            {
                COLUMN_NAME.CALLBACK_START_TIMESTAMP:
                    InfraHelper.cb_to_column(
                        callback_read, COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                COLUMN_NAME.CALLBACK_END_TIMESTAMP:
                    InfraHelper.cb_to_column(
                        callback_write, COLUMN_NAME.CALLBACK_END_TIMESTAMP),
            }
        )

    @staticmethod
    def _rename_publish_records(
        records: RecordsInterface,
        publisher_info: PublisherStructValue,
    ) -> None:
        topic_name = publisher_info.topic_name
        records.rename_columns(
            {
                COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP:
                f'{topic_name}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'
            }
        )

    def to_records(self):
        chain_info = self._info.child

        if isinstance(chain_info[0], CallbackStructValue):
            cb_info = chain_info[0]
            records = self._provider.callback_records(cb_info)
            # self._rename_callback_records(records, cb_info)
        else:
            var_pass_info = chain_info[0]
            records = self._provider.variable_passing_records(var_pass_info)
            # self._rename_var_pass_records(records, var_pass_info)

        for chain_element in chain_info[1:]:
            if isinstance(chain_element, CallbackStructValue):
                records_ = self._provider.callback_records(chain_element)
                self._rename_callback_records(records_, chain_element)
                join_key = records_.columns[0]
                records = merge(
                    left_records=records,
                    right_records=records_,
                    join_left_key=join_key,
                    join_right_key=join_key,
                    columns=Columns(records.columns + records_.columns),
                    how='left'
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
                    columns=Columns(records.columns + records_.columns).as_list(),
                    how='left'
                )
                continue

        # last_element = chain_info[-1]
        # if isinstance(last_element, CallbackStructInfo)
        # and self._info.publisher_info is not None:
        #     last_callback_end = Util.filter(lambda x: 'callback_end' in x, records.columns)[-1]
        #     records.drop_columns([last_callback_end])

        #     last_callback_start = Util.filter(
        # lambda x: 'callback_start' in x, records.columns)[-1]
        #     publish_records = self._provider.publish_records(self._info.publisher_info)
        #     self._rename_publish_records(publish_records, self._info.publisher_info)
        #     publish_column = publish_records.columns[0]
        #     columns = records.columns + [publish_column]
        #     records = merge_sequencial(
        #         left_records=records,
        #         right_records=publish_records,
        #         join_left_key=None,
        #         join_right_key=None,
        #         left_stamp_key=last_callback_start,
        #         right_stamp_key=publish_column,
        #         columns=columns,
        #         how='left'
        #     )
        return records

    @staticmethod
    def _validate(
        node_path: NodePathStructValue,
    ) -> None:
        if node_path.callbacks is None:
            raise UnsupportedNodeRecordsError('')

        head_callback = node_path.callbacks[0]
        tail_callback = node_path.callbacks[-1]

        if node_path.publish_topic_name is not None and \
            tail_callback.publish_topic_names is not None and \
            len(tail_callback.publish_topic_names) != 0 and \
                node_path.publish_topic_name not in tail_callback.publish_topic_names:
            raise UnsupportedNodeRecordsError('')

        if node_path.subscribe_topic_name is not None and \
                node_path.subscribe_topic_name != head_callback.subscribe_topic_name:
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
        from sys import maxsize

        inter_intra_publish_timestamp = 'rclcpp_inter_intra_publish_timestamp'
        columns = [
            COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            inter_intra_publish_timestamp,
        ]

        sub_records = self._provider.subscription_records(self._node_path.subscription)
        sub_records_intra = self._provider.intra_subscription_records(
            self._node_path.subscription)
        sub_records.drop_columns([COLUMN_NAME.SOURCE_TIMESTAMP])
        sub_records.concat(sub_records_intra)
        sub_records.sort(COLUMN_NAME.CALLBACK_START_TIMESTAMP)

        pub_records = self._provider.publish_records(self._node_path.publisher)
        pub_records_intra = self._provider.intra_publish_records(self._node_path.publisher)
        pub_merged = merge_sequencial(
            left_records=pub_records,
            right_records=pub_records_intra,
            left_stamp_key=COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP,
            right_stamp_key=COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP,
            join_left_key=COLUMN_NAME.MESSAGE_TIMESTAMP,
            join_right_key=COLUMN_NAME.MESSAGE_TIMESTAMP,
            columns=Columns(pub_records.columns+pub_records_intra.columns),
            how='outer'
        )

        pub_merged.append_column(inter_intra_publish_timestamp)
        for record in pub_merged.data:
            rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
            if COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP in record.columns:
                rclcpp_publish = record.get(COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP)
            if COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP in record.columns:
                rclcpp_intra_publish = record.get(COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP)
            inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
            record.add(inter_intra_publish_timestamp, inter_intra_publish)

        pub_sub_records = merge_sequencial(
            left_records=sub_records,
            right_records=pub_merged,
            left_stamp_key=COLUMN_NAME.CALLBACK_START_TIMESTAMP,
            right_stamp_key=inter_intra_publish_timestamp,
            join_left_key=COLUMN_NAME.MESSAGE_TIMESTAMP,
            join_right_key=COLUMN_NAME.MESSAGE_TIMESTAMP,
            columns=Columns(sub_records.columns + pub_merged.columns).as_list(),
            how='left_use_latest'
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
        self._validate(node_path, self._context)
        self._node_path = node_path

    def to_records(self):
        maxsize = 2**64-1

        sub_records = self._provider.subscription_records(self._node_path.subscription)
        sub_records_intra = self._provider.intra_subscription_records(
            self._node_path.subscription)
        sub_records.drop_columns([COLUMN_NAME.SOURCE_TIMESTAMP])
        sub_records.concat(sub_records_intra)
        sub_records.sort(sub_records.columns[0])

        pub_records = self._provider.publish_records(self._node_path.publisher)
        pub_records_intra = self._provider.intra_publish_records(self._node_path.publisher)
        pub_merged = merge_sequencial(
            left_records=pub_records,
            right_records=pub_records_intra,
            left_stamp_key=pub_records.columns[0],
            right_stamp_key=pub_records_intra.columns[0],
            join_left_key=None,
            join_right_key=None,
            columns=Columns(pub_records.columns+pub_records_intra.columns),
            how='outer'
        )

        rclcpp_publish_column = \
            f'{self._node_path.publish_topic_name}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'
        rclcpp_intra_publish_column = \
            f'{self._node_path.publish_topic_name}/{COLUMN_NAME.RCLCPP_INTRA_PUBLISH_TIMESTAMP}'
        # for record in pub_merged.data:
        inter_intra_publish_timestamps = []
        for i in range(len(pub_merged)):
            record = pub_merged.data[i]
            rclcpp_publish, rclcpp_intra_publish = maxsize, maxsize
            if rclcpp_publish_column in record.columns:
                rclcpp_publish = record.get(rclcpp_publish_column)
            if rclcpp_intra_publish_column in record.columns:
                rclcpp_intra_publish = record.get(rclcpp_intra_publish_column)
            inter_intra_publish = min(rclcpp_publish, rclcpp_intra_publish)
            inter_intra_publish_timestamps.append(inter_intra_publish)
            # pub_merged.data[i].add(inter_intra_publish_timestamp, inter_intra_publish)
            []

        columns = [
            sub_records.columns[0],
            f'{self._node_path.publish_topic_name}/rclcpp_inter_intra_publish_timestamp',
        ]

        pub_merged.append_column(columns[1], inter_intra_publish_timestamps)

        pub_sub_records = merge_sequencial(
            left_records=sub_records,
            right_records=pub_merged,
            left_stamp_key=sub_records.columns[0],
            right_stamp_key=columns[1],
            join_left_key=None,
            join_right_key=None,
            columns=Columns(sub_records.columns + pub_merged.columns).as_list(),
            how='left_use_latest'
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

            return True

        if is_valid():
            return None

        msg = f'UseLatest cannot build records. \n{node_path} \n{context}'
        raise UnsupportedNodeRecordsError(msg)
