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

from caret_analyze.exceptions import UnsupportedNodeRecordsError
from caret_analyze.infra.lttng import Lttng
from caret_analyze.infra.lttng.bridge import LttngBridge
from caret_analyze.infra.lttng.column_names import COLUMN_NAME
from caret_analyze.infra.lttng.records_provider_lttng import (FilteredRecordsSource,
                                                              NodeRecordsCallbackChain,
                                                              NodeRecordsInheritUniqueTimestamp,
                                                              NodeRecordsUseLatestMessage,
                                                              RecordsProviderLttng,
                                                              RecordsProviderLttngHelper)
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.ros2_tracing.data_model_service import DataModelService
from caret_analyze.infra.lttng.value_objects import (PublisherValueLttng,
                                                     SubscriptionCallbackValueLttng,
                                                     TimerCallbackValueLttng)
from caret_analyze.record.column import ColumnValue
from caret_analyze.record.interface import RecordInterface
from caret_analyze.record.record_cpp_impl import RecordCppImpl, RecordsCppImpl, RecordsInterface
from caret_analyze.value_objects import (CallbackChain, CallbackStructValue,
                                         CommunicationStructValue,
                                         MessageContextType,
                                         NodePathStructValue,
                                         PublisherStructValue,
                                         SubscriptionCallbackStructValue,
                                         SubscriptionStructValue,
                                         TimerCallbackStructValue,
                                         UseLatestMessage,
                                         VariablePassingStructValue)


class TestRecordsProviderLttng:

    def test_callback_records(self, mocker):

        records_mock = mocker.Mock(spec=RecordsInterface)

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records

        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        lttng_mock = mocker.Mock(spec=Lttng)

        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
                     return_value=helper_mock)

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
                     return_value=source_mock)

        callback_objects = (1, 2)
        mocker.patch.object(helper_mock, 'get_callback_objects', return_value=callback_objects)
        mocker.patch.object(source_mock, 'callback_records', return_value=records_mock)

        provider = RecordsProviderLttng(lttng_mock)
        callback_mock = mocker.Mock(spec=CallbackStructValue)
        records = provider.callback_records(callback_mock)

        assert records == records_mock

    # When node is implemented with subscription->take.
    def test_subscription_take_records(self, mocker):

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records

        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        lttng_mock = mocker.Mock(spec=Lttng)

        data_model_mock = mocker.Mock(spec=Ros2DataModel)

        lttng_mock.data = data_model_mock

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
                     return_value=helper_mock)

        callback_objects = (1, None)
        mocker.patch.object(helper_mock, 'get_subscription_callback_objects',
                            return_value=callback_objects)

        data_model_srv_mock = mocker.Mock(spec=DataModelService)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.DataModelService',
                     return_value=data_model_srv_mock)
        mocker.patch.object(data_model_srv_mock,
                            'get_rmw_subscription_handle_from_callback_object', return_value=4)

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
                     return_value=source_mock)

        rmw_records = RecordsCppImpl(
            [
                RecordCppImpl(
                    {
                        COLUMN_NAME.TID: 2,
                        COLUMN_NAME.RMW_TAKE_TIMESTAMP: 6,
                        COLUMN_NAME.RMW_SUBSCRIPTION_HANDLE: 4,
                        COLUMN_NAME.MESSAGE: 5,
                        COLUMN_NAME.SOURCE_TIMESTAMP: 3,
                    },
                )
            ],
            [
                ColumnValue(COLUMN_NAME.TID),
                ColumnValue(COLUMN_NAME.RMW_TAKE_TIMESTAMP),
                ColumnValue(COLUMN_NAME.RMW_SUBSCRIPTION_HANDLE),
                ColumnValue(COLUMN_NAME.MESSAGE),
                ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
            ]
        )
        mocker.patch.object(source_mock, '_grouped_rmw_take_records', {4: rmw_records})

        subscription_mock = mocker.Mock(spec=SubscriptionStructValue)
        provider = RecordsProviderLttng(lttng_mock)
        records = provider.subscription_take_records(subscription_mock)

        expected = RecordsCppImpl(
            [
                RecordCppImpl(
                    {
                        COLUMN_NAME.SOURCE_TIMESTAMP: 3,
                        COLUMN_NAME.RMW_TAKE_TIMESTAMP: 6,
                    },
                )
            ],
            [
                ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
                ColumnValue(COLUMN_NAME.RMW_TAKE_TIMESTAMP),
            ]
        )

        assert records.equals(expected)

    def test_node_records_callback_chain(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock
        node_path_info_mock = mocker.Mock(spec=NodePathStructValue)
        records_mock = mocker.Mock(spec=RecordsInterface)

        node_records_cb_chain_mock = mocker.Mock(spec=NodeRecordsCallbackChain)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.NodeRecordsCallbackChain',
                     return_value=node_records_cb_chain_mock)

        mocker.patch.object(node_records_cb_chain_mock,
                            'to_records', return_value=records_mock)

        mocker.patch.object(node_path_info_mock,
                            'message_context_type', MessageContextType.CALLBACK_CHAIN)

        provider = RecordsProviderLttng(lttng_mock)

        records = provider.node_records(node_path_info_mock)
        assert records == records_mock

    def test_node_records_inherit_timestamp(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock
        node_path_info_mock = mocker.Mock(spec=NodePathStructValue)
        records_mock = mocker.Mock(spec=RecordsInterface)

        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.NodeRecordsCallbackChain',
                     side_effect=UnsupportedNodeRecordsError(''))

        node_records_inherit_timestamp_mock = mocker.Mock(
            spec=NodeRecordsInheritUniqueTimestamp)
        mocker.patch(
            'caret_analyze.infra.lttng.records_provider_lttng.NodeRecordsInheritUniqueTimestamp',
            return_value=node_records_inherit_timestamp_mock)

        mocker.patch.object(node_path_info_mock,
                            'message_context_type', MessageContextType.INHERIT_UNIQUE_STAMP)

        mocker.patch.object(node_records_inherit_timestamp_mock,
                            'to_records', return_value=records_mock)
        provider = RecordsProviderLttng(lttng_mock)

        records = provider.node_records(node_path_info_mock)
        assert records == records_mock

    def test_get_publish_records(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock

        pub_handle = 6

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        records_mock = mocker.Mock(spec=RecordsInterface)
        mocker.patch.object(records_mock, 'columns', return_value=[])

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch(
            'caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
            return_value=helper_mock)
        mocker.patch.object(helper_mock, 'get_tilde_publishers',
                            return_value=[])
        mocker.patch.object(helper_mock, 'get_publisher_handles',
                            return_value=[pub_handle])

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch(
            'caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
            return_value=source_mock)
        mocker.patch.object(source_mock, 'publish_records', return_value=records_mock)

        provider = RecordsProviderLttng(lttng_mock)

        publisher_mock = mocker.Mock(spec=PublisherStructValue)
        records = provider.publish_records(publisher_mock)

        assert records == records_mock

    def test_get_rmw_impl(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock
        mocker.patch.object(
            lttng_mock, 'get_rmw_impl', return_value='rmw')
        provider = RecordsProviderLttng(lttng_mock)

        assert provider.get_rmw_implementation() == 'rmw'

    def test_intra_proc_comm_records(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock

        comm_mock = mocker.Mock(spec=CommunicationStructValue)
        pub_mock = mocker.Mock(spec=PublisherStructValue)
        sub_cb_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)

        cb_object = 5
        pub_handle = 6

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        records_mock = mocker.Mock(spec=RecordsInterface)

        mocker.patch.object(comm_mock, 'publisher', pub_mock)
        mocker.patch.object(comm_mock, 'subscribe_callback', sub_cb_mock)

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch(
            'caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
            return_value=helper_mock)
        mocker.patch.object(helper_mock, 'get_subscription_callback_object_intra',
                            return_value=cb_object)
        mocker.patch.object(helper_mock, 'get_publisher_handles',
                            return_value=[pub_handle])

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch(
            'caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
            return_value=source_mock)
        mocker.patch.object(source_mock, 'intra_comm_records', return_value=records_mock)

        mocker.patch.object(
            lttng_mock, 'compose_intra_proc_comm_records', return_value=records_mock)
        provider = RecordsProviderLttng(lttng_mock)

        records = provider._compose_intra_proc_comm_records(comm_mock)
        assert records == records_mock

    def test_is_intra_process_communication(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock
        provider = RecordsProviderLttng(lttng_mock)
        comm_info_mock = mocker.Mock(spec=Lttng)

        records = RecordsCppImpl()
        mocker.patch.object(
            provider, '_compose_intra_proc_comm_records', return_value=records)
        assert provider.is_intra_process_communication(comm_info_mock) is False
        records.concat(RecordsCppImpl([RecordCppImpl()]))

        assert provider.is_intra_process_communication(comm_info_mock) is True

    def test_path_beginning_records(self, mocker):

        records_mock = mocker.Mock(spec=RecordsInterface)

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
                     return_value=helper_mock)

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
                     return_value=source_mock)

        publisher = (1, 2)
        mocker.patch.object(helper_mock, 'get_publisher_handles', return_value=publisher)
        mocker.patch.object(source_mock, 'path_beginning_records', return_value=records_mock)

        provider = RecordsProviderLttng(lttng_mock)
        publisher_handles = mocker.Mock(spec=PublisherStructValue)
        records = provider.path_beginning_records(publisher_handles)

        assert records == records_mock

    def test_path_end_records(self, mocker):

        records_mock = mocker.Mock(spec=RecordsInterface)

        def _rename_column(records, callback_name, topic_name, node_name):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_rename_column', side_effect=_rename_column)

        def _format(records, columns):
            return records
        mocker.patch.object(
            RecordsProviderLttng, '_format', side_effect=_format)

        lttng_mock = mocker.Mock(spec=Lttng)
        data_model_mock = mocker.Mock(spec=Ros2DataModel)
        lttng_mock.data = data_model_mock

        helper_mock = mocker.Mock(spec=RecordsProviderLttngHelper)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.RecordsProviderLttngHelper',
                     return_value=helper_mock)

        source_mock = mocker.Mock(spec=FilteredRecordsSource)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.FilteredRecordsSource',
                     return_value=source_mock)

        callback_objects = (1, 2)
        mocker.patch.object(helper_mock, 'get_callback_objects', return_value=callback_objects)
        mocker.patch.object(source_mock, 'callback_records', return_value=records_mock)

        provider = RecordsProviderLttng(lttng_mock)
        callback_mock = mocker.Mock(spec=CallbackStructValue)
        records = provider.path_end_records(callback_mock)

        assert records == records_mock


class TestRecordsProviderLttngHelper:

    def test_get_callback_objects_timer_callback(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        timer_cb_info_mock = mocker.Mock(spec=TimerCallbackStructValue)
        cb_info_mock = mocker.Mock(spec=TimerCallbackValueLttng)

        callback_object = 3

        lttng_bridge_mock = mocker.Mock(spec=LttngBridge)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.LttngBridge',
                     return_value=lttng_bridge_mock)
        mocker.patch.object(
            lttng_bridge_mock, 'get_timer_callback', return_value=cb_info_mock)
        mocker.patch.object(cb_info_mock, 'callback_object', callback_object)

        helper = RecordsProviderLttngHelper(lttng_mock)

        inter, intra = helper.get_callback_objects(timer_cb_info_mock)
        assert inter == callback_object
        assert intra is None

    def test_get_subscription_callback_object(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        sub_cb_info_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)
        cb_info_mock = mocker.Mock(spec=SubscriptionCallbackValueLttng)

        callback_object = 3

        lttng_bridge_mock = mocker.Mock(spec=LttngBridge)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.LttngBridge',
                     return_value=lttng_bridge_mock)
        mocker.patch.object(
            lttng_bridge_mock, 'get_subscription_callback', return_value=cb_info_mock)
        mocker.patch.object(cb_info_mock, 'callback_object', callback_object)

        helper = RecordsProviderLttngHelper(lttng_mock)

        obj = helper.get_subscription_callback_object_inter(sub_cb_info_mock)
        assert obj == callback_object

    def test_get_subscription_callback_object_intra(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)
        sub_cb_info_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)
        cb_info_mock = mocker.Mock(spec=SubscriptionCallbackValueLttng)

        callback_object = 3

        lttng_bridge_mock = mocker.Mock(spec=LttngBridge)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.LttngBridge',
                     return_value=lttng_bridge_mock)
        mocker.patch.object(
            lttng_bridge_mock, 'get_subscription_callback', return_value=cb_info_mock)
        mocker.patch.object(cb_info_mock, 'callback_object_intra', callback_object)

        helper = RecordsProviderLttngHelper(lttng_mock)

        obj = helper.get_subscription_callback_object_intra(sub_cb_info_mock)
        assert obj == callback_object

    def test_get_publisher_handles(self, mocker):
        lttng_mock = mocker.Mock(spec=Lttng)

        pub_handle = 3

        lttng_bridge_mock = mocker.Mock(spec=LttngBridge)
        mocker.patch('caret_analyze.infra.lttng.records_provider_lttng.LttngBridge',
                     return_value=lttng_bridge_mock)
        helper = RecordsProviderLttngHelper(lttng_mock)

        pub_info_mock = mocker.Mock(spec=PublisherStructValue)

        pub_info_lttng_mock = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(pub_info_lttng_mock,
                            'publisher_handle', pub_handle)
        mocker.patch.object(lttng_bridge_mock, 'get_publishers', return_value=[
                            pub_info_lttng_mock])
        pub_handles = helper.get_publisher_handles(pub_info_mock)
        assert pub_handles == [pub_handle]


class TestNodeRecordsUseLatestMessage:

    def test_data_normal_flow(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        callback_name = 'callback'
        topic_name_1 = 'topic_1'
        topic_name_2 = 'topic_2'

        use_latest_message = mocker.Mock(spec=UseLatestMessage)
        mocker.patch.object(node_path_mock, 'message_context', use_latest_message)

        def noop(*args, **kwargs):
            pass
        mocker.patch.object(NodeRecordsUseLatestMessage, '_validate', noop)

        records_data: list[RecordInterface]
        records_data = [
            RecordCppImpl({
                f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 2,
            }),
            RecordCppImpl({
                f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
                f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 7,
            }),
        ]
        sub_records = RecordsCppImpl(
            records_data,
            [
                ColumnValue(f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'),
            ]
        )
        mocker.patch.object(provider_mock, 'subscribe_records', return_value=sub_records)

        records_data: list[RecordInterface]
        pub_records_data = [
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 3,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 4,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 5,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 6,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 7,
            }),
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 9,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 10,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 11,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 12,
            }),
        ]
        pub_records = RecordsCppImpl(
            pub_records_data,
            [
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
            ]
        )

        mocker.patch.object(provider_mock, 'publish_records', return_value=pub_records)
        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name_2)

        node_records = NodeRecordsUseLatestMessage(provider_mock, node_path_mock)
        records = node_records.to_records()

        expect_data = [
            RecordCppImpl({
                f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 2,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 3,
            }),
            RecordCppImpl({
                f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
                f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 7,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
            }),
        ]
        expect_records = RecordsCppImpl(
            expect_data,
            [
                ColumnValue(f'{callback_name}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
            ]
        )

        assert records.equals(expect_records)

    # When node is implemented with subscription->take
    def test_data_has_not_callback_start(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        topic_name_1 = 'topic_1'
        topic_name_2 = 'topic_2'

        use_latest_message = mocker.Mock(spec=UseLatestMessage)
        mocker.patch.object(node_path_mock, 'message_context', use_latest_message)

        def noop(*args, **kwargs):
            pass
        mocker.patch.object(NodeRecordsUseLatestMessage, '_validate', noop)

        records_data: list[RecordInterface]
        records_data = [
        ]
        sub_records = RecordsCppImpl(
            records_data,
            [
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
            ]
        )
        mocker.patch.object(provider_mock, 'subscribe_records', return_value=sub_records)

        take_records_data: list[RecordInterface]
        take_records_data = [
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_1}/{COLUMN_NAME.RMW_TAKE_TIMESTAMP}': 2,
            }),
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
                f'{topic_name_1}/{COLUMN_NAME.RMW_TAKE_TIMESTAMP}': 7,
            }),
        ]
        take_records = RecordsCppImpl(
            take_records_data,
            [
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.RMW_TAKE_TIMESTAMP}'),
            ]
        )

        mocker.patch.object(provider_mock, 'subscription_take_records', return_value=take_records)

        records_data: list[RecordInterface]
        pub_records_data = [
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 2,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 3,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 4,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 5,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
            }),
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 9,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 10,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 11,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 12,
            }),
        ]

        pub_records = RecordsCppImpl(
            pub_records_data,
            [
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
            ]
        )

        mocker.patch.object(provider_mock, 'publish_records', return_value=pub_records)
        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name_2)

        node_records = NodeRecordsUseLatestMessage(provider_mock, node_path_mock)
        records = node_records.to_records()

        expect_data = [
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 2,
            }),
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
            }),
        ]
        expect_records = RecordsCppImpl(
            expect_data,
            [
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
            ]
        )

        assert records.equals(expect_records)

    # When node is implemented with subscription->take.
    # there are cases where source_timestamp may become 0.
    def test_source_timestamp_is_0(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        node_path_mock = mocker.Mock(spec=NodePathStructValue)

        topic_name_1 = 'topic_1'
        topic_name_2 = 'topic_2'

        use_latest_message = mocker.Mock(spec=UseLatestMessage)
        mocker.patch.object(node_path_mock, 'message_context', use_latest_message)

        def noop(*args, **kwargs):
            pass
        mocker.patch.object(NodeRecordsUseLatestMessage, '_validate', noop)

        records_data: list[RecordInterface]
        records_data = [
        ]
        sub_records = RecordsCppImpl(
            records_data,
            [
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                ColumnValue(COLUMN_NAME.SOURCE_TIMESTAMP),
            ]
        )
        mocker.patch.object(provider_mock, 'subscribe_records', return_value=sub_records)

        take_records_data: list[RecordInterface]
        take_records_data = [
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                COLUMN_NAME.RMW_TAKE_TIMESTAMP: 2,
            }),
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 0,
                COLUMN_NAME.RMW_TAKE_TIMESTAMP: 7,
            }),
        ]
        take_records = RecordsCppImpl(
            take_records_data,
            [
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(COLUMN_NAME.RMW_TAKE_TIMESTAMP),
            ]
        )

        mocker.patch.object(provider_mock, 'subscription_take_records', return_value=take_records)

        records_data: list[RecordInterface]
        pub_records_data = [
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 2,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 3,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 4,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 5,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 6,
            }),
            RecordCppImpl({
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
                f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}': 9,
                f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}': 10,
                f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}': 11,
                f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 12,
            }),
        ]

        pub_records = RecordsCppImpl(
            pub_records_data,
            [
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCL_PUBLISH_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.DDS_WRITE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.MESSAGE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
            ]
        )

        mocker.patch.object(provider_mock, 'publish_records', return_value=pub_records)
        mocker.patch.object(node_path_mock, 'publish_topic_name', topic_name_2)

        node_records = NodeRecordsUseLatestMessage(provider_mock, node_path_mock)
        records = node_records.to_records()

        expect_data = [
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 2,
            }),
            RecordCppImpl({
                f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}': 1,
                f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}': 8,
            }),
        ]
        expect_records = RecordsCppImpl(
            expect_data,
            [
                ColumnValue(f'{topic_name_1}/{COLUMN_NAME.SOURCE_TIMESTAMP}'),
                ColumnValue(f'{topic_name_2}/{COLUMN_NAME.RCLCPP_PUBLISH_TIMESTAMP}'),
            ]
        )

        assert records.equals(expect_records)


class TestNodeRecordsCallbackChain:

    def test_single_callback(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        path_info_mock = mocker.Mock(spec=NodePathStructValue)
        cb_info_mock = mocker.Mock(spec=CallbackStructValue)

        records_data: list[RecordInterface]
        records_data = [
            RecordCppImpl({
                COLUMN_NAME.CALLBACK_START_TIMESTAMP: 0,
                COLUMN_NAME.CALLBACK_END_TIMESTAMP: 1,
            }),
            RecordCppImpl({
                COLUMN_NAME.CALLBACK_START_TIMESTAMP: 2,
                COLUMN_NAME.CALLBACK_END_TIMESTAMP: 3,
            }),
        ]
        cb_records = RecordsCppImpl(
            records_data,
            [
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_END_TIMESTAMP),
            ]
        )
        mocker.patch.object(provider_mock, 'callback_records', return_value=cb_records)
        mocker.patch.object(path_info_mock, 'callbacks', return_value=[cb_info_mock])
        mocker.patch.object(cb_info_mock, 'publish_topics', None)
        mocker.patch.object(cb_info_mock, 'callback_name', 'cb')
        mocker.patch.object(cb_info_mock, 'node_name', 'node')
        mocker.patch.object(path_info_mock, 'publish_topic_name', None)
        mocker.patch.object(path_info_mock, 'publisher', None)
        mocker.patch.object(path_info_mock, 'subscribe_topic_name', None)
        mocker.patch.object(path_info_mock, 'child', [cb_info_mock])
        callback_chain = mocker.Mock(spec=CallbackChain)
        mocker.patch.object(path_info_mock, 'message_context', callback_chain)

        # to_records changes the column name of cb_records,
        # so create expect before execution.
        expect = cb_records.clone()

        node_records = NodeRecordsCallbackChain(provider_mock, path_info_mock)

        records = node_records.to_records()
        assert records.equals(expect)

    def test_single_variable_passing(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        path_info_mock = mocker.Mock(spec=NodePathStructValue)
        vp_info_mock = mocker.Mock(spec=VariablePassingStructValue)

        records_data: list[RecordInterface]
        records_data = [
            RecordCppImpl({
                COLUMN_NAME.CALLBACK_END_TIMESTAMP: 1,
                COLUMN_NAME.CALLBACK_START_TIMESTAMP: 2,
            }),
            RecordCppImpl({
                COLUMN_NAME.CALLBACK_END_TIMESTAMP: 3,
                COLUMN_NAME.CALLBACK_START_TIMESTAMP: 4,
            }),
        ]
        vp_records = RecordsCppImpl(
            records_data,
            [
                ColumnValue(COLUMN_NAME.CALLBACK_END_TIMESTAMP),
                ColumnValue(COLUMN_NAME.CALLBACK_START_TIMESTAMP),
            ]
        )
        mocker.patch.object(provider_mock, 'variable_passing_records', return_value=vp_records)
        mocker.patch.object(path_info_mock, 'callbacks', return_value=[])
        mocker.patch.object(path_info_mock, 'publish_topic_name', None)
        mocker.patch.object(path_info_mock, 'publisher', None)
        mocker.patch.object(path_info_mock, 'subscribe_topic_name', None)
        mocker.patch.object(path_info_mock, 'child', [vp_info_mock])
        callback_chain = mocker.Mock(spec=CallbackChain)
        mocker.patch.object(path_info_mock, 'message_context', callback_chain)

        cb_write_mock = mocker.Mock(spec=CallbackStructValue)
        cb_read_mock = mocker.Mock(spec=CallbackStructValue)
        mocker.patch.object(cb_write_mock, 'callback_name', 'cb0')
        mocker.patch.object(cb_write_mock, 'node_name', 'node')
        mocker.patch.object(cb_read_mock, 'callback_name', 'cb1')
        mocker.patch.object(cb_read_mock, 'node_name', 'node')
        mocker.patch.object(vp_info_mock, 'callback_write', cb_write_mock)
        mocker.patch.object(vp_info_mock, 'callback_read', cb_read_mock)

        # to_records changes the column name of cb_records,
        # so create expect before execution.
        expect = vp_records.clone()

        node_records = NodeRecordsCallbackChain(provider_mock, path_info_mock)
        records = node_records.to_records()
        assert records.equals(expect)

    def test_multi_callback(self, mocker):
        provider_mock = mocker.Mock(spec=RecordsProviderLttng)
        path_info_mock = mocker.Mock(spec=NodePathStructValue)

        vp_info_mock = mocker.Mock(spec=VariablePassingStructValue)
        cb0_info_mock = mocker.Mock(spec=CallbackStructValue)
        cb1_info_mock = mocker.Mock(spec=CallbackStructValue)

        cb0_records = RecordsCppImpl(
            [
                RecordCppImpl({
                    f'cb0/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 0,
                    f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 1,
                }),
                RecordCppImpl({
                    f'cb0/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 2,
                    f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 3,
                }),
            ],
            [
                ColumnValue(f'cb0/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'),
                ColumnValue(f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'),
            ]
        )
        vp_records = RecordsCppImpl(
            [
                RecordCppImpl({
                    f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 1,
                    f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 2,
                }),
                RecordCppImpl({
                    f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 3,
                    f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 4,
                }),
            ],
            [
                ColumnValue(f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'),
                ColumnValue(f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'),
            ]
        )
        cb1_records = RecordsCppImpl(
            [
                RecordCppImpl({
                    f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 2,
                    f'cb1/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 3,
                }),
                RecordCppImpl({
                    f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}': 4,
                    f'cb1/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}': 5,
                }),
            ],
            [
                ColumnValue(f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}'),
                ColumnValue(f'cb1/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}'),
            ]
        )

        def callback_records(callback_info: CallbackStructValue):
            if callback_info == cb0_info_mock:
                return cb0_records
            return cb1_records

        mocker.patch.object(provider_mock, 'callback_records', side_effect=callback_records)
        mocker.patch.object(provider_mock, 'variable_passing_records', return_value=vp_records)
        mocker.patch.object(path_info_mock, 'callbacks', return_value=[vp_info_mock])
        mocker.patch.object(path_info_mock, 'publish_topic_name', None)
        mocker.patch.object(path_info_mock, 'publisher', None)
        mocker.patch.object(path_info_mock, 'subscribe_topic_name', None)
        mocker.patch.object(
            path_info_mock, 'child', [cb0_info_mock, vp_info_mock, cb1_info_mock])
        callback_chain = mocker.Mock(spec=CallbackChain)
        mocker.patch.object(path_info_mock, 'message_context', callback_chain)

        mocker.patch.object(cb0_info_mock, 'node_name', 'node')
        mocker.patch.object(cb1_info_mock, 'node_name', 'node')
        mocker.patch.object(cb0_info_mock, 'callback_name', 'cb0')
        mocker.patch.object(cb1_info_mock, 'callback_name', 'cb1')

        mocker.patch.object(vp_info_mock, 'callback_write', cb0_info_mock)
        mocker.patch.object(vp_info_mock, 'callback_read', cb1_info_mock)

        node_records = NodeRecordsCallbackChain(provider_mock, path_info_mock)

        column_names = [
            f'cb0/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}',
            f'cb0/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}',
            f'cb1/{COLUMN_NAME.CALLBACK_START_TIMESTAMP}',
            f'cb1/{COLUMN_NAME.CALLBACK_END_TIMESTAMP}',
        ]
        records = node_records.to_records()
        expect = RecordsCppImpl(
            [
                RecordCppImpl({
                    column_names[0]: 0,
                    column_names[1]: 1,
                    column_names[2]: 2,
                    column_names[3]: 3,
                }),
                RecordCppImpl({
                    column_names[0]: 2,
                    column_names[1]: 3,
                    column_names[2]: 4,
                    column_names[3]: 5,
                }),
            ],
            [ColumnValue(c) for c in column_names]
        )
        assert records.equals(expect)
