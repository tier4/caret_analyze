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

from typing import Dict, Optional, Sequence, Union


from caret_analyze.infra.lttng import Lttng, RecordsProviderLttng
from caret_analyze.infra.lttng.bridge import LttngBridge
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
)
from caret_analyze.record import RecordFactory, RecordsFactory, RecordsInterface
from caret_analyze.record.column import ColumnValue
from caret_analyze.value_objects import (
    CallbackChain,
    CallbackStructValue,
    CommunicationStructValue,
    MessageContextType,
    NodePathStructValue,
    PublisherStructValue,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    Tilde,
    TimerCallbackStructValue,
    TimerStructValue,
    UseLatestMessage,
    VariablePassingStructValue,
)

import pandas as pd

import pytest


@pytest.fixture
def bridge_mock(
    mocker,
):
    bridge_mock = mocker.Mock(spec=LttngBridge)
    mocker.patch(
        'caret_analyze.infra.lttng.bridge.LttngBridge',
        return_value=bridge_mock,
    )
    return bridge_mock


@pytest.fixture
def create_lttng(
    mocker,
    bridge_mock,
):
    def _lttng(data: Ros2DataModel):
        mocker.patch.object(Lttng, '_parse_lttng_data',
                            return_value=(data, None))
        lttng = Lttng('', validate=False)
        # mocker.patch.object(lttng, '_bridge',  bridge_mock)
        return lttng
    return _lttng


@pytest.fixture
def create_publisher_lttng(
    mocker,
):
    def _create_publisher_lttng(
        pub_handle: int,
        tilde_pub: Optional[int] = None
    ):
        publisher_lttng = mocker.Mock(spec=PublisherValueLttng)
        mocker.patch.object(
            publisher_lttng, 'publisher_handle', pub_handle)
        mocker.patch.object(publisher_lttng, 'tilde_publisher', tilde_pub)
        return publisher_lttng
    return _create_publisher_lttng


@pytest.fixture
def create_subscription_lttng(
    mocker,
):
    def _create(
        callback_object: int,
        subscription_handle: int,
        callback_object_intra: Optional[int] = None,
        tilde_subscription: Optional[int] = None,
    ):
        sub_lttng = mocker.Mock(spec=SubscriptionCallbackValueLttng)
        mocker.patch.object(
            sub_lttng, 'callback_object', callback_object)
        mocker.patch.object(
            sub_lttng, 'callback_object_intra', callback_object_intra)
        mocker.patch.object(
            sub_lttng, 'subscription_handle', subscription_handle)
        mocker.patch.object(
            sub_lttng, 'tilde_subscription', tilde_subscription)
        return sub_lttng
    return _create


@pytest.fixture
def create_subscription_callback_struct(
    mocker,
):
    def _create(
        node_name: str = 'node_name',
        topic_name: str = 'topic_name',
        callback_name: str = 'callback_name',
    ):
        sub = mocker.Mock(spec=SubscriptionCallbackStructValue)
        mocker.patch.object(sub, 'subscribe_topic_name', topic_name)
        mocker.patch.object(sub, 'node_name', node_name)
        mocker.patch.object(sub, 'callback_name', callback_name)
        return sub
    return _create


@pytest.fixture
def create_subscription_struct(
    mocker,
):
    def _create(
        node_name: str = 'node_name',
        topic_name: str = 'topic_name',
        callback_name: str = 'callback_name',
    ) -> SubscriptionStructValue:
        sub_cb = mocker.Mock(spec=SubscriptionCallbackStructValue)
        mocker.patch.object(sub_cb, 'subscribe_topic_name', topic_name)
        mocker.patch.object(sub_cb, 'node_name', node_name)
        mocker.patch.object(sub_cb, 'callback_name', callback_name)

        subscription = mocker.Mock(spec=SubscriptionStructValue)
        mocker.patch.object(subscription, 'topic_name', topic_name)
        mocker.patch.object(subscription, 'callback_name', callback_name)
        mocker.patch.object(subscription, 'callback', sub_cb)
        return subscription
    return _create


@pytest.fixture
def create_publisher_struct(
    mocker,
):
    def _create_publisher_lttng(topic_name: str = 'topic_name'):
        publisher = mocker.Mock(spec=PublisherStructValue)
        mocker.patch.object(publisher, 'topic_name', topic_name)
        return publisher

    return _create_publisher_lttng


@pytest.fixture
def create_comm_struct(
    mocker
):
    def _create(publisher: PublisherStructValue, subscription: SubscriptionStructValue):
        callback = subscription.callback
        assert callback is not None

        communication = mocker.Mock(spec=CommunicationStructValue)
        mocker.patch.object(communication, 'subscribe_callback', callback)
        mocker.patch.object(communication, 'subscription', subscription)
        mocker.patch.object(communication, 'publisher', publisher)
        mocker.patch.object(communication, 'topic_name',
                            callback.subscribe_topic_name)
        mocker.patch.object(
            communication, 'subscribe_callback_name', callback.callback_name)
        return communication

    return _create


@pytest.fixture
def create_timer_struct(mocker):
    def _create_timer_struct(
        callback_name: str,
        period: int,
    ):
        timer = mocker.Mock(spec=TimerStructValue)
        mocker.patch.object(timer, 'callback_name', callback_name)
        mocker.patch.object(timer, 'period_ns', period)

        callback = mocker.Mock(spec=TimerCallbackStructValue)
        mocker.patch.object(callback, 'callback_name', callback_name)

        mocker.patch.object(timer, 'callback', callback)

        return timer

    return _create_timer_struct


@pytest.fixture
def create_timer_cb_lttng(mocker):
    def _create_timer_cb_lttng(
        callback_object: int,
        timer_handle: Optional[int] = None
    ):
        callback_lttng = mocker.Mock(spec=TimerCallbackValueLttng)
        mocker.patch.object(callback_lttng, 'callback_object', callback_object)
        mocker.patch.object(callback_lttng, 'timer_handle', timer_handle)

        return callback_lttng
    return _create_timer_cb_lttng


@pytest.fixture
def setup_bridge_get_publisher(
    mocker,
    bridge_mock,
):
    pub_map: Dict[PublisherStructValue, Sequence[PublisherValueLttng]] = {}

    def _setup(
        publisher: PublisherStructValue,
        publisher_lttngs: Sequence[PublisherValueLttng]
    ):
        pub_map[publisher] = publisher_lttngs

        mocker.patch.object(
            bridge_mock,
            'get_publishers',
            lambda publisher: pub_map[publisher]
        )
        return bridge_mock

    return _setup


@pytest.fixture
def bridge_setup_get_callback(
    mocker,
    bridge_mock,
):

    cb_map: Dict[
        CallbackStructValue,
        Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]
    ] = {}

    def _setup(
        callback: CallbackStructValue,
        callback_lttng: Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng],
    ):
        cb_map[callback] = callback_lttng

        if isinstance(callback, TimerCallbackStructValue):
            mocker.patch.object(
                bridge_mock,
                'get_timer_callback',
                lambda callback: cb_map[callback]
            )
        elif isinstance(callback, SubscriptionCallbackStructValue):
            mocker.patch.object(
                bridge_mock,
                'get_subscription_callback',
                lambda callback: cb_map[callback]
            )

        if isinstance(callback, SubscriptionCallbackStructValue):
            mocker.patch.object(
                bridge_mock,
                'get_subscription_callback',
                lambda callback: cb_map[callback]
            )
        elif isinstance(callback, TimerCallbackStructValue):
            mocker.patch.object(
                bridge_mock,
                'get_timer_callback',
                lambda callback: cb_map[callback]
            )

        return bridge_mock

    return _setup


class TestCallbackRecords:

    def test_empty(
        self,
        create_lttng,
        create_subscription_lttng,
        create_subscription_callback_struct,
    ):
        callback = create_subscription_callback_struct('node', 'topic', 'callback_name')
        data = Ros2DataModel()
        data.finalize()

        lttng = create_lttng(data)

        provider = RecordsProviderLttng(lttng)
        records = provider.callback_records(callback)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [],
            columns=[
                # 'pid', for v0.4
                # 'tid',
                f'{callback.callback_name}/callback_start_timestamp',
                f'{callback.callback_name}/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    @pytest.mark.parametrize(
        'has_dispatch',
        [True, False]
    )
    def test_inter_callback(
        self,
        create_lttng,
        has_dispatch,
        create_subscription_lttng,
        create_subscription_callback_struct,
        bridge_setup_get_callback,
    ):
        callback_object = 5
        dispatch_timestamp = 3
        callback_start = 4
        callback_end = 7
        # pid = 1
        # tid = 2
        message = 8
        source_timestamp = 11
        message_timestamp = 15

        data = Ros2DataModel()
        if has_dispatch:
            data.add_dispatch_subscription_callback_instance(
                dispatch_timestamp, callback_object,
                message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(
            callback_start, callback_object, False)
        data.add_callback_end_instance(callback_end, callback_object)
        data.finalize()

        callback_lttng = create_subscription_lttng(
            callback_object=5,
            subscription_handle=7,
        )
        callback = create_subscription_callback_struct('node', 'topic', 'callback_name')

        bridge_setup_get_callback(callback, callback_lttng)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.callback_records(callback)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{callback.callback_name}/callback_start_timestamp': callback_start,
                    f'{callback.callback_name}/callback_end_timestamp': callback_end,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{callback.callback_name}/callback_start_timestamp',
                f'{callback.callback_name}/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    @pytest.mark.parametrize(
        'has_dispatch',
        [True, False]
    )
    def test_intra_callback(
        self,
        create_lttng,
        has_dispatch,
        create_subscription_lttng,
        create_subscription_struct,
        bridge_setup_get_callback,
    ):
        callback_lttng = create_subscription_lttng(
            callback_object=8,
            subscription_handle=7,
            callback_object_intra=5)

        subscription = create_subscription_struct()
        callback = subscription.callback

        dispatch_timestamp = 3
        callback_start = 4
        callback_end = 7
        # pid = 1
        # tid = 2
        message = 8
        message_timestamp = 15
        callback_object = callback_lttng.callback_object_intra

        data = Ros2DataModel()
        if has_dispatch:
            data.add_dispatch_intra_process_subscription_callback_instance(
                dispatch_timestamp, callback_object, message, message_timestamp)
        data.add_callback_start_instance(
            callback_start, callback_object, True)
        data.add_callback_end_instance(callback_end, callback_object)
        data.finalize()

        bridge_setup_get_callback(callback, callback_lttng)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.callback_records(callback)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{callback.callback_name}/callback_start_timestamp': callback_start,
                    f'{callback.callback_name}/callback_end_timestamp': callback_end,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{callback.callback_name}/callback_start_timestamp',
                f'{callback.callback_name}/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestPublisherRecords:

    def test_empty(
        self,
        setup_bridge_get_publisher,
        create_lttng,
        create_publisher_lttng,
        create_publisher_struct,
    ):
        data = Ros2DataModel()
        data.finalize()

        pub_handle = 3
        publisher_lttng_mock = create_publisher_lttng(pub_handle)
        publisher_mock = create_publisher_struct('topic_name')

        setup_bridge_get_publisher(publisher_mock, [publisher_lttng_mock])

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        records = provider.publish_records(publisher_mock)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            None,
            columns=[
                # 'pid',
                # 'tid',
                f'{publisher_mock.topic_name}/rclcpp_publish_timestamp',
                # f'{publisher_mock.topic_name}/rcl_publish_timestamp',
                # f'{publisher_mock.topic_name}/dds_write_timestamp',
                f'{publisher_mock.topic_name}/message_timestamp',
                f'{publisher_mock.topic_name}/source_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_single_publisher_without_tilde(
        self,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
    ):
        data = Ros2DataModel()
        pub_handle = 3
        message_timestamp = 4
        source_timestamp = 5
        message_addr = 6
        # pid = 2
        tid = 11
        data.add_rclcpp_publish_instance(tid, 1, pub_handle,
                                         message_addr, message_timestamp)
        data.add_rcl_publish_instance(tid, 2, pub_handle, message_addr)
        data.add_dds_write_instance(tid, 3, message_addr)
        data.add_dds_bind_addr_to_stamp(
            tid, 4, message_addr, source_timestamp)
        data.finalize()

        publisher_lttng_mock = create_publisher_lttng(pub_handle)
        publisher_struct_mock = create_publisher_struct('topic_name')
        setup_bridge_get_publisher(publisher_struct_mock, [
                                   publisher_lttng_mock])

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.publish_records(publisher_struct_mock)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{publisher_struct_mock.topic_name}/rclcpp_publish_timestamp': 1,
                    f'{publisher_struct_mock.topic_name}/rcl_publish_timestamp': 2,
                    f'{publisher_struct_mock.topic_name}/dds_write_timestamp': 3,
                    f'{publisher_struct_mock.topic_name}/message_timestamp': 4,
                    f'{publisher_struct_mock.topic_name}/source_timestamp': 5,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{publisher_struct_mock.topic_name}/rclcpp_publish_timestamp',
                f'{publisher_struct_mock.topic_name}/rcl_publish_timestamp',
                f'{publisher_struct_mock.topic_name}/dds_write_timestamp',
                f'{publisher_struct_mock.topic_name}/message_timestamp',
                f'{publisher_struct_mock.topic_name}/source_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_single_publisher_with_tilde(
        self,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
    ):
        data = Ros2DataModel()
        pub_handle = 3
        tilde_pub = 7
        message_timestamp = 5
        source_timestamp = 6
        message_addr = 8
        tilde_message_id = 9
        # pid = 2
        tid = 3
        tilde_sub_id = 10
        tilde_sub = 30

        data.add_tilde_subscription(
            tilde_sub, 'node', 'topic', 0)
        data.add_tilde_subscribe_added(
            tilde_sub_id, 'node', 'topic', 0)
        data.add_tilde_publish(1, tilde_pub,
                               tilde_sub_id, tilde_message_id)
        data.add_rclcpp_publish_instance(tid, 2, pub_handle,
                                         message_addr, message_timestamp)
        data.add_rcl_publish_instance(tid, 3, pub_handle, message_addr)
        data.add_dds_write_instance(tid, 4, message_addr)
        data.add_dds_bind_addr_to_stamp(
            tid, 5, message_addr, source_timestamp)
        data.finalize()

        publisher_struct_mock = create_publisher_struct('topic_name')
        publisher_lttng_mock = create_publisher_lttng(pub_handle, tilde_pub)
        setup_bridge_get_publisher(publisher_struct_mock, [
                                   publisher_lttng_mock])

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.publish_records(publisher_struct_mock)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{publisher_struct_mock.topic_name}/rclcpp_publish_timestamp': 2,
                    f'{publisher_struct_mock.topic_name}/rcl_publish_timestamp': 3,
                    f'{publisher_struct_mock.topic_name}/dds_write_timestamp': 4,
                    f'{publisher_struct_mock.topic_name}/message_timestamp': 5,
                    f'{publisher_struct_mock.topic_name}/source_timestamp': 6,
                    # f'{publisher_struct_mock.topic_name}/tilde_subscription': tilde_sub,
                    f'{publisher_struct_mock.topic_name}/tilde_publish_timestamp': 1,
                    f'{publisher_struct_mock.topic_name}/tilde_message_id': tilde_message_id,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{publisher_struct_mock.topic_name}/rclcpp_publish_timestamp',
                f'{publisher_struct_mock.topic_name}/rcl_publish_timestamp',
                f'{publisher_struct_mock.topic_name}/dds_write_timestamp',
                f'{publisher_struct_mock.topic_name}/message_timestamp',
                f'{publisher_struct_mock.topic_name}/source_timestamp',
                f'{publisher_struct_mock.topic_name}/tilde_publish_timestamp',
                f'{publisher_struct_mock.topic_name}/tilde_message_id',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestSubscriptionRecords:

    def test_empty(
        self,
        create_lttng,
        create_subscription_lttng,
        create_subscription_struct,
        bridge_setup_get_callback,
    ):
        data = Ros2DataModel()
        data.finalize()

        sub_lttng_mock = create_subscription_lttng(1, 2)
        sub_struct_mock = create_subscription_struct()
        bridge_setup_get_callback(sub_struct_mock.callback, sub_lttng_mock)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.subscribe_records(sub_struct_mock)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            None,
            columns=[
                # 'pid',
                # 'tid',
                f'{sub_struct_mock.callback_name}/callback_start_timestamp',
                f'{sub_struct_mock.topic_name}/message_timestamp',
                f'{sub_struct_mock.topic_name}/source_timestamp',
                # f'{sub_struct_mock.callback_name}/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_single_records_without_tilde(
        self,
        create_lttng,
        create_subscription_lttng,
        create_subscription_struct,
        bridge_setup_get_callback,
    ):
        callback_lttng = create_subscription_lttng(5, 58)
        subscription = create_subscription_struct()
        bridge_setup_get_callback(subscription.callback, callback_lttng)

        callback_object = callback_lttng.callback_object
        source_timestamp = 3
        message_timestamp = 2
        message = 4
        # pid = 15
        # tid = 16

        data = Ros2DataModel()
        data.add_dispatch_subscription_callback_instance(
            0, callback_object, message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(1, callback_object, False)
        data.add_callback_end_instance(3, callback_object)
        data.finalize()

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        records = provider.subscribe_records(subscription)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{subscription.callback_name}/callback_start_timestamp': 1,
                    f'{subscription.topic_name}/message_timestamp': 2,
                    f'{subscription.topic_name}/source_timestamp': 3,
                    # f'{subscription.callback_name}/callback_end_timestamp': 3,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{subscription.callback_name}/callback_start_timestamp',
                f'{subscription.topic_name}/message_timestamp',
                f'{subscription.topic_name}/source_timestamp',
                # f'{subscription.callback_name}/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_single_records_with_tilde(
        self,
        create_lttng,
        create_subscription_lttng,
        create_subscription_struct,
        bridge_setup_get_callback,
    ):
        sub_lttng = create_subscription_lttng(5, 53, tilde_subscription=8)
        subscription = create_subscription_struct()
        bridge_setup_get_callback(subscription.callback, sub_lttng)

        source_timestamp = 3
        message_timestamp = 2
        tilde_sub = 8
        message = 4
        tilde_message_id = 5
        # pid = 15
        # tid = 16
        callback_object = sub_lttng.callback_object

        data = Ros2DataModel()
        data.add_dispatch_subscription_callback_instance(
            0, callback_object, message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(6, callback_object, False)

        # subscription_id, node_name, topic_name, timestamp
        data.add_tilde_subscribe(7, tilde_sub, tilde_message_id)
        data.add_callback_end_instance(8, callback_object)
        data.finalize()

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        records = provider.subscribe_records(subscription)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    # 'pid': pid,
                    # 'tid': tid,
                    f'{subscription.callback_name}/callback_start_timestamp': 6,
                    # f'{subscription.callback_name}/callback_end_timestamp': 8,
                    f'{subscription.topic_name}/message_timestamp': message_timestamp,
                    f'{subscription.topic_name}/source_timestamp': source_timestamp,
                    f'{subscription.topic_name}/tilde_subscribe_timestamp': 7,
                    f'{subscription.topic_name}/tilde_message_id': tilde_message_id,
                }
            ],
            columns=[
                # 'pid',
                # 'tid',
                f'{subscription.callback_name}/callback_start_timestamp',
                # f'{subscription.callback_name}/callback_end_timestamp',
                f'{subscription.topic_name}/message_timestamp',
                f'{subscription.topic_name}/source_timestamp',
                f'{subscription.topic_name}/tilde_subscribe_timestamp',
                f'{subscription.topic_name}/tilde_message_id',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestNodeRecords:

    def test_callback_chain(
        self,
        mocker,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
        bridge_setup_get_callback,
        create_subscription_lttng,
        create_subscription_struct,
        create_timer_struct,
        create_timer_cb_lttng,
    ):
        callback_object = 5
        callback_object_ = 15

        data = Ros2DataModel()

        source_timestamp = 3
        message_timestamp = 2
        message = 4
        # pid, tid = 43, 44
        tid = 44
        pub_handle = 9
        data.add_dispatch_subscription_callback_instance(
            0, callback_object, message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(1, callback_object, False)
        data.add_callback_end_instance(2, callback_object)
        data.add_callback_start_instance(3, callback_object_, False)
        data.add_rclcpp_publish_instance(tid, 4, pub_handle, 5, 6)
        data.add_callback_end_instance(5, callback_object_)
        data.finalize()

        publisher = create_publisher_struct('pub_topic_name')
        publisher_lttng = create_publisher_lttng(pub_handle)
        setup_bridge_get_publisher(publisher, [publisher_lttng])

        subscription = create_subscription_struct('node_name', 'sub_topic_name', 'sub_callback')
        sub_cb_lttng = create_subscription_lttng(callback_object, 58)
        timer_cb_lttng = create_timer_cb_lttng(callback_object_, 59)
        timer = create_timer_struct('timer_callback', period=100)

        callback = subscription.callback
        callback_ = timer.callback

        bridge_setup_get_callback(callback, sub_cb_lttng)
        bridge_setup_get_callback(callback_, timer_cb_lttng)

        mocker.patch.object(
            callback_, 'publish_topic_names', ['pub_topic_name'])

        node_path = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path, 'message_context_type',
                            MessageContextType.CALLBACK_CHAIN)
        mocker.patch.object(node_path, 'publish_topic_name', 'pub_topic_name')
        mocker.patch.object(
            node_path, 'subscribe_topic_name', 'sub_topic_name')
        mocker.patch.object(node_path, 'subscription', subscription)
        mocker.patch.object(node_path, 'publisher', publisher)
        mocker.patch.object(node_path, 'callbacks', [callback, callback_])

        var_pass = mocker.Mock(spec=VariablePassingStructValue)
        mocker.patch.object(node_path, 'child', [
                            callback, var_pass, callback_])

        message_context = mocker.Mock(spec=CallbackChain)
        mocker.patch.object(
            message_context, 'publisher_topic_name', 'pub_topic_name')
        mocker.patch.object(
            message_context, 'subscription_topic_name', 'sub_topic_name')
        mocker.patch.object(node_path, 'message_context', message_context)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        def callback_records(callback_arg) -> RecordsInterface:
            if callback_arg == callback:
                return RecordsFactory.create_instance(
                    [
                        RecordFactory.create_instance(
                            {
                                f'{callback.callback_name}/callback_start_timestamp': 1,
                                f'{callback.callback_name}/callback_end_timestamp': 2,
                            },
                        )
                    ],
                    [
                        ColumnValue(f'{callback.callback_name}/callback_start_timestamp'),
                        ColumnValue(f'{callback.callback_name}/callback_end_timestamp'),
                    ]
                )
            return RecordsFactory.create_instance(
                [
                    RecordFactory.create_instance(
                        {
                            f'{callback_.callback_name}/callback_start_timestamp': 3,
                            f'{callback_.callback_name}/callback_end_timestamp': 4,
                        },
                    )
                ],
                [
                    ColumnValue(f'{callback_.callback_name}/callback_start_timestamp'),
                    ColumnValue(f'{callback_.callback_name}/callback_end_timestamp'),
                ]
            )

        mocker.patch.object(provider, 'callback_records', callback_records)

        def variable_passing_records(var_pass_args) -> RecordsInterface:
            return RecordsFactory.create_instance(
                [
                    RecordFactory.create_instance(
                        {
                            f'{callback.callback_name}/callback_end_timestamp': 2,
                            f'{callback_.callback_name}/callback_start_timestamp': 3,
                        },
                    )
                ],
                [
                    ColumnValue(f'{callback.callback_name}/callback_end_timestamp'),
                    ColumnValue(f'{callback_.callback_name}/callback_start_timestamp'),
                ]
            )

        mocker.patch.object(
            provider, 'variable_passing_records', variable_passing_records)

        records = provider.node_records(node_path)

        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    f'{callback.callback_name}/callback_start_timestamp': 1,
                    f'{callback.callback_name}/callback_end_timestamp': 2,
                    f'{callback_.callback_name}/callback_start_timestamp': 3,
                    f'{publisher.topic_name}/rclcpp_publish_timestamp': 4,
                }
            ],
            columns=[
                f'{callback.callback_name}/callback_start_timestamp',
                f'{callback.callback_name}/callback_end_timestamp',
                f'{callback_.callback_name}/callback_start_timestamp',
                f'{publisher.topic_name}/rclcpp_publish_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_use_latest_message(
        self,
        mocker,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
        bridge_setup_get_callback,
        create_subscription_lttng,
        create_subscription_struct,
    ):
        callback_object = 5

        data = Ros2DataModel()

        source_timestamp = 3
        message_timestamp = 2
        message = 4
        # pid, tid = 6, 7
        tid = 7
        data.add_dispatch_subscription_callback_instance(
            0, callback_object, message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(1, callback_object, False)
        pub_handle = 9
        data.add_rclcpp_publish_instance(tid, 2, pub_handle, 5, 6)
        data.finalize()

        subscription = create_subscription_struct('node_name', 'sub_topic_name', 'callback_name')
        publisher = create_publisher_struct('pub_topic_name')
        publisher_lttng = create_publisher_lttng(pub_handle)
        sub_cb_lttng = create_subscription_lttng(callback_object, 88)

        setup_bridge_get_publisher(publisher, [publisher_lttng])
        bridge_setup_get_callback(subscription.callback, sub_cb_lttng)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        node_path = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path, 'message_context_type',
                            MessageContextType.USE_LATEST_MESSAGE)
        mocker.patch.object(node_path, 'publish_topic_name', 'pub_topic_name')
        mocker.patch.object(node_path, 'publisher', publisher)
        mocker.patch.object(
            node_path, 'subscribe_topic_name', 'sub_topic_name')

        message_context = mocker.Mock(spec=UseLatestMessage)
        mocker.patch.object(
            message_context, 'publisher_topic_name', 'pub_topic_name')
        mocker.patch.object(
            message_context, 'subscription_topic_name', 'sub_topic_name')
        mocker.patch.object(node_path, 'message_context', message_context)
        mocker.patch.object(
            node_path, 'subscription_callback', subscription.callback)
        mocker.patch.object(
            node_path, 'subscription', subscription)
        records = provider.node_records(node_path)

        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    f'{subscription.callback_name}/callback_start_timestamp': 1,
                    f'{publisher.topic_name}/rclcpp_publish_timestamp': 2,
                }
            ],
            columns=[
                f'{subscription.callback_name}/callback_start_timestamp',
                f'{publisher.topic_name}/rclcpp_publish_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_tilde(
        self,
        mocker,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
        bridge_setup_get_callback,
        create_subscription_lttng,
        create_subscription_struct,
    ):
        callback_object = 5

        data = Ros2DataModel()

        source_timestamp = 3
        message_timestamp = 2
        message = 4
        pub_handle = 9

        tilde_pub = 7
        message_timestamp = 5
        source_timestamp = 6
        message_addr = 8
        tilde_message_id = 7
        tilde_sub_id = 0
        tilde_sub = 12

        # pid, tid = 5, 7
        tid = 7
        data.add_tilde_subscription(tilde_sub, 'node', 'topic', 0)
        data.add_tilde_subscribe_added(tilde_sub_id, 'node', 'topic', 0)

        data.add_dispatch_subscription_callback_instance(
            0, callback_object, message, source_timestamp, message_timestamp)
        data.add_callback_start_instance(1, callback_object, False)
        data.add_tilde_subscribe(2, tilde_sub, tilde_message_id)
        data.add_tilde_publish(3, tilde_pub, tilde_sub_id, tilde_message_id)
        data.add_rclcpp_publish_instance(
            tid, 4, pub_handle, message_addr, message_timestamp)
        data.add_rcl_publish_instance(tid, 5, pub_handle, message_addr)
        data.add_dds_write_instance(tid, 6, message_addr)
        data.add_dds_bind_addr_to_stamp(tid, 7, message_addr, source_timestamp)
        data.finalize()

        subscription = create_subscription_struct()
        publisher = create_publisher_struct()
        sub_cb_lttng = create_subscription_lttng(callback_object, 88, None, tilde_sub)
        pub_lttng = create_publisher_lttng(pub_handle, tilde_pub)
        setup_bridge_get_publisher(publisher, [pub_lttng])
        bridge_setup_get_callback(subscription.callback, sub_cb_lttng)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        node_path = mocker.Mock(spec=NodePathStructValue)
        mocker.patch.object(node_path, 'message_context_type',
                            MessageContextType.TILDE)
        mocker.patch.object(node_path, 'publish_topic_name', 'pub_topic_name')
        mocker.patch.object(node_path, 'publisher', publisher)
        mocker.patch.object(node_path, 'subscription', subscription)
        mocker.patch.object(
            node_path, 'subscribe_topic_name', 'sub_topic_name')

        message_context = mocker.Mock(spec=Tilde)
        mocker.patch.object(
            message_context, 'publisher_topic_name', 'pub_topic_name')
        mocker.patch.object(
            message_context, 'subscription_topic_name', 'sub_topic_name')
        mocker.patch.object(node_path, 'message_context', message_context)
        records = provider.node_records(node_path)

        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    f'{subscription.callback_name}/callback_start_timestamp': 1,
                    f'{publisher.topic_name}/rclcpp_publish_timestamp': 4,
                }
            ],
            columns=[
                f'{subscription.callback_name}/callback_start_timestamp',
                f'{publisher.topic_name}/rclcpp_publish_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestCommunicationRecords:

    def test_inter_proc(
        self,
        mocker,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
        bridge_setup_get_callback,
        create_subscription_lttng,
        create_subscription_struct,
        create_comm_struct
    ):
        pub_handle = 7
        send_message = 5
        recv_message = 8
        message_stamp = 6
        source_stamp = 9
        callback_obj = 12
        sub_handle = 28
        # pid, tid = 15, 16
        tid = 16

        data = Ros2DataModel()
        data.add_rclcpp_publish_instance(
            tid, 1, pub_handle, send_message, message_stamp)
        data.add_rcl_publish_instance(tid, 2, pub_handle, send_message)
        data.add_dds_write_instance(tid, 3, send_message)
        data.add_dds_bind_addr_to_stamp(
            tid, 4, send_message, source_stamp)
        data.add_dispatch_subscription_callback_instance(
            5, callback_obj, recv_message, source_stamp, message_stamp)
        data.add_callback_start_instance(16, callback_obj, False)
        data.add_callback_end_instance(17, callback_obj)
        data.finalize()

        pub_lttng = create_publisher_lttng(pub_handle)
        publisher = create_publisher_struct('topic_name')
        setup_bridge_get_publisher(publisher, [pub_lttng])

        subscription = create_subscription_struct()
        callback = subscription.callback
        callback_lttng = create_subscription_lttng(callback_obj, sub_handle)
        bridge_setup_get_callback(callback, callback_lttng)

        communication = create_comm_struct(publisher, subscription)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        mocker.patch.object(
            provider, 'is_intra_process_communication', return_value=False)

        records = provider.communication_records(communication)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    f'{communication.topic_name}/rclcpp_publish_timestamp': 1,
                    f'{communication.topic_name}/rcl_publish_timestamp': 2,
                    f'{communication.topic_name}/dds_write_timestamp': 3,
                    # f'{communication.topic_name}/message_timestamp': message_stamp,
                    f'{callback.callback_name}/callback_start_timestamp': 16,
                }
            ],
            columns=[
                f'{communication.topic_name}/rclcpp_publish_timestamp',
                f'{communication.topic_name}/rcl_publish_timestamp',
                f'{communication.topic_name}/dds_write_timestamp',
                # f'{communication.topic_name}/message_timestamp',
                f'{callback.callback_name}/callback_start_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)

    def test_intra_proc(
        self,
        mocker,
        create_lttng,
        create_publisher_lttng,
        setup_bridge_get_publisher,
        create_publisher_struct,
        bridge_setup_get_callback,
        create_subscription_lttng,
        create_subscription_struct,
        create_comm_struct
    ):
        pub_handle = 7
        message = 8
        message_stamp = 6
        callback_obj = 12
        # pid, tid = 58, 14
        tid = 14

        data = Ros2DataModel()
        data.add_rclcpp_intra_publish_instance(
            tid, 1, pub_handle, message, message_stamp)
        data.add_dispatch_intra_process_subscription_callback_instance(
            5, callback_obj, message, message_stamp)
        data.add_callback_start_instance(6, callback_obj, True)
        data.add_callback_end_instance(7, callback_obj)
        data.finalize()

        publisher = create_publisher_struct()
        subscription = create_subscription_struct()
        callback = subscription.callback
        communication = create_comm_struct(publisher, subscription)
        publisher_lttng = create_publisher_lttng(pub_handle)
        sub_cb_lttng = create_subscription_lttng(callback_obj, 5, callback_obj)

        setup_bridge_get_publisher(publisher, [publisher_lttng])
        bridge_setup_get_callback(subscription.callback, sub_cb_lttng)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        mocker.patch.object(
            provider, 'is_intra_process_communication', return_value=True)

        records = provider.communication_records(communication)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            [
                {
                    f'{communication.topic_name}/rclcpp_publish_timestamp': 1,
                    # f'{communication.topic_name}/message_timestamp': message_stamp,
                    f'{callback.callback_name}/callback_start_timestamp': 6,
                }
            ],
            columns=[
                f'{communication.topic_name}/rclcpp_publish_timestamp',
                # f'{communication.topic_name}/message_timestamp',
                f'{callback.callback_name}/callback_start_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestTimerRecords:

    def test_timer(
        self,
        mocker,
        create_lttng,
        create_timer_struct,
        create_timer_cb_lttng,
        bridge_setup_get_callback,
    ):
        handle = 5
        timer_init_stamp = 3
        period = 2
        callback_obj = 12
        # pid, tid = 15, 16
        tid = 16

        data = Ros2DataModel()
        data.add_timer(tid, handle, timer_init_stamp, period)
        data.add_callback_start_instance(7, callback_obj, False)
        data.add_callback_end_instance(8, callback_obj)
        data.finalize()

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        mocker.patch.object(
            provider, 'is_intra_process_communication', return_value=False)

        timer = create_timer_struct('callback_name', period)
        timer_callback_lttng = create_timer_cb_lttng(callback_obj, handle)
        bridge_setup_get_callback(timer.callback,  timer_callback_lttng)

        records = provider.timer_records(timer)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            {
                # 'pid': [None, pid],
                # 'tid': [None, tid],
                'callback_name/timer_event_timestamp': [3, 5],
                'callback_name/callback_start_timestamp': [None, 7],
                'callback_name/callback_end_timestamp': [None, 8],
            },
            columns=[
                # 'pid',
                # 'tid',
                'callback_name/timer_event_timestamp',
                'callback_name/callback_start_timestamp',
                'callback_name/callback_end_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestVarPassRecords:

    @pytest.fixture
    def create_callback_lttng(self, mocker):
        def _create_callback_lttng(
            callback_object: int,
        ):
            callback_lttng = mocker.Mock(spec=TimerCallbackValueLttng)
            mocker.patch.object(
                callback_lttng, 'callback_object', callback_object)
            return callback_lttng

        return _create_callback_lttng

    @pytest.fixture
    def create_callback_struct(self, mocker):
        def _create_timer_struct(
            callback_name: str,
        ):
            callback = mocker.Mock(spec=TimerCallbackStructValue)
            mocker.patch.object(callback, 'callback_name', callback_name)

            return callback

        return _create_timer_struct

    def test_var_pass(
        self,
        mocker,
        create_lttng,
        create_callback_lttng,
        create_callback_struct,
        bridge_mock,
        bridge_setup_get_callback,
    ):
        callback_obj = 5
        callback_obj_ = 7
        # pid, tid = 15, 16
        data = Ros2DataModel()
        data.add_callback_start_instance(7, callback_obj, False)
        data.add_callback_end_instance(8, callback_obj)
        data.add_callback_start_instance(9, callback_obj_, False)
        data.add_callback_end_instance(10, callback_obj_)
        data.finalize()

        var_pass = mocker.Mock(spec=VariablePassingStructValue)
        callback_read_strct = create_callback_struct('callback_read')
        callback_write_strct = create_callback_struct('callback_write')

        mocker.patch.object(
            var_pass, 'callback_read', callback_read_strct
        )
        mocker.patch.object(
            var_pass, 'callback_write', callback_write_strct
        )
        callback_write = create_callback_lttng(callback_obj)
        callback_read = create_callback_lttng(callback_obj_)

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)

        bridge_setup_get_callback(callback_write_strct, callback_write)
        bridge_setup_get_callback(callback_read_strct, callback_read)

        records = provider.variable_passing_records(var_pass)
        df = records.to_dataframe()

        df_expect = pd.DataFrame(
            {
                # 'pid': pid,
                'callback_write/callback_end_timestamp': [8],
                'callback_read/callback_start_timestamp': [9],
            },
            columns=[
                # 'pid',
                'callback_write/callback_end_timestamp',
                'callback_read/callback_start_timestamp',
            ],
            dtype='Int64'
        )

        assert df.equals(df_expect)


class TestSimTimeConverter:

    def test_converter(
        self,
        create_lttng,
    ):
        data = Ros2DataModel()
        # pid, tid = 4, 5
        data.add_sim_time(0, 1)
        data.add_sim_time(1, 2)
        data.finalize()

        lttng = create_lttng(data)
        provider = RecordsProviderLttng(lttng)
        converter = provider.get_sim_time_converter()

        assert converter.convert(0) - 1.0 <= 1e-6
        assert converter.convert(1) - 2.0 <= 1e-6
