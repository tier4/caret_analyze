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

from caret_analyze.infra.lttng.lttng_info import (DataFrameFormatted,
                                                  LttngInfo)
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (CallbackGroupValueLttng,
                                                     NodeValueLttng,
                                                     PublisherValueLttng,
                                                     ServiceCallbackValueLttng,
                                                     SubscriptionCallbackValueLttng,
                                                     TimerCallbackValueLttng)
from caret_analyze.infra.trace_point_data import TracePointData
from caret_analyze.value_objects import (CallbackGroupType, ExecutorType,
                                         ExecutorValue)

from caret_analyze.value_objects.node import NodeValue
from caret_analyze.value_objects.service import ServiceValue
from caret_analyze.value_objects.subscription import SubscriptionValue
from caret_analyze.value_objects.timer import TimerValue

import pandas as pd

import pytest


@pytest.fixture
def create_trace_point_data(mocker):
    def _create_trace_point_data() -> TracePointData:
        data = mocker.Mock(spec=TracePointData)
        mocker.patch.object(data, 'clone', return_value=data)
        return data
    return _create_trace_point_data


class TestLttngInfo:

    def test_rmw_implementation(self, mocker):
        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        data = Ros2DataModel()
        data.finalize()

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})
        info = LttngInfo(data)
        assert info.get_rmw_impl() == ''

        data = Ros2DataModel()
        data.add_rmw_implementation('xxx_dds')
        data.finalize()
        info = LttngInfo(data)
        assert info.get_rmw_impl() == 'xxx_dds'

    def test_distribution(self, mocker):
        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        data = Ros2DataModel()
        data.finalize()

        info = LttngInfo(data)
        assert info.get_distribution() == 'NOTFOUND'

        data = Ros2DataModel()
        data.add_caret_init(0, 0, 'distribution')
        data.finalize()
        info = LttngInfo(data)
        assert info.get_distribution() == 'distribution'

    def test_get_node_names(self, mocker):
        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        nodes = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': 2,
                    'node_name': '/node',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', nodes)

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)
        nodes = info.get_nodes()
        assert len(nodes) == 1
        expect = NodeValueLttng('/node', 'node_id')
        assert nodes == [expect]

    def test_get_publishers_info(self, mocker):
        data = Ros2DataModel()

        publisher_handle = 9
        depth = 5
        node_handle = 3
        node_name = 'node_name'
        tilde_publisher = 8

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        pub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'publisher_handle': publisher_handle,
                    'node_handle': node_handle,
                    'topic_name': '/topic_name',
                    'depth': depth,
                    'construction_order': 0
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'publishers', pub)

        tilde_pub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'tilde_publisher': tilde_publisher,
                    'node_name': node_name,
                    'topic_name': '/topic_name',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'tilde_publishers', tilde_pub)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        pub_info_expect = PublisherValueLttng(
            node_name='/node',
            topic_name='/topic_name',
            node_id='node_id',
            callback_ids=None,
            publisher_handle=publisher_handle,
            construction_order=0,
            tilde_publisher=None
        )

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        data.finalize()
        info = LttngInfo(data)
        pubs_info = info.get_publishers(NodeValue('/node', 'node_id'))
        assert len(pubs_info) == 1
        assert pubs_info[0] == pub_info_expect

        pubs_info = info.get_publishers(NodeValue('/node_', 'node_id_'))
        assert len(pubs_info) == 0

    def test_get_subscriptions_info(self, mocker):
        data = Ros2DataModel()

        subscription_handle = 9
        depth = 5
        node_handle = 3
        node_name = 'node_name'
        tilde_subscription = 8

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'subscription_handle': subscription_handle,
                    'node_handle': node_handle,
                    'topic_name': '/topic_name',
                    'depth': depth,
                    'construction_order': 0
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'subscriptions', sub)

        tilde_sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'tilde_subscription': tilde_subscription,
                    'node_name': node_name,
                    'topic_name': '/topic_name',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'tilde_subscriptions', tilde_sub)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        cbg_addr = 11
        symbol = 'some_callback'
        callback_object = 2
        callback_object_intra = 4
        sub_cbs = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'callback_object_intra': callback_object_intra,
                    'node_handle': node_handle,
                    'subscription_handle': subscription_handle,
                    'callback_group_addr': cbg_addr,
                    'topic_name': '/topic_name',
                    'symbol': symbol,
                    'callback_id': 'subscription_callback_0',
                    'depth': depth,
                    'construction_order': 0,
                },
            ]
        ))
        mocker.patch.object(formatted_mock, 'subscription_callbacks', sub_cbs)
        mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        sub_info_expect = SubscriptionValue(
            node_name='/node',
            topic_name='/topic_name',
            node_id='node_id',
            callback_id='subscription_callback_0',
            construction_order=0,
        )

        data.finalize()
        info = LttngInfo(data)

        subs_info = info.get_subscriptions(NodeValue('/node', 'node_id'))
        assert len(subs_info) == 1
        assert subs_info[0] == sub_info_expect

        subs_info = info.get_subscriptions(NodeValue('/node_', 'node_id_'))
        assert len(subs_info) == 0

    def test_get_timer_callbacks_info(self, mocker):

        node_handle = 1
        timer_handle = 3
        period_ns = 8
        callback_object = 11
        callback_group_addr = 14
        timer_handle = 15
        symbol = 'symbol'
        construction_order = 0

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        timer = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': timer_handle,
                    'callback_group_addr': callback_group_addr,
                    'period_ns': period_ns,
                    'symbol': symbol,
                    'callback_id': 'timer_callback_0',
                    'construction_order': 0
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'timer_callbacks', timer)

        nodes = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node1'
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', nodes)
        data = Ros2DataModel()

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        # mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        data.finalize()
        info = LttngInfo(data)

        timer_cbs_info = info.get_timer_callbacks(NodeValue('/node1', 'node_id'))
        timer_cb_info_expect = TimerCallbackValueLttng(
            'timer_callback_0',
            'node_id',
            '/node1',
            symbol,
            period_ns,
            timer_handle,
            None,
            callback_object=callback_object,
            construction_order=construction_order
        )

        assert timer_cbs_info == [timer_cb_info_expect]

        assert info.get_timer_callbacks(NodeValue('/', 'id')) == []

    def test_get_subscription_callbacks_info(self, mocker):
        callback_object = [2, 3]
        callback_object_intra = [4]
        node_handle = [7, 8]

        topic_name = ['/topic1', '/topic2']
        node_name = ['/node1', '/node2']
        symbol = ['symbol0', 'symbol1']
        depth = [9, 10]
        subscription_handle = [11, 12]
        cbg_addr = [13, 14]
        tilde_subscription = [17, 18]
        subscription_handle = [19, 20]

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object[0],
                    'callback_object_intra': callback_object_intra[0],
                    'node_handle': node_handle[0],
                    'subscription_handle': subscription_handle[0],
                    'callback_group_addr': cbg_addr[0],
                    'topic_name': topic_name[0],
                    'symbol': symbol[0],
                    'callback_id': 'subscription_callback_0',
                    'depth': depth[0],
                    'construction_order': 0,
                },
                {
                    'callback_object': callback_object[1],
                    'node_handle': node_handle[1],
                    'subscription_handle': subscription_handle[1],
                    'callback_group_addr': cbg_addr[1],
                    'topic_name': topic_name[1],
                    'symbol': symbol[1],
                    'callback_id': 'subscription_callback_1',
                    'depth': depth[1],
                    'construction_order': 0,
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(
            formatted_mock, 'subscription_callbacks', sub)

        tilde_sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'tilde_subscription': tilde_subscription[0],
                    'node_name': node_name[0],
                    'topic_name': topic_name[0],
                },
                {
                    'tilde_subscription': tilde_subscription[1],
                    'node_name': node_name[1],
                    'topic_name': topic_name[1],
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(
            formatted_mock, 'tilde_subscriptions', tilde_sub)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle[0],
                    'node_name': node_name[0]
                },
                {
                    'node_id': 'node_id_2',
                    'node_handle': node_handle[1],
                    'node_name': node_name[1]
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(formatted_mock, 'nodes', node)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        sub_cbs_info = info.get_subscription_callbacks(NodeValue('/node1', 'node_id'))
        sub_cb_info_expect = SubscriptionCallbackValueLttng(
            'subscription_callback_0',
            'node_id',
            node_name[0],
            symbol[0],
            topic_name[0],
            subscription_handle[0],
            None,
            callback_object=callback_object[0],
            callback_object_intra=callback_object_intra[0],
            tilde_subscription=tilde_subscription[0],
            construction_order=0
        )
        assert sub_cbs_info == [sub_cb_info_expect]

        sub_cbs_info = info.get_subscription_callbacks(NodeValue('/node2', 'node_id_2'))
        sub_cb_info_expect = SubscriptionCallbackValueLttng(
            'subscription_callback_1',
            'node_id_2',
            node_name[1],
            symbol[1],
            topic_name[1],
            subscription_handle[1],
            None,
            callback_object[1],
            None,
            tilde_subscription=tilde_subscription[1],
            construction_order=0
        )
        assert sub_cbs_info == [sub_cb_info_expect]

        sub_cbs_info = info.get_subscription_callbacks(NodeValue('/', '/'))
        assert sub_cbs_info == []

    def test_get_service_callbacks_info(self, mocker):
        callback_object = [2, 3]
        node_handle = [7, 8]

        service_name = ['/service1', '/service2']
        node_name = ['/node1', '/node2']
        symbol = ['symbol0', 'symbol1']
        service_handle = [11, 12]
        cbg_addr = [13, 14]

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        srv = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object[0],
                    'node_handle': node_handle[0],
                    'service_handle': service_handle[0],
                    'callback_group_addr': cbg_addr[0],
                    'service_name': service_name[0],
                    'symbol': symbol[0],
                    'callback_id': 'service_callback_0',
                    'construction_order': 0,
                },
                {
                    'callback_object': callback_object[1],
                    'node_handle': node_handle[1],
                    'service_handle': service_handle[1],
                    'callback_group_addr': cbg_addr[1],
                    'service_name': service_name[1],
                    'symbol': symbol[1],
                    'callback_id': 'service_callback_1',
                    'construction_order': 0,
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(
            formatted_mock, 'service_callbacks', srv)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle[0],
                    'node_name': node_name[0]
                },
                {
                    'node_id': 'node_id_2',
                    'node_handle': node_handle[1],
                    'node_name': node_name[1]
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(formatted_mock, 'nodes', node)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        srv_cbs_info = info.get_service_callbacks(NodeValue('/node1', 'node_id'))
        srv_cb_info_expect = ServiceCallbackValueLttng(
            'service_callback_0',
            'node_id',
            node_name[0],
            symbol[0],
            service_name[0],
            service_handle[0],
            None,
            callback_object=callback_object[0],
            construction_order=0
        )
        assert srv_cbs_info == [srv_cb_info_expect]

        srv_cbs_info = info.get_service_callbacks(NodeValue('/node2', 'node_id_2'))
        srv_cb_info_expect = ServiceCallbackValueLttng(
            'service_callback_1',
            'node_id_2',
            node_name[1],
            symbol[1],
            service_name[1],
            service_handle[1],
            None,
            callback_object[1],
            construction_order=0
        )
        assert srv_cbs_info == [srv_cb_info_expect]

        srv_cbs_info = info.get_service_callbacks(NodeValue('/', '/'))
        assert srv_cbs_info == []

    def test_get_callback_groups_info(self, mocker):
        node_handle = 3
        callback_object = 10
        node_name = '/node1'
        cbg_addr = 8
        exec_addr = 9

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        timer = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'period_ns': 0,
                    'symbol': 'symbol',
                    'callback_id': 'timer_callback_0'
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'timer_callbacks', timer)

        sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'callback_object_intra': 0,
                    'node_handle': node_handle,
                    'subscription_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'topic_name': 'topic',
                    'symbol': 'symbol',
                    'callback_id': 'subscription_callback_0',
                    'depth': 0
                },
            ]
        ))
        mocker.patch.object(
            formatted_mock, 'subscription_callbacks', sub)

        srv = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'service_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'service_name': 'service',
                    'symbol': 'symbol',
                    'callback_id': 'service_callback_0'
                },
            ]
        ))
        mocker.patch.object(
            formatted_mock, 'service_callbacks', srv)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': node_name
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        cbg = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_group_id': 'callback_group_id',
                    'callback_group_addr': cbg_addr,
                    'executor_addr': exec_addr,
                    'group_type_name': CallbackGroupType.REENTRANT.type_name,
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'callback_groups', cbg)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)
        cbg_info = info.get_callback_groups(NodeValue(node_name, 'node_id'))

        cbg_info_expect = CallbackGroupValueLttng(
            CallbackGroupType.REENTRANT.type_name,
            '/node1',
            'node_id',
            ('timer_callback_0', 'subscription_callback_0', 'service_callback_0'),
            'callback_group_id',
            callback_group_addr=cbg_addr,
            executor_addr=exec_addr,
        )
        assert cbg_info == [cbg_info_expect]

    def test_get_undefined_callback_groups_info(self, mocker):
        node_handle = 3
        callback_object = 10
        node_name = '/node1'
        cbg_addr = 8

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        timer = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'period_ns': 0,
                    'symbol': 'symbol',
                    'callback_id': 'timer_callback_0'
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'timer_callbacks', timer)

        sub = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'callback_object_intra': 0,
                    'node_handle': node_handle,
                    'subscription_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'topic_name': 'topic',
                    'symbol': 'symbol',
                    'callback_id': 'subscription_callback_0',
                    'depth': 0
                },
            ]
        ))
        mocker.patch.object(
            formatted_mock, 'subscription_callbacks', sub)

        srv = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'service_handle': 0,
                    'callback_group_addr': cbg_addr,
                    'service_name': 'service',
                    'symbol': 'symbol',
                    'callback_id': 'service_callback_0'
                },
            ]
        ))
        mocker.patch.object(
            formatted_mock, 'service_callbacks', srv)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': node_name
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        cbg = TracePointData(pd.DataFrame(columns=[
            'callback_group_id',
            'callback_group_addr',
            'executor_addr',
            'group_type_name']
        ))
        mocker.patch.object(formatted_mock, 'callback_groups', cbg)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)
        cbg_info = info.get_callback_groups(NodeValue(node_name, 'node_id'))
        dummy_addr_expect = 0
        callback_group_id_expect = f'callback_group_{cbg_addr}'
        cbg_info_expect = CallbackGroupValueLttng(
            CallbackGroupType.UNDEFINED.type_name,
            '/node1',
            'node_id',
            ('timer_callback_0', 'subscription_callback_0', 'service_callback_0'),
            callback_group_id_expect,
            callback_group_addr=cbg_addr,
            executor_addr=dummy_addr_expect,
        )
        assert cbg_info == [cbg_info_expect]

    def test_get_executors_info(self, mocker):
        cbg_addr = 13
        executor_addr = 15

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        executor = TracePointData(pd.DataFrame.from_dict([
                {
                    'executor_addr': executor_addr,
                    'executor_type_name': ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
                }
            ]
        ).convert_dtypes())
        mocker.patch.object(formatted_mock, 'executor', executor)
        cbg = TracePointData(pd.DataFrame.from_dict([{
                'callback_group_id': 'callback_group_id',
                'callback_group_addr': cbg_addr,
                'executor_addr': executor_addr,
                'group_type_name': CallbackGroupType.REENTRANT.type_name,
            }
        ]).convert_dtypes())
        mocker.patch.object(formatted_mock, 'callback_groups', cbg)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        execs_info = info.get_executors()

        exec_info_expect = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            ('callback_group_id',),
        )

        assert execs_info == [exec_info_expect]

    def test_get_services_info(self, mocker):
        data = Ros2DataModel()

        service_handle = 9
        node_handle = 3

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        srv = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'service_id': 'service_0',
                    'service_handle': service_handle,
                    'node_handle': node_handle,
                    'service_name': '/service_name',
                    'construction_order': 0
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'services', srv)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        cbg_addr = 11
        symbol = 'some_callback'
        callback_object = 2
        srv_cbs = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_id': 'service_callback_0',
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'service_handle': service_handle,
                    'callback_group_addr': cbg_addr,
                    'service_name': '/service_name',
                    'symbol': symbol,
                    'construction_order': 0,
                },
            ]
        ))
        mocker.patch.object(formatted_mock, 'service_callbacks', srv_cbs)

        srv_info_expect = ServiceValue(
            node_name='/node',
            service_name='/service_name',
            node_id='node_id',
            callback_id='service_callback_0',
            construction_order=0,
        )

        data.finalize()
        info = LttngInfo(data)

        srvs_info = info.get_services(NodeValue('/node', 'node_id'))
        assert len(srvs_info) == 1
        assert srvs_info[0] == srv_info_expect

        srvs_info = info.get_services(NodeValue('/node_', 'node_id_'))
        assert len(srvs_info) == 0

    def test_get_timers_info(self, mocker):
        data = Ros2DataModel()

        timer_handle = 9
        node_handle = 3
        period = 1000

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        tim = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'timer_id': 'timer_0',
                    'timer_handle': timer_handle,
                    'node_handle': node_handle,
                    'period': period,
                    'construction_order': 0
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'timers', tim)

        node = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node',
                }
            ]
        ))
        mocker.patch.object(formatted_mock, 'nodes', node)

        cbg_addr = 11
        symbol = 'some_callback'
        callback_object = 2
        tim_cbs = TracePointData(pd.DataFrame.from_dict(
            [
                {
                    'callback_id': 'timer_callback_0',
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': timer_handle,
                    'callback_group_addr': cbg_addr,
                    'period_ns': period,
                    'symbol': symbol,
                    'construction_order': 0,
                },
            ]
        ))
        mocker.patch.object(formatted_mock, 'timer_callbacks', tim_cbs)

        tim_info_expect = TimerValue(
            node_name='/node',
            period=1000,
            node_id='node_id',
            callback_id='timer_callback_0',
            construction_order=0,
        )

        data.finalize()
        info = LttngInfo(data)

        times_info = info.get_timers(NodeValue('/node', 'node_id'))
        assert len(times_info) == 1
        assert times_info[0] == tim_info_expect

        times_info = info.get_timers(NodeValue('/node_', 'node_id_'))
        assert len(times_info) == 0


class TestDataFrameFormatted:

    def test_build_timer_callbacks_df(self):
        data = Ros2DataModel()

        node_handle = 1
        timer_handle = 3
        rmw_handle = 6
        period_ns = 8
        callback_object = 11
        callback_group_addr = 13
        symbol = 'symbol1'

        data.add_node(0, node_handle, 0, rmw_handle, 'node1', '/')

        data.add_timer(0, timer_handle, 0, period_ns)
        data.add_callback_object(timer_handle, 0, callback_object)
        data.add_timer_node_link(timer_handle, 0, node_handle)
        data.add_callback_symbol(callback_object, 0, symbol)

        data.callback_group_add_timer(
            callback_group_addr, 0, timer_handle
        )
        data.finalize()

        timer = DataFrameFormatted._build_timer_callbacks(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'callback_id': f'timer_callback_{callback_object}',
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': timer_handle,
                    'callback_group_addr': callback_group_addr,
                    'period_ns': period_ns,
                    'symbol': symbol,
                    'construction_order': 0,
                },
            ]
        ).convert_dtypes()
        assert timer.df.equals(expect)

    def test_build_timer_control_df(self):
        data = Ros2DataModel()
        timer_handle = 3
        period = 8
        timestamp = 15
        params = {'period': period}
        type_name = 'init'

        data.add_timer(0, timer_handle, timestamp, period)

        data.finalize()

        timer = DataFrameFormatted._build_timer_control(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'timestamp': timestamp,
                    'timer_handle': timer_handle,
                    'type': type_name,
                    'params': params,
                },
            ]
        ).convert_dtypes()
        assert timer.df.equals(expect)

    def test_build_subscription_callbacks_df(self, mocker):
        data = Ros2DataModel()

        node_handle = [0, 1]
        rmw_handle = [2, 3]
        subscription_handle = [4, 5]
        callback_object_intra = [7]
        callback_object_inter = [8, 9]
        depth = [11, 12]
        sub_ptr = [24, 25, 26]
        symbol = ['symbol_', 'symbol__']
        topic_name = ['topic1', 'topic2']
        callback_group_addr = [15, 16]

        data.add_node(0, node_handle[0], 0, rmw_handle[0], 'node1', '/')
        data.add_node(0, node_handle[1], 0, rmw_handle[1], 'node2', '/')

        # When intra-process communication is set, add_rclcpp_subscription is called twice.
        # The first one will be the record of intra-process communication.
        data.add_rclcpp_subscription(sub_ptr[0], 0, subscription_handle[0])
        data.add_rclcpp_subscription(sub_ptr[1], 0, subscription_handle[0])
        data.add_rclcpp_subscription(sub_ptr[2], 0, subscription_handle[1])

        data.add_rcl_subscription(
            subscription_handle[0], 0, node_handle[0], rmw_handle[0], topic_name[0], depth[0])
        data.add_rcl_subscription(
            subscription_handle[1], 0, node_handle[1], rmw_handle[1], topic_name[1], depth[1])

        data.add_callback_object(sub_ptr[0], 0, callback_object_intra[0])
        data.add_callback_object(sub_ptr[1], 0, callback_object_inter[0])
        data.add_callback_object(sub_ptr[2], 0, callback_object_inter[1])

        data.add_callback_symbol(callback_object_intra[0], 0, symbol[0])
        data.add_callback_symbol(callback_object_inter[0], 0, symbol[0])
        data.add_callback_symbol(callback_object_inter[1], 0, symbol[1])

        data.callback_group_add_subscription(
            callback_group_addr[0], 0, subscription_handle[0]
        )
        data.callback_group_add_subscription(
            callback_group_addr[1], 0, subscription_handle[1]
        )

        data.finalize()

        mocker.patch.object(DataFrameFormatted, '_is_ignored_subscription', return_value=False)

        sub = DataFrameFormatted._build_sub_callbacks(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'callback_id': f'subscription_callback_{callback_object_inter[0]}',
                    'callback_object': callback_object_inter[0],
                    'callback_object_intra': callback_object_intra[0],
                    'node_handle': node_handle[0],
                    'subscription_handle': subscription_handle[0],
                    'callback_group_addr': callback_group_addr[0],
                    'topic_name': topic_name[0],
                    'symbol': symbol[0],
                    'depth': depth[0],
                    'construction_order': 0,
                },
                {
                    'callback_id': f'subscription_callback_{callback_object_inter[1]}',
                    'callback_object': callback_object_inter[1],
                    'node_handle': node_handle[1],
                    'subscription_handle': subscription_handle[1],
                    'callback_group_addr': callback_group_addr[1],
                    'topic_name': topic_name[1],
                    'symbol': symbol[1],
                    'depth': depth[1],
                    'construction_order': 0,
                },
            ]
        ).convert_dtypes()
        assert sub.df.equals(expect)

    def test_build_service_callbacks_df(self, mocker):
        data = Ros2DataModel()

        node_handle = [0, 1]
        rmw_handle = [2, 3]
        service_handle = [4, 5]
        callback_object_inter = [8, 9]
        symbol = ['symbol_', 'symbol__']
        node_name = ['node1', 'node2']
        service_name = ['service1', 'service2']
        callback_group_addr = [15, 16]

        data.add_node(0, node_handle[0], 0, rmw_handle[0], node_name[0], '/')
        data.add_node(0, node_handle[1], 0, rmw_handle[1], node_name[1], '/')

        data.add_service(
            service_handle[0], 0, node_handle[0], rmw_handle[0], service_name[0])
        data.add_service(
            service_handle[1], 0, node_handle[1], rmw_handle[1], service_name[1])

        data.add_callback_object(service_handle[0], 0, callback_object_inter[0])
        data.add_callback_object(service_handle[1], 0, callback_object_inter[1])

        data.add_callback_symbol(callback_object_inter[0], 0, symbol[0])
        data.add_callback_symbol(callback_object_inter[1], 0, symbol[1])

        data.callback_group_add_service(
            callback_group_addr[0], 0, service_handle[0]
        )
        data.callback_group_add_service(
            callback_group_addr[1], 0, service_handle[1]
        )

        data.finalize()

        srv = DataFrameFormatted._build_srv_callbacks(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'callback_id': f'service_callback_{callback_object_inter[0]}',
                    'callback_object': callback_object_inter[0],
                    'node_handle': node_handle[0],
                    'service_handle': service_handle[0],
                    'callback_group_addr': callback_group_addr[0],
                    'service_name': service_name[0],
                    'symbol': symbol[0],
                    'construction_order': 0,
                },
                {
                    'callback_id': f'service_callback_{callback_object_inter[1]}',
                    'callback_object': callback_object_inter[1],
                    'node_handle': node_handle[1],
                    'service_handle': service_handle[1],
                    'callback_group_addr': callback_group_addr[1],
                    'service_name': service_name[1],
                    'symbol': symbol[1],
                    'construction_order': 0,
                },
            ]
        ).convert_dtypes()
        assert srv.df.equals(expect)

    def test_executor_df(self):
        data = Ros2DataModel()
        exec_addr = 3
        exec_type = 'single_threaded_executor'

        data.add_executor(exec_addr, 0, exec_type)
        data.finalize()

        executor = DataFrameFormatted._build_executor(data)

        expect = [
            {
                'executor_id': f'executor_{exec_addr}',
                'executor_addr': exec_addr,
                'executor_type_name': exec_type,
            }
        ]
        expect_df = pd.DataFrame.from_dict(expect).convert_dtypes()

        assert executor.df.equals(expect_df)

    def test_executor_static_df(self):
        data = Ros2DataModel()
        exec_addr = 3
        exec_type = 'static_single_threaded_executor'
        collector_addr = 4

        data.add_executor_static(exec_addr, collector_addr, 0, exec_type)
        data.finalize()

        executor = DataFrameFormatted._build_executor(data)

        expect = [
            {
                'executor_id': f'executor_{exec_addr}',
                'executor_addr': exec_addr,
                'executor_type_name': exec_type,
            }
        ]
        expect_df = pd.DataFrame.from_dict(expect).convert_dtypes()

        assert executor.df.equals(expect_df)

    def test_format_subscription_callback_object(self, mocker):
        data = Ros2DataModel()

        subscription_handle = [4, 5]
        callback_object_intra = [7]

        callback_object_inter = [8, 9]
        sub_ptr = [11, 12, 13]

        # When intra-process communication is set, add_rclcpp_subscription is called twice.
        # The first one will be the record of intra-process communication.
        data.add_rclcpp_subscription(sub_ptr[0], 0, subscription_handle[0])
        data.add_callback_object(sub_ptr[0], 0, callback_object_intra[0])

        data.add_rclcpp_subscription(sub_ptr[1], 0, subscription_handle[0])
        data.add_callback_object(sub_ptr[1], 0, callback_object_inter[0])

        data.add_rclcpp_subscription(sub_ptr[2], 0, subscription_handle[1])
        data.add_callback_object(sub_ptr[2], 0, callback_object_inter[1])

        data.finalize()

        mocker.patch.object(DataFrameFormatted, '_is_ignored_subscription', return_value=False)

        sub = DataFrameFormatted._format_subscription_callback_object(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'subscription_handle': subscription_handle[0],
                    'callback_object': callback_object_inter[0],
                    'callback_object_intra': callback_object_intra[0],
                },
                {
                    'subscription_handle': subscription_handle[1],
                    'callback_object': callback_object_inter[1],
                },
            ]
        ).convert_dtypes()
        assert sub.df.equals(expect)

    def test_build_nodes_df(self):
        data = Ros2DataModel()

        node_handle = 0
        rmw_handle = 2
        data.add_node(
            node_handle=node_handle,
            timestamp=0,
            tid=0,
            rmw_handle=rmw_handle,
            name='node1',
            namespace='/')
        data.finalize()
        nodes = DataFrameFormatted._build_nodes(data)

        expect = pd.DataFrame.from_dict(
            [{
                'node_id': '/node1_0',
                'node_handle': node_handle,
                'node_name': '/node1',
            }]
        ).convert_dtypes()
        assert nodes.df.equals(expect)

    def test_build_callback_groups_df(self):
        group_type = 'reentrant'
        collector_addr = 2
        cbg_addr = 3
        exec_addr = 4

        data_humble = Ros2DataModel()
        data_humble.add_caret_init(0, 0, 'humble')
        data_humble.add_callback_group_static_executor(collector_addr, 0, cbg_addr, group_type)
        data_humble.add_executor_static(exec_addr, collector_addr, 0, 'exec_type')
        data_humble.finalize()

        cbg_humble = DataFrameFormatted._build_cbg(data_humble)

        expect = pd.DataFrame.from_dict(
            [{
                'callback_group_id': f'callback_group_{cbg_addr}',
                'callback_group_addr': cbg_addr,
                'group_type_name': group_type,
                'executor_addr': exec_addr,
            }]
        ).convert_dtypes()
        assert cbg_humble.df.equals(expect)

        data_jazzy = Ros2DataModel()
        data_jazzy.add_caret_init(0, 0, 'jazzy')
        data_jazzy.add_callback_group_to_executor_entity_collector(
            collector_addr, cbg_addr, group_type, 0)
        data_jazzy.add_executor_entity_collector_to_executor(
            exec_addr, collector_addr, 0)
        data_jazzy.add_executor_static(exec_addr, collector_addr, 0, 'exec_type')
        data_jazzy.finalize()

        cbg_jazzy = DataFrameFormatted._build_cbg(data_jazzy)

        expect = pd.DataFrame.from_dict(
            [{
                'callback_group_id': f'callback_group_{cbg_addr}',
                'callback_group_addr': cbg_addr,
                'group_type_name': group_type,
                'executor_addr': exec_addr,
            }]
        ).convert_dtypes()
        assert cbg_jazzy.df.equals(expect)

        assert cbg_humble.df.equals(cbg_jazzy.df)

    def test_build_publisher_df(self):
        pub_handle = 1
        node_handle = 2
        rmw_handle = 3
        topic_name = 'topic'
        depth = 5
        construction_order = 0

        data = Ros2DataModel()
        data.add_publisher(pub_handle, 0, node_handle,
                           rmw_handle, topic_name, depth)
        data.finalize()

        pub = DataFrameFormatted._build_publisher(data)

        expect = pd.DataFrame.from_dict(
            [{
                'publisher_id': f'publisher_{pub_handle}',
                'publisher_handle': pub_handle,
                'node_handle': node_handle,
                'topic_name': topic_name,
                'depth': depth,
                'construction_order': construction_order
            }]
        ).convert_dtypes()
        assert pub.df.equals(expect)

    def test_init(self, mocker, create_trace_point_data):
        exec_mock = create_trace_point_data()
        node_mock = create_trace_point_data()
        timer_mock = create_trace_point_data()
        timer_control_mock = create_trace_point_data()
        sub_cb_mock = create_trace_point_data()
        srv_cb_mock = create_trace_point_data()
        cbg_mock = create_trace_point_data()
        pub_mock = create_trace_point_data()
        sub_mock = create_trace_point_data()
        srv_mock = create_trace_point_data()
        tim_mock = create_trace_point_data()
        tilde_sub_mock = create_trace_point_data()
        tilde_pub_mock = create_trace_point_data()
        tilde_sub_id_mock = create_trace_point_data()

        mocker.patch.object(
            DataFrameFormatted, '_build_executor', return_value=exec_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_nodes', return_value=node_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_timer_callbacks', return_value=timer_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_timer_control', return_value=timer_control_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_sub_callbacks', return_value=sub_cb_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_srv_callbacks', return_value=srv_cb_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_cbg', return_value=cbg_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_publisher', return_value=pub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_subscription', return_value=sub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_service', return_value=srv_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_timer', return_value=tim_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_subscription', return_value=tilde_sub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_publisher', return_value=tilde_pub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_sub_id', return_value=tilde_sub_id_mock)
        data_mock = mocker.Mock(spec=Ros2DataModel)
        formatted = DataFrameFormatted(data_mock)

        assert formatted.executor == exec_mock
        assert formatted.nodes == node_mock
        assert formatted.timer_callbacks == timer_mock
        assert formatted.timer_controls == timer_control_mock
        assert formatted.subscription_callbacks == sub_cb_mock
        assert formatted.callback_groups == cbg_mock
        assert formatted.service_callbacks == srv_cb_mock
        assert formatted.publishers == pub_mock
        assert formatted.subscriptions == sub_mock
        assert formatted.services == srv_mock
        assert formatted.timers == tim_mock

    def test_tilde_subscription(self, mocker):
        data = Ros2DataModel()
        data.finalize()
        formatted = DataFrameFormatted(data)

        data = formatted.tilde_subscriptions
        columns = [
            'tilde_subscription',
            'node_name',
            'topic_name',
        ]
        df_expect = pd.DataFrame(columns=columns, dtype='Int64')
        assert data.df.equals(df_expect)
