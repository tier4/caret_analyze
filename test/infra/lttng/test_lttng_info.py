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


from caret_analyze.infra.lttng.lttng_info import (DataFrameFormatted,
                                                  LttngInfo)
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (CallbackGroupValueLttng,
                                                     NodeValueLttng,
                                                     PublisherValueLttng,
                                                     SubscriptionCallbackValueLttng,
                                                     TimerCallbackValueLttng)
from caret_analyze.value_objects import (CallbackGroupType, ExecutorType,
                                         ExecutorValue)
from caret_analyze.value_objects.node import NodeValue

import pandas as pd
from pytest_mock import MockerFixture


class TestLttngInfo:

    def test_rmw_implementation(self, mocker: MockerFixture):
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

    def test_get_node_names(self, mocker: MockerFixture):
        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        nodes_df = pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': 2,
                    'node_name': '/node',
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'nodes_df', nodes_df)

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)
        nodes = info.get_nodes()
        assert len(nodes) == 1
        expect = NodeValueLttng('/node', 'node_id')
        assert nodes == [expect]

    def test_get_publishers_info(self, mocker: MockerFixture):
        data = Ros2DataModel()

        publisher_handle = 9
        depth = 5
        node_handle = 3
        node_name = 'node_name'
        tilde_publisher = 8

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        pub_df = pd.DataFrame.from_dict(
            [
                {
                    'publisher_handle': publisher_handle,
                    'node_handle': node_handle,
                    'topic_name': '/topic_name',
                    'depth': depth
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'publishers_df', pub_df)

        tilde_pub_df = pd.DataFrame.from_dict(
            [
                {
                    'tilde_publisher': tilde_publisher,
                    'node_name': node_name,
                    'topic_name': '/topic_name',
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'tilde_publishers_df', tilde_pub_df)

        node_df = pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node',
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'nodes_df', node_df)

        pub_info_expect = PublisherValueLttng(
            node_name='/node',
            topic_name='/topic_name',
            node_id='node_id',
            callback_ids=None,
            publisher_handle=publisher_handle,
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

    def test_get_timer_callbacks_info(self, mocker: MockerFixture):

        node_handle = 1
        timer_handle = 3
        period_ns = 8
        callback_object = 11
        callback_group_addr = 14
        timer_handle = 15
        symbol = 'symbol'

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        timer_df = pd.DataFrame.from_dict(
            [
                {
                    'callback_object': callback_object,
                    'node_handle': node_handle,
                    'timer_handle': timer_handle,
                    'callback_group_addr': callback_group_addr,
                    'period_ns': period_ns,
                    'symbol': symbol,
                    'callback_id': 'timer_callback_0'
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'timer_callbacks_df', timer_df)

        node_df = pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': '/node1'
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'nodes_df', node_df)
        data = Ros2DataModel()

        mocker.patch.object(LttngInfo, '_get_sub_cbs_without_pub', return_value={})
        # mocker.patch.object(LttngInfo, '_get_timer_cbs_without_pub', return_value={})

        data.finalize()
        info = LttngInfo(data)

        timer_cbs_info = info.get_timer_callbacks(NodeValue('/node1', 'node_id'))
        timer_cb_info_expct = TimerCallbackValueLttng(
            'timer_callback_0',
            'node_id',
            '/node1',
            symbol,
            period_ns,
            timer_handle,
            None,
            callback_object=callback_object
        )

        assert timer_cbs_info == [timer_cb_info_expct]

        assert info.get_timer_callbacks(NodeValue('/', 'id')) == []

    def test_get_subscription_callbacks_info(self, mocker: MockerFixture):
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

        sub_df = pd.DataFrame.from_dict(
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
                    'depth': depth[0]
                },
                {
                    'callback_object': callback_object[1],
                    'node_handle': node_handle[1],
                    'subscription_handle': subscription_handle[1],
                    'callback_group_addr': cbg_addr[1],
                    'topic_name': topic_name[1],
                    'symbol': symbol[1],
                    'callback_id': 'subscription_callback_1',
                    'depth': depth[1]
                }
            ]
        ).convert_dtypes()
        mocker.patch.object(
            formatted_mock, 'subscription_callbacks_df', sub_df)

        tilde_sub_df = pd.DataFrame.from_dict(
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
        ).convert_dtypes()
        mocker.patch.object(
            formatted_mock, 'tilde_subscriptions_df', tilde_sub_df)

        node_df = pd.DataFrame.from_dict(
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
        ).convert_dtypes()
        mocker.patch.object(formatted_mock, 'nodes_df', node_df)

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
            tilde_subscription=tilde_subscription[0]
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
            tilde_subscription=tilde_subscription[1]
        )
        assert sub_cbs_info == [sub_cb_info_expect]

        sub_cbs_info = info.get_subscription_callbacks(NodeValue('/', '/'))
        assert sub_cbs_info == []

    def test_get_callback_groups_info(self, mocker: MockerFixture):
        node_handle = 3
        callback_object = 10
        node_name = '/node1'
        cbg_addr = 8
        exec_addr = 9

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        timer_df = pd.DataFrame.from_dict(
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
        )
        mocker.patch.object(formatted_mock, 'timer_callbacks_df', timer_df)

        sub_df = pd.DataFrame.from_dict(
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
        )
        mocker.patch.object(
            formatted_mock, 'subscription_callbacks_df', sub_df)

        node_df = pd.DataFrame.from_dict(
            [
                {
                    'node_id': 'node_id',
                    'node_handle': node_handle,
                    'node_name': node_name
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'nodes_df', node_df)

        cbg_df = pd.DataFrame.from_dict(
            [
                {
                    'callback_group_id': 'callback_group_id',
                    'callback_group_addr': cbg_addr,
                    'executor_addr': exec_addr,
                    'group_type_name': CallbackGroupType.REENTRANT.type_name,
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'callback_groups_df', cbg_df)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)
        cbg_info = info.get_callback_groups(NodeValue(node_name, 'node_id'))

        cbg_info_expect = CallbackGroupValueLttng(
            CallbackGroupType.REENTRANT.type_name,
            '/node1',
            'node_id',
            ('timer_callback_0', 'subscription_callback_0'),
            'callback_group_id',
            callback_group_addr=cbg_addr,
            executor_addr=exec_addr,
        )
        assert cbg_info == [cbg_info_expect]

    def test_get_executors_info(self, mocker: MockerFixture):
        cbg_addr = 13
        executor = 15

        formatted_mock = mocker.Mock(spec=DataFrameFormatted)
        mocker.patch('caret_analyze.infra.lttng.lttng_info.DataFrameFormatted',
                     return_value=formatted_mock)

        exec_df = pd.DataFrame.from_dict(
            [
                {
                    'executor_addr': executor,
                    'executor_type_name': ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
                }
            ]
        )
        mocker.patch.object(formatted_mock, 'executor_df', exec_df)
        cbg_df = pd.DataFrame.from_dict([
            {
                'callback_group_id': 'callback_group_id',
                'callback_group_addr': cbg_addr,
                'executor_addr': executor,
                'group_type_name': CallbackGroupType.REENTRANT.type_name,
            }
        ])
        mocker.patch.object(formatted_mock, 'callback_groups_df', cbg_df)

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        execs_info = info.get_executors()

        exec_info_expect = ExecutorValue(
            ExecutorType.SINGLE_THREADED_EXECUTOR.type_name,
            ('callback_group_id',),
        )

        assert execs_info == [exec_info_expect]


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

        data.add_node(node_handle, 0, 0, rmw_handle, 'node1', '/')

        data.add_timer(timer_handle, 0, period_ns, 0)
        data.add_callback_object(timer_handle, 0, callback_object)
        data.add_timer_node_link(timer_handle, 0, node_handle)
        data.add_callback_symbol(callback_object, 0, symbol)

        data.callback_group_add_timer(
            callback_group_addr, 0, timer_handle
        )
        data.finalize()

        timer_df = DataFrameFormatted._build_timer_callbacks_df(data)

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
                },
            ]
        )
        assert timer_df.equals(expect)

    def test_build_timer_control_df(self):
        data = Ros2DataModel()
        timer_handle = 3
        period = 8
        timestamp = 15
        params = {'period': period}
        type_name = 'init'

        data.add_timer(timer_handle, timestamp, period, 0)

        data.finalize()

        timer_df = DataFrameFormatted._build_timer_control_df(data)

        expect = pd.DataFrame.from_dict(
            [
                {
                    'timestamp': timestamp,
                    'timer_handle': timer_handle,
                    'type': type_name,
                    'params': params,
                },
            ]
        )
        assert timer_df.equals(expect)

    def test_build_subscription_callbacks_df(self, mocker: MockerFixture):
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

        data.add_node(node_handle, 0, 0, rmw_handle, 'node1', '/')

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

        sub_df = DataFrameFormatted._build_sub_callbacks_df(data)

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
                    'depth': depth[0]
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
                },
            ]
        ).convert_dtypes()
        assert sub_df.equals(expect)

    def test_executor_df(self):
        data = Ros2DataModel()
        exec_addr = 3
        exec_type = 'single_threaded_executor'

        data.add_executor(exec_addr, 0, exec_type)
        data.finalize()

        exec_df = DataFrameFormatted._build_executor_df(data)

        expect = [
            {
                'executor_id': f'executor_{exec_addr}',
                'executor_addr': exec_addr,
                'executor_type_name': exec_type,
            }
        ]
        expect_df = pd.DataFrame.from_dict(expect)

        assert exec_df.equals(expect_df)

    def test_executor_static_df(self):
        data = Ros2DataModel()
        exec_addr = 3
        exec_type = 'static_single_threaded_executor'
        collector_addr = 4

        data.add_executor_static(exec_addr, collector_addr, 0, exec_type)
        data.finalize()

        exec_df = DataFrameFormatted._build_executor_df(data)

        expect = [
            {
                'executor_id': f'executor_{exec_addr}',
                'executor_addr': exec_addr,
                'executor_type_name': exec_type,
            }
        ]
        expect_df = pd.DataFrame.from_dict(expect)

        assert exec_df.equals(expect_df)

    def test_format_subscription_callback_object(self, mocker: MockerFixture):
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

        sub_df = DataFrameFormatted._format_subscription_callback_object(data)

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
        assert sub_df.equals(expect)

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
        nodes_df = DataFrameFormatted._build_nodes_df(data)

        expect = pd.DataFrame.from_dict(
            [{
                'node_id': '/node1_0',
                'node_handle': node_handle,
                'node_name': '/node1',
            }]
        )
        assert nodes_df.equals(expect)

    def test_build_callback_groups_df(self):
        exec_addr = 2
        callback_group_addr = 3
        group_type = 'reentrant'

        data = Ros2DataModel()
        data.add_callback_group(exec_addr, 0, callback_group_addr, group_type)
        data.finalize()

        cbg_df = DataFrameFormatted._build_cbg_df(data)

        expect = pd.DataFrame.from_dict(
            [{
                'callback_group_id': f'callback_group_{callback_group_addr}',
                'callback_group_addr': callback_group_addr,
                'group_type_name': group_type,
                'executor_addr': exec_addr,
            }]
        )
        assert cbg_df.equals(expect)

    def test_build_callback_groups_static_df(self):
        group_type = 'reentrant'
        collector_addr = 2
        cbg_addr = 3
        exec_addr = 4

        data = Ros2DataModel()
        data.add_callback_group_static_executor(collector_addr, 0, cbg_addr, group_type)
        data.add_executor_static(exec_addr, collector_addr, 0, 'exec_type')
        data.finalize()

        cbg_df = DataFrameFormatted._build_cbg_df(data)

        expect = pd.DataFrame.from_dict(
            [{
                'callback_group_id': f'callback_group_{cbg_addr}',
                'callback_group_addr': cbg_addr,
                'group_type_name': group_type,
                'executor_addr': exec_addr,
            }]
        )
        assert cbg_df.equals(expect)

    def test_build_publisher_df(self):
        pub_handle = 1
        node_handle = 2
        rmw_handle = 3
        topic_name = 'topic'
        depth = 5

        data = Ros2DataModel()
        data.add_publisher(pub_handle, 0, node_handle,
                           rmw_handle, topic_name, depth)
        data.finalize()

        pub_df = DataFrameFormatted._build_publisher_df(data)

        expect = pd.DataFrame.from_dict(
            [{
                'publisher_id': f'publisher_{pub_handle}',
                'publisher_handle': pub_handle,
                'node_handle': node_handle,
                'topic_name': topic_name,
                'depth': depth
            }]
        )
        assert pub_df.equals(expect)

    def test_init(self, mocker: MockerFixture):
        exec_mock = mocker.Mock(spec=pd.DataFrame)
        node_mock = mocker.Mock(spec=pd.DataFrame)
        timer_mock = mocker.Mock(spec=pd.DataFrame)
        timer_control_mock = mocker.Mock(spec=pd.DataFrame)
        sub_mock = mocker.Mock(spec=pd.DataFrame)
        srv_mock = mocker.Mock(spec=pd.DataFrame)
        cbg_mock = mocker.Mock(spec=pd.DataFrame)
        pub_mock = mocker.Mock(spec=pd.DataFrame)
        tilde_sub_mock = mocker.Mock(spec=pd.DataFrame)
        tilde_pub_mock = mocker.Mock(spec=pd.DataFrame)
        tilde_sub_id_mock = mocker.Mock(spec=pd.DataFrame)

        mocker.patch.object(
            DataFrameFormatted, '_build_executor_df', return_value=exec_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_nodes_df', return_value=node_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_timer_callbacks_df', return_value=timer_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_timer_control_df', return_value=timer_control_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_sub_callbacks_df', return_value=sub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_srv_callbacks_df', return_value=srv_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_cbg_df', return_value=cbg_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_publisher_df', return_value=pub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_subscription_df', return_value=tilde_sub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_publisher_df', return_value=tilde_pub_mock)
        mocker.patch.object(
            DataFrameFormatted, '_build_tilde_sub_id_df', return_value=tilde_sub_id_mock)
        data_mock = mocker.Mock(spec=Ros2DataModel)
        formatted = DataFrameFormatted(data_mock)

        assert formatted.executor_df == exec_mock
        assert formatted.nodes_df == node_mock
        assert formatted.timer_callbacks_df == timer_mock
        assert formatted.timer_controls_df == timer_control_mock
        assert formatted.subscription_callbacks_df == sub_mock
        assert formatted.callback_groups_df == cbg_mock
        assert formatted.services_df == srv_mock
        assert formatted.publishers_df == pub_mock

    # def test_build_subscription_callbacks_df(self):

    #     data = Ros2DataModel()

    #     node_handle = [0, 1]
    #     rmw_handle = [2, 3]
    #     subscription_handle = [4, 5, 6]
    #     callback_object_intra = [7]
    #     callback_object_inter = [8, 9, 10]
    #     depth = [11, 12, 13]
    #     symbol = ['symbol1', 'symbol2', 'symbol3']
    #     topic_name = ['topic_name1', 'topic_name2', 'topic_name3']

    #     data.add_node(node_handle[0], 0, 0, rmw_handle[0], 'node1', '/')

    #     # node_handle[0] intra / inter
    #     data.add_rclcpp_subscription(
    #         callback_object_intra[0], 0, subscription_handle[0])
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[0], 0, subscription_handle[0])
    #     data.add_rcl_subscription(
    #         subscription_handle[0], 0, node_handle[0], rmw_handle[0], topic_name[0], depth[0])
    #     data.add_callback_object(
    #         subscription_handle[0], 0, callback_object_intra[0])
    #     data.add_callback_object(
    #         subscription_handle[0], 0, callback_object_inter[0])
    #     data.add_callback_symbol(callback_object_intra[0], 0, symbol[0])

    #     # node_handle[0] inter
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[1], 0, subscription_handle[1])
    #     data.add_rcl_subscription(
    #         subscription_handle[1], 0, node_handle[0], rmw_handle[0], topic_name[1], depth[1])
    #     data.add_callback_object(
    #         subscription_handle[1], 0, callback_object_inter[1])
    #     data.add_callback_symbol(callback_object_inter[1], 0, symbol[1])

    #     data.add_node(node_handle[1], 0, 0, rmw_handle[1], 'node2', '/')

    #     # node_handle[1] inter
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[2], 0, subscription_handle[2])
    #     data.add_rcl_subscription(
    #         subscription_handle[2], 0, node_handle[1], rmw_handle[1], topic_name[2], depth[2])
    #     data.add_callback_object(
    #         subscription_handle[2], 0, callback_object_inter[2])
    #     data.add_callback_symbol(callback_object_inter[2], 0, symbol[2])

    #     data.finalize()

    #     df = formated.timer_callbacks_df
    #     expect = pd.DataFrame.from_dict(
    #         [
    #             {
    #                 'subscription_handle': subscription_handle[0],
    #                 'topic_name': topic_name[0],
    #                 'symbol': symbol[0],
    #                 'callback_object_inter': callback_object_inter[0],
    #                 'callback_object_intra': callback_object_intra[0],
    #                 'callback_id': '/node1/subscription_callback_1',
    #             },
    #             {
    #                 'subscription_handle': subscription_handle[1],
    #                 'topic_name': topic_name[1],
    #                 'symbol': symbol[1],
    #                 'callback_object_inter': callback_object_inter[1],
    #                 'callback_object_intra': None,
    #                 'callback_id': '/node1/subscription_callback_1',
    #             },
    #             {
    #                 'subscription_handle': subscription_handle[2],
    #                 'topic_name': topic_name[2],
    #                 'symbol': symbol[2],
    #                 'callback_object_inter': callback_object_inter[2],
    #                 'callback_object_intra': None,
    #                 'callback_id': '/node2/subscription_callback_0',
    #             },
    #         ]
    #     )
    #     assert df.equals(expect)

    # def test_subscription_callbacks_full_df(self):

    #     data = Ros2DataModel()

    #     node_handle = [0, 1]
    #     rmw_handle = [2, 3]
    #     subscription_handle = [4, 5, 6]
    #     callback_object_intra = [7]
    #     callback_object_inter = [8, 9, 10]
    #     depth = [11, 12, 13]
    #     symbol = ['symbol1', 'symbol2', 'symbol3']
    #     topic_name = ['topic_name1', 'topic_name2', 'topic_name3']

    #     data.add_node(node_handle[0], 0, 0, rmw_handle[0], 'node1', '/')

    #     # node_handle[0] intra / inter
    #     data.add_rclcpp_subscription(
    #         callback_object_intra[0], 0, subscription_handle[0])
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[0], 0, subscription_handle[0])
    #     data.add_rcl_subscription(
    #         subscription_handle[0], 0, node_handle[0], rmw_handle[0], topic_name[0], depth[0])
    #     data.add_callback_object(
    #         subscription_handle[0], 0, callback_object_intra[0])
    #     data.add_callback_object(
    #         subscription_handle[0], 0, callback_object_inter[0])
    #     data.add_callback_symbol(callback_object_intra[0], 0, symbol[0])

    #     # node_handle[0] inter
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[1], 0, subscription_handle[1])
    #     data.add_rcl_subscription(
    #         subscription_handle[1], 0, node_handle[0], rmw_handle[0], topic_name[1], depth[1])
    #     data.add_callback_object(
    #         subscription_handle[1], 0, callback_object_inter[1])
    #     data.add_callback_symbol(callback_object_inter[1], 0, symbol[1])

    #     data.add_node(node_handle[1], 0, 0, rmw_handle[1], 'node2', '/')

    #     # node_handle[1] inter
    #     data.add_rclcpp_subscription(
    #         callback_object_inter[2], 0, subscription_handle[2])
    #     data.add_rcl_subscription(
    #         subscription_handle[2], 0, node_handle[1], rmw_handle[1], topic_name[2], depth[2])
    #     data.add_callback_object(
    #         subscription_handle[2], 0, callback_object_inter[2])
    #     data.add_callback_symbol(callback_object_inter[2], 0, symbol[2])

    #     data.finalize()

    #     formated = DataFrameFormatted(data)

    #     df = formated.timer_callbacks_df
    #     expect = pd.DataFrame.from_dict(
    #         [
    #             {
    #                 'node_name': '/node1',
    #                 'node_handle': node_handle[0],
    #                 'subscription_handle': subscription_handle[0],
    #                 'topic_name': topic_name[0],
    #                 'symbol': symbol[0],
    #                 'callback_object_inter': callback_object_inter[0],
    #                 'callback_object_intra': callback_object_intra[0],
    #                 'callback_id': '/node1/subscription_callback_1',
    #             },
    #             {
    #                 'node_name': '/node1',
    #                 'node_handle': node_handle[0],
    #                 'subscription_handle': subscription_handle[1],
    #                 'topic_name': topic_name[1],
    #                 'symbol': symbol[1],
    #                 'callback_object_inter': callback_object_inter[1],
    #                 'callback_object_intra': None,
    #                 'callback_id': '/node1/subscription_callback_1',
    #             },
    #             {
    #                 'node_name': '/node2',
    #                 'node_handle': node_handle[1],
    #                 'subscription_handle': subscription_handle[2],
    #                 'topic_name': topic_name[2],
    #                 'symbol': symbol[2],
    #                 'callback_object_inter': callback_object_inter[2],
    #                 'callback_object_intra': None,
    #                 'callback_id': '/node2/subscription_callback_0',
    #             },
    #         ]
    #     )
    #     assert df.equals(expect)

    # def test_timer_callbacks_df(self):
    #     data = Ros2DataModel()

    #     node_handle = [1, 2]
    #     timer_handle = [3, 4, 5]
    #     rmw_handle = [6,  7]
    #     period_ns = [8, 9, 10]
    #     callback_object = [11, 12, 13]
    #     symbol = ['symbol1', 'symbol2', 'symbol3']

    #     data.add_node(node_handle[0], 0, 0, rmw_handle[0], 'node1', '/')
    #     data.add_node(node_handle[1], 0, 0, rmw_handle[1], 'node2', '/')

    #     data.add_timer(timer_handle[0], 0, period_ns[0], 0)
    #     data.add_callback_object(timer_handle[0], 0, callback_object[0])
    #     data.add_timer_node_link(timer_handle[0], 0, node_handle[0])
    #     data.add_callback_symbol(callback_object[0], 0, symbol[0])

    #     data.add_timer(timer_handle[1], 0, period_ns[1], 0)
    #     data.add_callback_object(timer_handle[1], 0, callback_object[1])
    #     data.add_timer_node_link(timer_handle[1], 0, node_handle[0])
    #     data.add_callback_symbol(callback_object[1], 0, symbol[1])

    #     data.add_timer(timer_handle[2], 0, period_ns[2], tid=0)
    #     data.add_callback_object(timer_handle[2], 0, callback_object[2])
    #     data.add_timer_node_link(timer_handle[2], 0, node_handle[1])
    #     data.add_callback_symbol(callback_object[2], 0, symbol[2])

    #     data.finalize()

    #     formated = DataFrameFormatted(data)

    #     df = formated.timer_callbacks_df
    #     expect = pd.DataFrame.from_dict(
    #         [
    #             {
    #                 'node_name': '/node1',
    #                 'node_handle': node_handle[0],
    #                 'timer_handle': timer_handle[0],
    #                 'period': period_ns[0],
    #                 'symbol': symbol[0],
    #                 'callback_object': callback_object[0],
    #                 'callback_id': '/node1/timer_callback_0',
    #             },
    #             {
    #                 'node_name': '/node1',
    #                 'node_handle': node_handle[0],
    #                 'timer_handle': timer_handle[1],
    #                 'period': period_ns[1],
    #                 'symbol': symbol[1],
    #                 'callback_object': callback_object[1],
    #                 'callback_id': '/node1/timer_callback_1',
    #             },
    #             {
    #                 'node_name': '/node2',
    #                 'node_handle': node_handle[1],
    #                 'timer_handle': timer_handle[2],
    #                 'period': period_ns[2],
    #                 'symbol': symbol[2],
    #                 'callback_object': callback_object[2],
    #                 'callback_id': '/node2/timer_callback_0',
    #             },
    #         ]
    #     )
    #     assert df.equals(expect)
