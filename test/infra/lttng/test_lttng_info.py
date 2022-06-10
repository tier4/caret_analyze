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


from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.infra.lttng.lttng_info import (
    LttngInfo,
)
from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.value_objects import (
    CallbackGroupValueLttng,
    ExecutorValueLttng,
    IntraProcessBufferValueLttng,
    NodeValueLttng,
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
)

import pytest


class TestLttngInfo:

    @pytest.mark.parametrize(
        'lib_caret_version',
        [None, '0']
    )
    def test_get_nodes(self, lib_caret_version):
        data = Ros2DataModel()

        node_id = 'node_0'
        node_handle = 0
        rmw_handle = 2
        pid, tid = 5, 6

        if lib_caret_version is not None:
            data.add_rcl_init_caret(lib_caret_version, 0, pid, tid)
        data.add_rcl_node_init(pid, tid, node_handle, 1, rmw_handle, 'node', 'ns')
        data.finalize()

        info = LttngInfo(data)
        nodes = info.get_nodes()

        expect = NodeValueLttng(pid, 'ns/node', node_handle, node_id, lib_caret_version)

        assert nodes == [expect]

    @pytest.mark.parametrize(
        'rmw_impl',
        [None, 'rmw_impl']
    )
    def test_rmw_implementation(self, rmw_impl):
        pid, tid = 1, 2

        data = Ros2DataModel()
        if rmw_impl is not None:
            data.add_rmw_implementation(pid, tid, rmw_impl)
        data.finalize()
        info = LttngInfo(data)

        assert info.get_rmw_impl() is rmw_impl

    def test_get_node(self):
        data = Ros2DataModel()
        pid, tid = 1, 2
        rmw_handle = 3
        node_handle = 4

        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.finalize()
        info = LttngInfo(data)

        with pytest.raises(ItemNotFoundError):
            info.get_node('not_exist')

        node = info.get_node('/node')
        assert node.node_name == '/node'

    def test_get_publishers(self):

        depth = 5
        node_handle = 3
        pid, tid = 1, 2
        pub_handle = 10
        rmw_handle = 11
        topic_name = 'topic'

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.add_rcl_node_init(pid, tid, node_handle+1, 0, rmw_handle, 'node_empty', '/')
        data.add_rcl_publisher_init(
            pid, tid, pub_handle, 0, node_handle, rmw_handle, topic_name, depth)
        data.finalize()

        pub_expect = PublisherValueLttng(
            pid=pid,
            publisher_id='publisher_0',
            caret_rclcpp_version=None,
            node_name='/node',
            topic_name=topic_name,
            node_id='node_0',
            callback_ids=None,
            publisher_handle=pub_handle,
            tilde_publisher=None
        )

        info = LttngInfo(data)
        node = info.get_node('/node')
        pubs = info.get_publishers(node)

        assert pubs == [pub_expect]

        node = info.get_node('/node_empty')
        pubs = info.get_publishers(node)
        assert pubs == []

    def test_get_timer_callbacks(self):

        node_handle = 1
        timer_handle = 3
        period = 8
        callback_object = 11
        timer_handle = 15
        symbol = 'symbol'
        pid, tid = 18, 19
        rmw_handle = 20
        cbg_addr = 21

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.add_rcl_node_init(pid, tid, node_handle+1, 0, rmw_handle, 'node_empty', '/')
        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.finalize()
        info = LttngInfo(data)

        timer_cb_expct = TimerCallbackValueLttng(
            pid,
            'timer_callback_0',
            'node_0',
            '/node',
            symbol,
            period,
            timer_handle,
            publish_topic_names=None,
            callback_object=callback_object
        )

        node = info.get_node('/node')
        timers = info.get_timer_callbacks(node)
        assert timers == [timer_cb_expct]

        node = info.get_node('/node_empty')
        timers = info.get_timer_callbacks(node)
        assert timers == []

    def test_client_callbacks(self):
        pid, tid = 1, 2
        node_handle = 3
        rmw_handle = 4

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.finalize()
        info = LttngInfo(data)

        node = info.get_node('/node')
        clts = info.get_client_callbacks(node)
        assert clts == []

    def test_get_ipc_buffers(self):
        pid, tid = 1, 2
        node_handle = 3
        rmw_handle = 4
        buffer = 5
        capacity = 8
        topic_name = '/topic'
        sub = 9
        sub_handle = 10
        callback_object = 11
        symbol = 'symbol'
        cbg_addr = 12
        depth = 13

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.add_rcl_node_init(pid, tid, node_handle+1, 0, rmw_handle, 'node_empty', '/')
        data.add_construct_ring_buffer(pid, tid, 1, buffer, capacity)
        data.add_rclcpp_subscription_init(pid, tid, sub, 2, sub_handle)
        data.add_rcl_subscription_init(
            pid, tid, sub_handle, 3, node_handle, rmw_handle, topic_name, depth)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 6, symbol)
        data.add_callback_group_add_subscription(pid, tid, cbg_addr, 9, sub_handle)
        data.add_rclcpp_subscription_callback_added(pid, tid, sub, 10, callback_object)
        data.finalize()

        info = LttngInfo(data)

        node = info.get_node('/node_empty')
        buffs = info.get_ipc_buffers(node)
        assert buffs == []

        node = info.get_node('/node')
        buffs = info.get_ipc_buffers(node)
        ipc_except = IntraProcessBufferValueLttng(
            pid,
            '/node',
            topic_name,
            buffer
        )
        assert buffs == [ipc_except]

    def test_get_subscription_callbacks(self):
        pid, tid = 1, 2
        node_handle = 3
        rmw_handle = 4
        topic_name = '/topic'
        sub = 9
        sub_handle = 10
        callback_object = 11
        symbol = 'symbol'
        cbg_addr = 12
        depth = 13

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.add_rcl_node_init(pid, tid, node_handle+1, 0, rmw_handle, 'node_empty', '/')
        data.add_rclcpp_subscription_init(pid, tid, sub, 2, sub_handle)
        data.add_rcl_subscription_init(
            pid, tid, sub_handle, 3, node_handle, rmw_handle, topic_name, depth)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 6, symbol)
        data.add_callback_group_add_subscription(pid, tid, cbg_addr, 9, sub_handle)
        data.add_rclcpp_subscription_callback_added(pid, tid, sub, 10, callback_object)
        data.finalize()

        info = LttngInfo(data)

        node = info.get_node('/node_empty')
        subs = info.get_subscription_callbacks(node)
        assert subs == []

        node = info.get_node('/node')
        subs = info.get_subscription_callbacks(node)
        sub_except = SubscriptionCallbackValueLttng(
            pid,
            'subscription_callback_0',
            'node_0',
            '/node',
            symbol,
            topic_name,
            sub_handle,
            None,
            callback_object,
            None,
            None
        )
        assert subs == [sub_except]

    def test_get_callback_groups(self):
        node_handle = 3
        callback_object = 10
        cbg_addr = 8
        exec_addr = 9
        rmw_handle = 10
        pid, tid = 11, 12
        timer_handle = 13
        symbol = ''
        period = 2

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')
        data.add_rcl_node_init(pid, tid, node_handle+1, 0, rmw_handle, 'node_empty', '/')

        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.add_add_callback_group(pid, tid, exec_addr, 1,  cbg_addr, 'mutually_exclusive')
        data.finalize()

        info = LttngInfo(data)

        node = info.get_node('/node_empty')
        cbgs = info.get_callback_groups(node)
        assert cbgs == []

        node = info.get_node('/node')
        cbgs = info.get_callback_groups(node)
        cbg_expect = CallbackGroupValueLttng(
            pid, 'mutually_exclusive', '/node', 'node_0', (),
            'callback_group_0', cbg_addr, exec_addr
        )
        assert cbgs == [cbg_expect]

    def test_get_callback_groups_duplicated_add_cbg(self):
        node_handle = 3
        callback_object = 10
        cbg_addr = 8
        exec_addr = 9
        rmw_handle = 10
        pid, tid = 11, 12
        timer_handle = 13
        symbol = ''
        period = 2

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')

        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.add_add_callback_group(pid, tid, exec_addr + 1, 1,  cbg_addr, 'mutually_exclusive')
        data.add_add_callback_group(pid, tid, exec_addr, 2,  cbg_addr, 'mutually_exclusive')
        data.finalize()

        info = LttngInfo(data)

        node = info.get_node('/node')
        cbgs = info.get_callback_groups(node)
        cbg_expect = CallbackGroupValueLttng(
            pid, 'mutually_exclusive', '/node', 'node_0', (),
            'callback_group_1', cbg_addr, exec_addr
        )
        assert cbgs == [cbg_expect]

    def test_get_callback_groups_static(self):
        node_handle = 3
        callback_object = 10
        cbg_addr = 8
        exec_addr = 9
        rmw_handle = 10
        pid, tid = 11, 12
        timer_handle = 13
        symbol = ''
        period = 2
        entity_collector_addr = 88

        data = Ros2DataModel()
        data.add_rcl_node_init(pid, tid, node_handle, 0, rmw_handle, 'node', '/')

        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.add_add_callback_group_static_executor(
            pid, tid, entity_collector_addr, 8, cbg_addr, 'mutually_exclusive')
        data.add_construct_static_executor(
            pid, tid, exec_addr, entity_collector_addr, 9, 'single_threaded_executor')
        data.finalize()

        info = LttngInfo(data)

        node = info.get_node('/node')
        cbgs = info.get_callback_groups(node)
        cbg_expect = CallbackGroupValueLttng(
            pid, 'mutually_exclusive', '/node', 'node_0', (),
            'callback_group_0', cbg_addr, exec_addr
        )
        assert cbgs == [cbg_expect]

    def test_get_executors_info(self):
        pid, tid = 1, 2
        node_handle = 3
        exec_addr = 5
        timer_handle = 6
        period = 7
        callback_object = 8
        symbol = ''
        cbg_addr = 9

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        execs = info.get_executors()
        assert execs == []

        data = Ros2DataModel()
        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.add_add_callback_group(pid, tid, exec_addr, 1,  cbg_addr, 'mutually_exclusive')
        data.add_construct_executor(pid, tid, exec_addr, 1, 'single_threaded_executor')
        data.finalize()

        info = LttngInfo(data)
        execs = info.get_executors()

        exec_expect = ExecutorValueLttng(
            pid, 'executor_0', 'single_threaded_executor', ('callback_group_0',)
        )

        assert execs == [exec_expect]

    def test_get_executors_info_static(self):
        pid, tid = 1, 2
        node_handle = 3
        exec_addr = 5
        timer_handle = 6
        period = 7
        callback_object = 8
        symbol = ''
        cbg_addr = 9
        entity_addr = 10

        data = Ros2DataModel()
        data.finalize()
        info = LttngInfo(data)

        execs = info.get_executors()
        assert execs == []

        data = Ros2DataModel()
        data.add_rcl_timer_init(pid, tid, timer_handle, 1, period)
        data.add_rclcpp_timer_link_node(pid, tid, timer_handle, 2, node_handle)
        data.add_rclcpp_timer_callback_added(pid, tid, timer_handle, 4, callback_object)
        data.add_rclcpp_callback_register(pid, tid, callback_object, 5, symbol)
        data.add_callback_group_add_timer(pid, tid, cbg_addr, 7, timer_handle)
        data.add_add_callback_group_static_executor(
            pid, tid, entity_addr, 1, cbg_addr, 'mutually_exclusive')
        data.add_construct_static_executor(
            pid, tid, exec_addr, entity_addr, 2, 'single_threaded_executor')
        data.finalize()

        info = LttngInfo(data)
        execs = info.get_executors()

        exec_expect = ExecutorValueLttng(
            pid, 'executor_0', 'single_threaded_executor', ('callback_group_0',)
        )

        assert execs == [exec_expect]