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

from caret_analyze.infra.lttng.ros2_tracing.data_model import Ros2DataModel
from caret_analyze.infra.lttng.ros2_tracing.data_model_service \
    import DataModelService


class TestDataModelService:

    def test_get_node_names_and_cb_symbols_not_exist(self):
        data = Ros2DataModel()
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(0)
        assert node_names_and_cb_symbols == []

    def test_get_node_names_and_cb_symbols_exist_rcl_node_init_timer(self):
        data = Ros2DataModel()
        cbg_addr = 1
        timer_handle = 2
        node_handle = 3
        cb_object = 4
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_timer_node_link(timer_handle, 0, node_handle)
        data.callback_group_add_timer(cbg_addr, 0, timer_handle)
        data.add_callback_object(timer_handle, 0, cb_object)
        data.add_callback_symbol(cb_object, 0, 'cb')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name', 'cb')]

    def test_get_node_names_and_cb_symbols_exist_rcl_node_init_sub(self):
        data = Ros2DataModel()
        cbg_addr = 1
        sub_handle = 2
        node_handle = 3
        cb_object = 4
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_rcl_subscription(sub_handle, 0, node_handle, 0, 'topic', 0)
        data.callback_group_add_subscription(cbg_addr, 0, sub_handle)
        data.add_callback_object(sub_handle, 0, cb_object)
        data.add_callback_symbol(cb_object, 0, 'cb')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name', 'cb')]

    def test_get_node_names_and_cb_symbols_exist_get_parameters_srv(self):
        data = Ros2DataModel()
        cbg_addr = 1
        srv_handle = 2
        node_handle = 3
        cb_object = 4
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_service(srv_handle, 0, node_handle, 0, 'srv')
        data.callback_group_add_service(cbg_addr, 0, srv_handle)
        data.add_callback_object(srv_handle, 0, cb_object)
        data.add_callback_symbol(cb_object, 0, 'cb')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name', 'cb')]

    def test_get_node_names_and_cb_symbols_multiple_exist(self):
        data = Ros2DataModel()
        duplicated_cbg_addr = 1
        timer_handle1 = 2
        timer_handle2 = 3
        node_handle1 = 4
        node_handle2 = 5
        cb_object1 = 6
        cb_object2 = 7
        data.add_node(0, node_handle1, 0, 0, 'name1', 'ns')
        data.add_node(0, node_handle2, 0, 0, 'name2', 'ns')
        data.add_timer_node_link(timer_handle1, 0, node_handle1)
        data.add_timer_node_link(timer_handle2, 0, node_handle2)
        data.callback_group_add_timer(duplicated_cbg_addr, 0, timer_handle1)
        data.callback_group_add_timer(duplicated_cbg_addr, 0, timer_handle2)
        data.add_callback_object(timer_handle1, 0, cb_object1)
        data.add_callback_object(timer_handle2, 0, cb_object2)
        data.add_callback_symbol(cb_object1, 0, 'cb1')
        data.add_callback_symbol(cb_object2, 0, 'cb2')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(duplicated_cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name1', 'cb1'), ('ns/name2', 'cb2')]

    def test_get_node_names_and_cb_symbols_multiple_symbols_for_one_handle(self):
        data = Ros2DataModel()
        cbg_addr = 1
        duplicated_timer_handle = 2
        node_handle = 3
        cb_object1 = 4
        cb_object2 = 5
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_timer_node_link(duplicated_timer_handle, 0, node_handle)
        data.callback_group_add_timer(cbg_addr, 0, duplicated_timer_handle)
        data.add_callback_object(duplicated_timer_handle, 0, cb_object1)
        data.add_callback_object(duplicated_timer_handle, 0, cb_object2)
        data.add_callback_symbol(cb_object1, 0, 'cb1')
        data.add_callback_symbol(cb_object2, 0, 'cb2')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name', 'cb1'), ('ns/name', 'cb2')]

    def test_get_node_names_and_cb_symbols_drop_node_name(self):
        data = Ros2DataModel()
        cbg_addr = 1
        timer_handle = 2
        cb_object = 3
        data.callback_group_add_timer(cbg_addr, 0, timer_handle)
        data.add_callback_object(timer_handle, 0, cb_object)
        data.add_callback_symbol(cb_object, 0, 'cb')
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [(None, 'cb')]

    def test_get_node_names_and_cb_symbols_drop_callback_symbol(self):
        data = Ros2DataModel()
        cbg_addr = 1
        timer_handle = 2
        node_handle = 3
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_timer_node_link(timer_handle, 0, node_handle)
        data.callback_group_add_timer(cbg_addr, 0, timer_handle)
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names_and_cb_symbols = \
            data_model_srv.get_node_names_and_cb_symbols(cbg_addr)
        assert node_names_and_cb_symbols == [('ns/name', None)]
