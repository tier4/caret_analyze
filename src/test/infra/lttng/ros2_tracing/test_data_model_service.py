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

    def test_get_node_names_not_exist(self):
        data = Ros2DataModel()
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names = data_model_srv.get_node_names(0)
        assert node_names == []

    def test_get_node_names_exist_rcl_node_init_timer(self):
        data = Ros2DataModel()
        cbg_addr = 1
        timer_handle = 2
        node_handle = 3
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_timer_node_link(timer_handle, 0, node_handle)
        data.callback_group_add_timer(cbg_addr, 0, timer_handle)
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names = data_model_srv.get_node_names(cbg_addr)
        assert node_names == ['ns/name']

    def test_get_node_names_exist_rcl_node_init_sub(self):
        data = Ros2DataModel()
        cbg_addr = 1
        sub_handle = 2
        node_handle = 3
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_rcl_subscription(sub_handle, 0, node_handle, 0, 'topic', 0)
        data.callback_group_add_subscription(cbg_addr, 0, sub_handle)
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names = data_model_srv.get_node_names(cbg_addr)
        assert node_names == ['ns/name']

    def test_get_node_names_exist_get_parameters_srv(self):
        data = Ros2DataModel()
        cbg_addr = 1
        srv_handle = 2
        node_handle = 3
        data.add_node(0, node_handle, 0, 0, 'name', 'ns')
        data.add_service(srv_handle, 0, node_handle, 0, 'srv')
        data.callback_group_add_service(cbg_addr, 0, srv_handle)
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names = data_model_srv.get_node_names(cbg_addr)
        assert node_names == ['ns/name']

    def test_get_node_names_multiple_exist(self):
        data = Ros2DataModel()
        duplicated_cbg_addr = 1
        timer_handle1 = 2
        timer_handle2 = 3
        node_handle1 = 4
        node_handle2 = 5
        data.add_node(0, node_handle1, 0, 0, 'name1', 'ns')
        data.add_node(0, node_handle2, 0, 0, 'name2', 'ns')
        data.add_timer_node_link(timer_handle1, 0, node_handle1)
        data.add_timer_node_link(timer_handle2, 0, node_handle2)
        data.callback_group_add_timer(duplicated_cbg_addr, 0, timer_handle1)
        data.callback_group_add_timer(duplicated_cbg_addr, 0, timer_handle2)
        data.finalize()

        data_model_srv = DataModelService(data)
        node_names = data_model_srv.get_node_names(duplicated_cbg_addr)
        assert node_names == ['ns/name1', 'ns/name2']
