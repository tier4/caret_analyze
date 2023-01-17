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

from caret_analyze.plot.callback_scheduling import CallbackSchedulingPlot
from caret_analyze.runtime import Application, CallbackGroup, Communication, Executor, Node, Path


class TestCallbackSchedulingPlot:

    def test_get_callback_groups_app(self, mocker):
        app_mock = mocker.Mock(spec=Application)
        cbg_mock = mocker.Mock(spec=CallbackGroup)
        mocker.patch.object(app_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(app_mock) == [cbg_mock]

    def test_get_callback_groups_executor(self, mocker):
        exe_mock = mocker.Mock(spec=Executor)
        cbg_mock = mocker.Mock(spec=CallbackGroup)
        mocker.patch.object(exe_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(exe_mock) == [cbg_mock]

    def test_get_callback_groups_node(self, mocker):
        node_mock = mocker.Mock(spec=Node)
        cbg_mock = mocker.Mock(spec=CallbackGroup)
        mocker.patch.object(node_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(node_mock) == [cbg_mock]

    def test_get_callback_groups_node_none(self, mocker):
        node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(node_mock, 'callback_groups', None)

        assert CallbackSchedulingPlot._get_callback_groups(node_mock) == []

    def test_get_callback_groups_path(self, mocker):
        pub_node_mock = mocker.Mock(spec=Node)
        cbg_mock0 = mocker.Mock(spec=CallbackGroup)
        mocker.patch.object(pub_node_mock, 'callback_groups', [cbg_mock0])
        sub_node_mock = mocker.Mock(spec=Node)
        cbg_mock1 = mocker.Mock(spec=CallbackGroup)
        mocker.patch.object(sub_node_mock, 'callback_groups', [cbg_mock1])
        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(comm_mock, 'publish_node', pub_node_mock)
        mocker.patch.object(comm_mock, 'subscribe_node', sub_node_mock)
        path_mock = mocker.Mock(spec=Path)
        mocker.patch.object(path_mock, 'communications', [comm_mock])

        assert CallbackSchedulingPlot._get_callback_groups(path_mock) == [cbg_mock0, cbg_mock1]

    def test_get_callback_groups_path_none(self, mocker):
        pub_node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(pub_node_mock, 'callback_groups', None)
        sub_node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(sub_node_mock, 'callback_groups', None)
        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(comm_mock, 'publish_node', pub_node_mock)
        mocker.patch.object(comm_mock, 'subscribe_node', sub_node_mock)
        path_mock = mocker.Mock(spec=Path)
        mocker.patch.object(path_mock, 'communications', [comm_mock])

        assert CallbackSchedulingPlot._get_callback_groups(path_mock) == []

    def test_get_callback_groups_callback_groups(self, mocker):
        cbg_mock = mocker.Mock(spec=CallbackGroup)

        assert CallbackSchedulingPlot._get_callback_groups(cbg_mock) == [cbg_mock]

    def test_get_callback_groups_callback_groups_list(self, mocker):
        cbg_mock0 = mocker.Mock(spec=CallbackGroup)
        cbg_mock1 = mocker.Mock(spec=CallbackGroup)

        assert CallbackSchedulingPlot._get_callback_groups(
            [cbg_mock0, cbg_mock1]) == [cbg_mock0, cbg_mock1]
