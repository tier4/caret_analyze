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

from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.plot.callback_scheduling import CallbackSchedulingPlot
from caret_analyze.runtime import (Application, CallbackBase, CallbackGroup,
                                   Communication, Executor, Node, Path)

import pytest


class TestCallbackSchedulingPlot:

    def test_get_callback_groups_app(self, mocker):
        app_mock = mocker.Mock(spec=Application)
        cbg_mock = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        mocker.patch.object(app_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(app_mock) == [cbg_mock]

    def test_get_callback_groups_executor(self, mocker):
        exe_mock = mocker.Mock(spec=Executor)
        cbg_mock = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        mocker.patch.object(exe_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(exe_mock) == [cbg_mock]

    def test_get_callback_groups_node(self, mocker):
        node_mock = mocker.Mock(spec=Node)
        cbg_mock = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        mocker.patch.object(node_mock, 'callback_groups', [cbg_mock])

        assert CallbackSchedulingPlot._get_callback_groups(node_mock) == [cbg_mock]

    def test_get_callback_groups_node_none(self, mocker):
        node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(node_mock, 'callback_groups', None)

        with pytest.raises(ItemNotFoundError, match='callback_groups is empty.'):
            CallbackSchedulingPlot._get_callback_groups(node_mock)

    def test_get_callback_groups_path(self, mocker):
        pub_node_mock = mocker.Mock(spec=Node)
        cbg_mock0 = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        mocker.patch.object(pub_node_mock, 'callback_groups', [cbg_mock0])
        sub_node_mock = mocker.Mock(spec=Node)
        cbg_mock1 = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        mocker.patch.object(sub_node_mock, 'callback_groups', [cbg_mock1])
        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(comm_mock, 'publish_node', pub_node_mock)
        mocker.patch.object(comm_mock, 'subscribe_node', sub_node_mock)
        path_mock = mocker.Mock(spec=Path)
        mocker.patch.object(path_mock, 'communications', [comm_mock])

        assert CallbackSchedulingPlot._get_callback_groups(path_mock) == [cbg_mock0, cbg_mock1]

    def test_get_callback_groups_callback_groups(self, mocker):
        cbg_mock = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])

        assert CallbackSchedulingPlot._get_callback_groups(cbg_mock) == [cbg_mock]

    def test_get_callback_groups_callback_groups_list(self, mocker):
        cbg_mock0 = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])
        cbg_mock1 = mocker.Mock(spec=CallbackGroup, callbacks=[mocker.Mock(spec=CallbackBase)])

        assert CallbackSchedulingPlot._get_callback_groups(
            [cbg_mock0, cbg_mock1]) == [cbg_mock0, cbg_mock1]

    def test_get_callback_groups_node_empty_callback_groups_list(self, mocker):
        # Node object with an empty 'callback_groups' list.
        node_mock = mocker.Mock(spec=Node)
        mocker.patch.object(node_mock, 'callback_groups', [])
        with pytest.raises(ItemNotFoundError, match='callback_groups is empty.'):
            CallbackSchedulingPlot._get_callback_groups(node_mock)

    def test_get_callback_groups_callback_group_no_callbacks(self, mocker):
        # CallbackGroup object with an empty 'callbacks' list.
        cbg_mock = mocker.Mock(spec=CallbackGroup, callbacks=[])
        with pytest.raises(ItemNotFoundError, match='callback_groups has no callback.'):
            CallbackSchedulingPlot._get_callback_groups(cbg_mock)

    def test_get_callback_groups_list_with_empty_callback_group(self, mocker):
        # List of CallbackGroup objects, where one or more have no callbacks.
        cbg_mock_with_empty_callbacks_1 = mocker.Mock(spec=CallbackGroup, callbacks=[])
        cbg_mock_with_empty_callbacks_2 = mocker.Mock(spec=CallbackGroup, callbacks=[])

        with pytest.raises(ItemNotFoundError, match='callback_groups has no callback.'):
            CallbackSchedulingPlot._get_callback_groups([cbg_mock_with_empty_callbacks_1,
                                                         cbg_mock_with_empty_callbacks_2])

    def test_get_callback_groups_path_none(self, mocker):
        # Path where nodes have None callback_groups.
        pub_node_mock = mocker.Mock(spec=Node, callback_groups=None)
        sub_node_mock = mocker.Mock(spec=Node, callback_groups=None)
        comm_mock = mocker.Mock(spec=Communication,
                                publish_node=pub_node_mock, subscribe_node=sub_node_mock)
        path_mock = mocker.Mock(spec=Path, communications=[comm_mock])
        with pytest.raises(ItemNotFoundError, match='callback_groups is empty.'):
            CallbackSchedulingPlot._get_callback_groups(path_mock)

    def test_get_callback_groups_path_no_actual_callbacks(self, mocker):
        # Path where CallbackGroups are found, but none of them contain actual Callbacks.
        cbg_empty_callbacks_pub_0 = mocker.Mock(spec=CallbackGroup, callbacks=[])
        cbg_empty_callbacks_sub_0 = mocker.Mock(spec=CallbackGroup, callbacks=[])
        cbg_empty_callbacks_pub_1 = mocker.Mock(spec=CallbackGroup, callbacks=[])
        cbg_empty_callbacks_sub_1 = mocker.Mock(spec=CallbackGroup, callbacks=[])

        pub_node_mock_0 = mocker.Mock(spec=Node, callback_groups=[cbg_empty_callbacks_pub_0])
        sub_node_mock_0 = mocker.Mock(spec=Node, callback_groups=[cbg_empty_callbacks_sub_0])
        comm_mock_0 = mocker.Mock(spec=Communication,
                                  publish_node=pub_node_mock_0, subscribe_node=sub_node_mock_0)

        pub_node_mock_1 = mocker.Mock(spec=Node, callback_groups=[cbg_empty_callbacks_pub_1])
        sub_node_mock_1 = mocker.Mock(spec=Node, callback_groups=[cbg_empty_callbacks_sub_1])
        comm_mock_1 = mocker.Mock(spec=Communication,
                                  publish_node=pub_node_mock_1, subscribe_node=sub_node_mock_1)

        path_mock = mocker.Mock(spec=Path, communications=[comm_mock_0, comm_mock_1])

        with pytest.raises(ItemNotFoundError, match='callback_groups has no callback.'):
            CallbackSchedulingPlot._get_callback_groups(path_mock)

    def test_get_callback_groups_path_empty_communications(self, mocker):
        # Test case for a Path with an empty list of communications.
        path_mock = mocker.Mock(spec=Path, communications=[])
        with pytest.raises(ItemNotFoundError, match='callback_groups is empty.'):
            CallbackSchedulingPlot._get_callback_groups(path_mock)
