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
from caret_analyze.value_objects import (NodeStructValue, PublisherStructValue,
                                         ServiceStructValue, SubscriptionStructValue,
                                         TimerStructValue)
import pytest


class TestNodeStructValue:

    def test_node_struct_value(self, mocker):
        pub_info_mock = mocker.Mock(spec=PublisherStructValue)
        mocker.patch.object(pub_info_mock, 'topic_name', 'pub_name')
        mocker.patch.object(pub_info_mock, 'construction_order', 1)

        sub_info_mock = mocker.Mock(spec=SubscriptionStructValue)
        mocker.patch.object(sub_info_mock, 'topic_name', 'sub_name')
        mocker.patch.object(sub_info_mock, 'construction_order', 1)

        service_info_mock = mocker.Mock(spec=ServiceStructValue)
        mocker.patch.object(service_info_mock, 'service_name', 'service_name')
        mocker.patch.object(service_info_mock, 'construction_order', 1)

        timer_info_mock = mocker.Mock(spec=TimerStructValue)
        mocker.patch.object(timer_info_mock, 'node_name', 'node_name')
        mocker.patch.object(timer_info_mock, 'construction_order', 1)

        node_info = NodeStructValue('node_name', [pub_info_mock],
                                    [sub_info_mock], [service_info_mock], [timer_info_mock],
                                    None, None, None)
        node = node_info.get_publisher('pub_name', None)
        assert node == pub_info_mock
        node = node_info.get_publisher('pub_name', 1)
        assert node == pub_info_mock
        with pytest.raises(ItemNotFoundError):
            node_info.get_publisher('pub_name', 0)

        node = node_info.get_subscription('sub_name', None)
        assert node == sub_info_mock
        node = node_info.get_subscription('sub_name', 1)
        assert node == sub_info_mock
        with pytest.raises(ItemNotFoundError):
            node_info.get_subscription('sub_name', 0)

        node = node_info.get_service('service_name', None)
        assert node == service_info_mock
        node = node_info.get_service('service_name', 1)
        assert node == service_info_mock
        with pytest.raises(ItemNotFoundError):
            node_info.get_service('service_name', 0)

        node = node_info.get_timer('node_name', None)
        assert node == timer_info_mock
        node = node_info.get_timer('node_name', 1)
        assert node == timer_info_mock
        with pytest.raises(ItemNotFoundError):
            node_info.get_timer('node_name', 0)
