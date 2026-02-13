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

# cspell: ignore ciid

from caret_analyze.infra.lttng.ros2_tracing.processor import Ros2Handler

import pytest


class TestProcessor:

    @pytest.mark.parametrize(
        'id_field_name, expected_id_value',
        [
            ('pid_callback_info_id', 111),
            ('pid_ciid', 222),
        ],
    )
    def test_handle_agnocast_subscription_init_id_variations(
        self, mocker, id_field_name, expected_id_value
    ):
        data_mock = mocker.Mock()
        remapper_mock = mocker.Mock()
        handler = Ros2Handler(data_mock, remapper_mock, None)

        remappers_dict = {
            "subscription_handle": remapper_mock.subscription_handle_remapper
            .register_and_get_object_id,
            "node_handle": remapper_mock.node_handle_remapper
            .get_nearest_object_id,
            "callback": remapper_mock.callback_remapper
            .register_and_get_object_id,
            "callback_group_addr": remapper_mock
            .callback_group_addr_remapper.get_nearest_object_id,
        }

        def identify_function(addr: int, event: dict) -> int:
            return addr

        for mock in remappers_dict.values():
            mock.side_effect = identify_function

        event = {
            'subscription_handle': 0x1,
            '_timestamp': 1000,
            'node_handle': 0x2,
            'callback': 0x3,
            'callback_group': 0x4,
            'symbol': 'test_symbol',
            'queue_depth': 10,
            'topic_name': 'test_topic',
            id_field_name: expected_id_value
        }

        handler._handle_agnocast_subscription_init(event)

        data_mock.add_agnocast_subscription.assert_called_once_with(
            0x1,
            1000,
            0x2,
            0x3,
            0x4,
            'test_symbol',
            'test_topic_agnocast',
            10,
            expected_id_value
        )

    @pytest.mark.parametrize(
        'id_field_name, expected_id_value',
        [
            ('pid_callback_info_id', 333),
            ('pid_ciid', 444),
        ],
    )
    def test_handle_agnocast_create_callable_id_variations(
        self, mocker, id_field_name, expected_id_value
    ):
        data_mock = mocker.Mock()
        remapper_mock = mocker.Mock()
        handler = Ros2Handler(data_mock, remapper_mock, None)

        def identify_function(addr: int, event: dict) -> int:
            return addr

        remapper_mock.callable_remapper.register_and_get_object_id.side_effect = (
            identify_function
        )

        event = {
            '_timestamp': 2000,
            'callable': 0x5,
            'entry_id': 123,
            id_field_name: expected_id_value
        }

        handler._handle_agnocast_create_callable(event)

        data_mock.add_agnocast_create_callable_instance.assert_called_once_with(
            2000,
            0x5,
            123,
            expected_id_value
        )