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

from caret_analyze.infra.lttng.id_remapper import IDRemapper


class TestIdRemapper:

    def test_register_and_get_object_id(self):
        event = {}
        event[0] = {'_name': 'ros2_caret:rcl_publisher_init',
                    'publisher_handle': 94360959746544,
                    'node_handle': 94360959171870,
                    'rmw_publisher_handle': 94360959675070,
                    'topic_name': '/tf_static',
                    'queue_depth': 1,
                    '_timestamp': 1686891736321876640,
                    '_vtid': 106550,
                    '_vpid': 106250,
                    '_procname': 'robot_state_pub'
                    }
        event[1] = {'_name': 'ros2_caret:rcl_publisher_init',
                    'publisher_handle': 94360959675072,
                    'node_handle': 94360959171871,
                    'rmw_publisher_handle': 94360959675071,
                    'topic_name': '/tf_static',
                    'queue_depth': 1,
                    '_timestamp': 1686891736321876641,
                    '_vtid': 106551,
                    '_vpid': 106251,
                    '_procname': 'robot_state_pub'
                    }
        event[2] = {'_name': 'ros2_caret:rcl_publisher_init',
                    'publisher_handle': 94360959746544,
                    'node_handle': 94360959171872,
                    'rmw_publisher_handle': 94360959675072,
                    'topic_name': '/tf_static',
                    'queue_depth': 1,
                    '_timestamp': 1686891736321876642,
                    '_vtid': 106552,
                    '_vpid': 106252,
                    '_procname': 'robot_state_pub'
                    }
        event[3] = {'_name': 'ros2_caret:rcl_publisher_init',
                    'publisher_handle': 94360959746544,
                    'node_handle': 94360959171873,
                    'rmw_publisher_handle': 94360959675073,
                    'topic_name': '/tf_static',
                    'queue_depth': 1,
                    '_timestamp': 1686891736321876643,
                    '_vtid': 106553,
                    '_vpid': 106253,
                    '_procname': 'robot_state_pub'
                    }

        id_remap = IDRemapper()
        # publisher_handle
        handle = 94360959746544
        new_handle = id_remap.register_and_get_object_id(handle, event[0])
        assert handle == new_handle

        # rmw_publisher_handle
        handle = 94360959675072
        new_handle = id_remap.register_and_get_object_id(handle, event[1])
        assert handle == new_handle

        # publisher_handle(same handle)
        handle = 94360959746544
        new_handle = id_remap.register_and_get_object_id(handle, event[2])
        assert 1 == new_handle

        # publisher_handle(same handle + 2)
        handle = 94360959746544
        new_handle = id_remap.register_and_get_object_id(handle, event[3])
        assert 2 == new_handle

        # exact match data
        handle = 94360959746544
        new_handle = id_remap.register_and_get_object_id(handle, event[0])
        assert handle == new_handle

        # publisher_handle(same remaped handle)
        handle = 1
        new_handle = id_remap.register_and_get_object_id(handle, event[2])
        assert 3 == new_handle

    def test_get_object_id(self):
        event = {}
        event[0] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 500,
                    '_vtid': 108250,
                    '_vpid': 1001,
                    }
        # callbackが違うパターン
        event[1] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10100,
                    '_timestamp': 501,
                    '_vtid': 108280,
                    '_vpid': 1001,
                    }
        # _timestampが小さい
        event[2] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 499,
                    '_vtid': 108252,
                    '_vpid': 1001,
                    }
        # callbackは一緒だがpidが異なるパターン
        event[3] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 502,
                    '_vtid': 108253,
                    '_vpid': 1002,
                    }
        # _timestampが同じ値 (tidが異なるのでaddr_to_remapping_info_に入る)
        event[4] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 500,
                    '_vtid': 108253,
                    '_vpid': 1001,
                    }
        # _timestampが大きい
        event[5] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 504,
                    '_vtid': 108253,
                    '_vpid': 1001,
                    }
        event[6] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 503,
                    '_vtid': 108253,
                    '_vpid': 1001,
                    }
        # _timestampが最も小さい
        event[7] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 498,
                    '_vtid': 108253,
                    '_vpid': 1001,
                    }
        event[8] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': 10000,
                    '_timestamp': 503,
                    '_vtid': 108253,
                    '_vpid': 1005,
                    }

        id_remap = IDRemapper()
        # callback
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[0])

        # callback
        callback = 10100
        id_remap.register_and_get_object_id(callback, event[1])

        # callback(same callback)
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[2])

        # callback(same callback + 2) & != pid
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[3])

        # callback(same callback + 2) same timestamp
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[4])

        # callback(same callback + 2)
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[5])

        # callback(same callback + 2)
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[6])

        # callback(same callback + 2)
        callback = 10000
        id_remap.register_and_get_object_id(callback, event[7])

        # この段階でself.all_object_ids_ = {1, 2, 3, 4, 5, 6, 10000, 10100}
        # callback 10000
        # callback 10000 pid:1001
        # callback 10000 pid:1001 _timestamp:500
        # 期待する結果は (499 502 500 504 503 498) ->
        #   pid,timestamp検索結果(499 500 504 503 498) -> 504(remapped_id=4)
        callback = 10000
        ret_value = id_remap.get_object_id(callback, event[5])
        assert ret_value == 4

        # self.addr_to_remapping_info_[addr] より、pid が event['_pid'] に一致しない場合
        callback = 10000
        ret_value = id_remap.get_object_id(callback, event[8])
        assert ret_value == 10000

        # self.addr_to_remapping_info_ にキー addr が存在しない場合
        callback = 10100
        ret_value = id_remap.get_object_id(callback, event[1])
        assert ret_value == 10100
