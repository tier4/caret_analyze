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
        event[4] = {'_name': 'ros2_caret:rcl_publisher_init',
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
        event[5] = {'_name': 'ros2_caret:rcl_publisher_init',
                    'publisher_handle': 94360959746544,
                    'node_handle': 94360959171874,
                    'rmw_publisher_handle': 94360959675074,
                    'topic_name': '/tf_static',
                    'queue_depth': 1,
                    '_timestamp': 1686891736321876644,
                    '_vtid': 106554,
                    '_vpid': 106254,
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

        # exact match data(0,4)
        handle = 94360959746544
        new_handle = id_remap.register_and_get_object_id(handle, event[4])
        assert handle == new_handle

        # publisher_handle(same handle)
        handle = 1
        new_handle = id_remap.register_and_get_object_id(handle, event[2])
        assert 3 == new_handle

        # Increase self._next_object_id until it is no longer included in self._all_object_ids.
        id_remap._all_object_ids.add(4)
        new_handle = id_remap.register_and_get_object_id(handle, event[5])
        assert 5 == new_handle

    def test_get_object_id(self):
        CALLBACK_ID1 = 10000
        CALLBACK_ID2 = 10100
        T_ID1 = 108253
        T_ID2 = 108252
        P_ID1 = 1001
        P_ID2 = 1002

        event = {}
        event[0] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 4980,
                    '_vtid': T_ID1,
                    '_vpid': P_ID1,
                    }
        event[1] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 4990,
                    '_vtid': T_ID2,
                    '_vpid': P_ID1,
                    }
        event[2] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5000,
                    '_vtid': T_ID2,
                    '_vpid': P_ID2,
                    }
        # _timestamp is the same value(enters addr_to_remapping_info_ because tid is different)
        event[3] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5000,
                    '_vtid': T_ID1,
                    '_vpid': P_ID1,
                    }
        # Patterns with different callbacks
        event[4] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID2,
                    '_timestamp': 5010,
                    '_vtid': T_ID2,
                    '_vpid': P_ID1,
                    }
        # Patterns with the same callback but different pid
        event[5] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5020,
                    '_vtid': T_ID1,
                    '_vpid': P_ID2,
                    }
        event[6] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5030,
                    '_vtid': T_ID1,
                    '_vpid': P_ID1,
                    }
        event[7] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5030,
                    '_vtid': T_ID1,
                    '_vpid': P_ID2,
                    }
        event[8] = {'_name': 'ros2_caret:rclcpp_callback_register',
                    'callback': CALLBACK_ID1,
                    '_timestamp': 5040,
                    '_vtid': T_ID1,
                    '_vpid': P_ID1,
                    }

        id_remap = IDRemapper()
        # callback
        callback = CALLBACK_ID1
        id_remap.register_and_get_object_id(callback, event[0])  # minimum timestamp
        id_remap.register_and_get_object_id(callback, event[1])  # tid2
        id_remap.register_and_get_object_id(callback, event[2])  # tid2  pid2 timestamp5000
        id_remap.register_and_get_object_id(callback, event[3])  # timestamp5000
        # different callbacks
        callback = CALLBACK_ID2
        id_remap.register_and_get_object_id(callback, event[4])  # tid2
        callback = CALLBACK_ID1
        id_remap.register_and_get_object_id(callback, event[5])  # pid2
        id_remap.register_and_get_object_id(callback, event[6])  # timestamp5030
        id_remap.register_and_get_object_id(callback, event[7])  # pid2 timestamp5030
        id_remap.register_and_get_object_id(callback, event[8])  # maximum timestamp

        # registered addr_to_remapping_info_ : {1, 2, 3, 4, 5, 6, 7, 10000, 10100}

        event_test = {}
        event_test[0] = {'_name': 'ros2:callback_start',
                         'callback': CALLBACK_ID1,
                         '_timestamp': 4980,
                         '_vtid': T_ID1,
                         '_vpid': P_ID1,
                         }
        event_test[1] = {'_name': 'ros2:callback_start',
                         'callback': CALLBACK_ID1,
                         '_timestamp': 4990,
                         '_vtid': T_ID2,
                         '_vpid': P_ID1,
                         }
        event_test[3] = {'_name': 'ros2:callback_start',
                         'callback': CALLBACK_ID1,
                         '_timestamp': 5000,
                         '_vtid': T_ID1,
                         '_vpid': P_ID1,
                         }
        event_test[4] = {'_name': 'ros2:callback_start',
                         'callback': CALLBACK_ID2,
                         '_timestamp': 5010,
                         '_vtid': T_ID2,
                         '_vpid': P_ID1,
                         }
        event_test[8] = {'_name': 'ros2:callback_start',
                         'callback': CALLBACK_ID1,
                         '_timestamp': 5040,
                         '_vtid': T_ID1,
                         '_vpid': P_ID1,
                         }
        event_test[11] = {'_name': 'ros2:callback_start',
                          'callback': CALLBACK_ID1,
                          '_timestamp': 4970,
                          '_vtid': T_ID1,
                          '_vpid': P_ID1,
                          }
        event_test[12] = {'_name': 'ros2:callback_start',
                          'callback': CALLBACK_ID1,
                          '_timestamp': 5050,
                          '_vtid': T_ID1,
                          '_vpid': P_ID1,
                          }
        event_test[13] = {'_name': 'ros2:callback_start',
                          'callback': CALLBACK_ID1,
                          '_timestamp': 4985,
                          '_vtid': T_ID1,
                          '_vpid': P_ID1,
                          }
        event_test[14] = {'_name': 'ros2:callback_start',
                          'callback': CALLBACK_ID1,
                          '_timestamp': 5035,
                          '_vtid': T_ID1,
                          '_vpid': P_ID1,
                          }

        # If key addr does not exist in _self.addr_to_remapping_info_
        callback = CALLBACK_ID2
        ret_value = id_remap.get_object_id(callback, event_test[4])
        assert ret_value == CALLBACK_ID2

        # same pid & maximum timestamp

        # same pid = (0 1 3 6 8) & event timestamp(maximum:5040)
        #   -> Returns the remapped_id of the element with the largest timestamp.
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[8])
        assert ret_value == 7
        # same pid = (0 1 3) & event timestamp(middle:5000)
        #   -> event['_timestamp'] Exclude elements greater than or equal to
        #   -> Returns the remapped_id of the element with the largest timestamp.
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[3])
        assert ret_value == 3

        # same pid = (0 1) & event timestamp(small:4990)
        #   -> list_search=1
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[1])
        assert ret_value == 1

        # same pid = (0) & event timestamp(minimum:4980)
        #   -> list_search=0
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[0])
        assert ret_value == CALLBACK_ID1

        # minimum value not registered in addr_to_remapping_info_
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[11])
        assert ret_value == CALLBACK_ID1

        # maximum value not registered in addr_to_remapping_info_
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[12])
        assert ret_value == 7

        # value between the registered values in addr_to_remapping_info_
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[13])
        assert ret_value == CALLBACK_ID1

        # value between the registered values in addr_to_remapping_info_
        callback = CALLBACK_ID1
        ret_value = id_remap.get_object_id(callback, event_test[14])
        assert ret_value == 5
