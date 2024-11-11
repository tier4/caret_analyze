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

from __future__ import annotations

from abc import ABCMeta, abstractmethod


Event = dict[str, int]


class LttngEventFilter(metaclass=ABCMeta):
    NAME = '_name'
    TIMESTAMP = '_timestamp'
    CPU_ID = '_cpuid'
    VPID = '_vpid'
    VTID = '_vtid'
    PROCNAME = '_procname'

    class Common:
        start_time: int
        end_time: int

    @staticmethod
    def duration_filter(duration_s: float, offset_s: float) -> LttngEventFilter:
        return EventDurationFilter(duration_s, offset_s)

    @staticmethod
    def strip_filter(lsplit_s: float | None, rsplit_s: float | None) -> LttngEventFilter:
        return EventStripFilter(lsplit_s, rsplit_s)

    @staticmethod
    def init_pass_filter() -> LttngEventFilter:
        return InitEventPassFilter()

    @staticmethod
    def same_address_filter(max_count: int) -> LttngEventFilter:
        return SameAddressFilter(max_count)

    @abstractmethod
    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        pass

    def reset(self) -> None:
        pass


class SameAddressFilter(LttngEventFilter):

    def __init__(self, max_count: int) -> None:
        self._max_count = max_count
        self._list_construct_executor: dict = {}
        self._list_callback_group: dict = {}

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        event_name = event[self.NAME]
        if event_name == 'ros2_caret:construct_executor':
            event_addr = event['executor_addr']
            self._list_construct_executor[event_addr] = \
                self._list_construct_executor.get(event_addr, 0) + 1
            if self._list_construct_executor[event_addr] > self._max_count:
                return False
            return True
        if event_name == 'ros2_caret:add_callback_group':
            event_addr = event['callback_group_addr']
            self._list_callback_group[event_addr] = \
                self._list_callback_group.get(event_addr, 0) + 1
            if self._list_callback_group[event_addr] > self._max_count:
                return False
            return True
        return True

    def reset(self) -> None:
        super().reset()
        self._list_construct_executor.clear()
        self._list_callback_group.clear()


class InitEventPassFilter(LttngEventFilter):

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        # TODO(hsgwa): Definitions on tracepoint types are scattered. Refactor required.
        init_events = {
            'ros2:rcl_init',
            'ros2_caret:rcl_init',
            'ros2:rcl_node_init',
            'ros2_caret:rcl_node_init',
            'ros2:rcl_publisher_init',
            'ros2_caret:rcl_publisher_init',
            'ros2:rcl_subscription_init',
            'ros2_caret:rcl_subscription_init',
            'ros2:rclcpp_subscription_init',
            'ros2_caret:rclcpp_subscription_init',
            'ros2:rclcpp_subscription_callback_added',
            'ros2_caret:rclcpp_subscription_callback_added',
            'ros2:rcl_service_init',
            'ros2_caret:rcl_service_init',
            'ros2:rclcpp_service_callback_added',
            'ros2_caret:rclcpp_service_callback_added',
            'ros2:rcl_client_init',
            'ros2_caret:rcl_client_init',
            'ros2:rcl_timer_init',
            'ros2_caret:rcl_timer_init',
            'ros2:rclcpp_timer_callback_added',
            'ros2_caret:rclcpp_timer_callback_added',
            'ros2:rclcpp_timer_link_node',
            'ros2_caret:rclcpp_timer_link_node',
            'ros2:rclcpp_callback_register',
            'ros2_caret:rclcpp_callback_register',
            'ros2:rcl_lifecycle_state_machine_init',
            'ros2_caret:rcl_lifecycle_state_machine_init',
            'ros2:rcl_lifecycle_transition',
            'ros2_caret:caret_init',
            'ros2_caret:rmw_implementation',
            'ros2_caret:add_callback_group',
            'ros2_caret:add_callback_group_static_executor',
            'ros2_caret:callback_group_to_executor_entity_collector',
            'ros2_caret:executor_entity_collector_to_executor',
            'ros2_caret:construct_executor',
            'ros2_caret:construct_static_executor',
            'ros2_caret:callback_group_add_timer',
            'ros2_caret:callback_group_add_subscription',
            'ros2_caret:callback_group_add_service',
            'ros2_caret:callback_group_add_client',
            'ros2_caret:tilde_subscription_init',
            'ros2_caret:tilde_publisher_init',
            'ros2_caret:tilde_subscribe_added',
        }

        return event[self.NAME] in init_events


class EventStripFilter(LttngEventFilter):
    def __init__(
        self,
        lstrip_s: float | None,
        rstrip_s: float | None
    ) -> None:
        self._lstrip = lstrip_s
        self._rstrip = rstrip_s
        self._init_events = InitEventPassFilter()

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        if self._init_events.accept(event, common):
            return True

        if self._lstrip:
            diff_ns = event[self.TIMESTAMP] - common.start_time
            diff_s = diff_ns * 1.0e-9
            if diff_s < self._lstrip:
                return False

        if self._rstrip:
            diff_ns = common.end_time - event[self.TIMESTAMP]
            diff_s = diff_ns * 1.0e-9
            if diff_s < self._rstrip:
                return False
        return True


class EventDurationFilter(LttngEventFilter):

    def __init__(self, duration_s: float, offset_s: float) -> None:
        self._duration = duration_s
        self._offset = offset_s
        self._init_events = InitEventPassFilter()

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        if self._init_events.accept(event, common):
            return True

        elapsed_ns = event[self.TIMESTAMP] - common.start_time
        elapsed_s = elapsed_ns * 1.0e-9
        return self._offset <= elapsed_s and elapsed_s < (self._offset + self._duration)
