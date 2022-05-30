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

from __future__ import annotations

from abc import ABCMeta, abstractmethod

from typing import Dict, Optional

from .trace_points import TRACE_POINT as TP

Event = Dict[str, int]


class LttngEventFilter(metaclass=ABCMeta):
    NAME = '_name'
    TIMESTAMP = '_timestamp'
    CPU_ID = '_cpuid'
    VPID = 'vpid'
    VTID = 'vtid'

    class Common:
        start_time: int
        end_time: int

    @staticmethod
    def duration_filter(duration_s: float, offset_s: float) -> LttngEventFilter:
        return EventDurationFilter(duration_s, offset_s)

    @staticmethod
    def strip_filter(lsplit_s: Optional[float], rsplit_s: Optional[float]) -> LttngEventFilter:
        return EventStripFilter(lsplit_s, rsplit_s)

    @staticmethod
    def init_pass_filter() -> LttngEventFilter:
        return InitEventPassFilter()

    @abstractmethod
    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        pass


class InitEventPassFilter(LttngEventFilter):

    def accept(self, event: Event, common: LttngEventFilter.Common) -> bool:
        init_events = {
            TP.ADD_CALLBACK_GROUP,
            TP.ADD_CALLBACK_GROUP_STATIC_EXECUTOR,
            TP.CALLBACK_GROUP_ADD_CLIENT,
            TP.CALLBACK_GROUP_ADD_SERVICE,
            TP.CALLBACK_GROUP_ADD_SUBSCRIPTION,
            TP.CALLBACK_GROUP_ADD_TIMER,
            TP.CONSTRUCT_EXECUTOR,
            TP.CONSTRUCT_IPM,
            TP.CONSTRUCT_NODE_HOOK,
            TP.CONSTRUCT_RING_BUFFER,
            TP.CONSTRUCT_STATIC_EXECUTOR,
            TP.CONSTRUCT_TF_BUFFER,
            TP.INIT_BIND_TF_BROADCASTER_SEND_TRANSFORM,
            TP.INIT_BIND_TF_BUFFER_CORE,
            TP.INIT_BIND_TRANSFORM_BROADCASTER,
            TP.INIT_TF_BROADCASTER_FRAME_ID_COMPACT,
            TP.INIT_TF_BUFFER_FRAME_ID_COMPACT,
            TP.INIT_TF_BUFFER_LOOKUP_TRANSFORM,
            TP.INIT_TF_BUFFER_SET_TRANSFORM,
            TP.IPM_ADD_PUBLISHER,
            TP.IPM_ADD_SUBSCRIPTION,
            TP.IPM_INSERT_SUB_ID_FOR_PUB,
            TP.RCLCPP_CALLBACK_REGISTER,
            TP.RCLCPP_PUBLISHER_INIT,
            TP.RCLCPP_SERVICE_CALLBACK_ADDED,
            TP.RCLCPP_SUBSCRIPTION_CALLBACK_ADDED,
            TP.RCLCPP_SUBSCRIPTION_INIT,
            TP.RCLCPP_TIMER_CALLBACK_ADDED,
            TP.RCLCPP_TIMER_LINK_NODE,
            TP.RCL_CLIENT_INIT,
            TP.RCL_INIT,
            TP.RCL_INIT_CARET,
            TP.RCL_LIFECYCLE_STATE_MACHINE_INIT,
            TP.RCL_NODE_INIT,
            TP.RCL_PUBLISHER_INIT,
            TP.RCL_SERVICE_INIT,
            TP.RCL_SUBSCRIPTION_INIT,
            TP.RCL_TIMER_INIT,
            TP.RMW_IMPLEMENTATION,
            TP.SYMBOL_RENAME,
            TP.TILDE_PUBLISHER_INIT,
            TP.TILDE_SUBSCRIBE_ADDED,
            TP.TILDE_SUBSCRIPTION_INIT,
        }

        return event[self.NAME] in init_events


class EventStripFilter(LttngEventFilter):
    def __init__(
        self,
        lstrip_s: Optional[float],
        rstrip_s: Optional[float]
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
