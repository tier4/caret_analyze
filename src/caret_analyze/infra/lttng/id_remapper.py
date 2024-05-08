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

from collections import defaultdict

from .lttng_event_filter import LttngEventFilter
from .ros2_tracing.processor import get_field


class IDRemappingInfo():
    def __init__(
            self,
            timestamp: int,
            pid: int,
            remapped_id: int,
            event: dict
    ) -> None:
        self._timestamp = timestamp
        self._pid = pid
        self._remapped_id = remapped_id
        self._event = event

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def remapped_id(self) -> int:
        return self._remapped_id

    @property
    def event(self) -> dict:
        return self._event


class IDRemapper():

    def __init__(self) -> None:
        self._addr_to_remapping_info: dict = defaultdict(list)
        self._all_object_ids: set = set()
        self._next_object_id = 1

    def register_and_get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        # register initialization trace event
        if addr not in self._all_object_ids:
            self._all_object_ids.add(addr)
            remap_info = IDRemappingInfo(get_field(event, LttngEventFilter.TIMESTAMP),
                                         get_field(event, LttngEventFilter.VPID),
                                         addr,
                                         event)
            self._addr_to_remapping_info[addr].append(remap_info)
            return addr
        else:
            # same address already in use
            for info in self._addr_to_remapping_info[addr]:
                if info.event == event:
                    # events with exact contents
                    return info.remapped_id
            # the address is the same,
            # but the contents do not match, so it needs to be replaced.
            while self._next_object_id in self._all_object_ids:
                self._next_object_id += 1
            remap_info = IDRemappingInfo(get_field(event, LttngEventFilter.TIMESTAMP),
                                         get_field(event, LttngEventFilter.VPID),
                                         self._next_object_id,
                                         event)
            self._addr_to_remapping_info[addr].append(remap_info)
            self._all_object_ids.add(self._next_object_id)
            self._next_object_id += 1
            return self._next_object_id - 1

    def get_latest_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        if addr in self._addr_to_remapping_info:
            pid = get_field(event, LttngEventFilter.VPID)
            timestamp = get_field(event, LttngEventFilter.TIMESTAMP)
            list_search = \
                [item for item in self._addr_to_remapping_info[addr]
                 if item.pid == pid and item.timestamp <= timestamp]
            if len(list_search) == 0:
                return addr
            elif len(list_search) == 1:
                return list_search[0].remapped_id

            max_item = max(list_search, key=lambda item: item.timestamp)
            return max_item.remapped_id
        else:
            return addr

    def get_nearest_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        if addr in self._addr_to_remapping_info:
            pid = get_field(event, LttngEventFilter.VPID)
            timestamp = get_field(event, LttngEventFilter.TIMESTAMP)
            list_search = \
                [item for item in self._addr_to_remapping_info[addr]
                 if item.pid == pid]
            if len(list_search) == 0:
                return addr
            else:
                min_distance = abs(list_search[0].timestamp - timestamp)
                min_distance_item = list_search[0]
                for item in list_search:
                    distance = abs(item.timestamp - timestamp)
                    if min_distance > distance:
                        min_distance = distance
                        min_distance_item = item
                return min_distance_item.remapped_id
        else:
            return addr


class IDRemapperCollection:

    def __init__(self):
        self._context_handle_remapper = IDRemapper()
        self._node_handle_remapper = IDRemapper()
        self._rmw_node_handle_remapper = IDRemapper()
        self._publisher_handle_remapper = IDRemapper()
        self._rmw_publisher_handle_remapper = IDRemapper()
        self._subscription_handle_remapper = IDRemapper()
        self._rmw_subscription_handle_remapper = IDRemapper()
        self._subscription_remapper = IDRemapper()
        self._service_handle_remapper = IDRemapper()
        self._rmw_service_handle_remapper = IDRemapper()
        self._client_handle_remapper = IDRemapper()
        self._rmw_client_handle_remapper = IDRemapper()
        self._callback_holder_id_remapper = IDRemapper()
        self._state_machine_remapper = IDRemapper()
        self._executor_addr_remapper = IDRemapper()
        self._entities_collector_addr_remapper = IDRemapper()
        self._callback_group_addr_remapper = IDRemapper()
        self._callback_remapper = IDRemapper()
        self._buffer_remapper = IDRemapper()
        self._ipb_remapper = IDRemapper()

    @property
    def context_handle_remapper(self):
        return self._context_handle_remapper

    @property
    def node_handle_remapper(self):
        return self._node_handle_remapper

    @property
    def rmw_node_handle_remapper(self):
        return self._rmw_node_handle_remapper

    @property
    def publisher_handle_remapper(self):
        return self._publisher_handle_remapper

    @property
    def rmw_publisher_handle_remapper(self):
        return self._rmw_publisher_handle_remapper

    @property
    def subscription_handle_remapper(self):
        return self._subscription_handle_remapper

    @property
    def rmw_subscription_handle_remapper(self):
        return self._rmw_subscription_handle_remapper

    @property
    def subscription_remapper(self):
        return self._subscription_remapper

    @property
    def service_handle_remapper(self):
        return self._service_handle_remapper

    @property
    def rmw_service_handle_remapper(self):
        return self._rmw_service_handle_remapper

    @property
    def client_handle_remapper(self):
        return self._client_handle_remapper

    @property
    def rmw_client_handle_remapper(self):
        return self._rmw_client_handle_remapper

    @property
    def callback_holder_id_remapper(self):
        return self._callback_holder_id_remapper

    @property
    def state_machine_remapper(self):
        return self._state_machine_remapper

    @property
    def executor_addr_remapper(self):
        return self._executor_addr_remapper

    @property
    def entities_collector_addr_remapper(self):
        return self._entities_collector_addr_remapper

    @property
    def callback_group_addr_remapper(self):
        return self._callback_group_addr_remapper

    @property
    def callback_remapper(self):
        return self._callback_remapper

    @property
    def buffer_remapper(self):
        return self._buffer_remapper

    @property
    def ipb_remapper(self):
        return self._ipb_remapper
