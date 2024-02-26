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

from collections import defaultdict
from bisect import bisect_right, insort_right
from operator import attrgetter

from .lttng_event_filter import LttngEventFilter
from .ros2_tracing.processor import get_field


class IDRemappingInfo():
    def __init__(
            self,
            timestamp: int,
            pid: int,
            remapped_id: int
    ) -> None:
        self._timestamp = timestamp
        self._pid = pid
        self._remapped_id = remapped_id

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def pid(self) -> int:
        return self._pid

    @property
    def remapped_id(self) -> int:
        return self._remapped_id


class IDRemapper():

    def __init__(self) -> None:
        self._addr_to_init_event: dict = defaultdict(list)
        self._addr_to_remapping_info: dict = {}
        self._all_object_ids: set = set()
        self._next_object_id = 1

    def register_and_get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        # register initialization trace event
        if addr not in self._all_object_ids:
            self._addr_to_init_event[addr].append(event)
            self._all_object_ids.add(addr)
            return addr
        else:
            # same address already in use
            for val_event in self._addr_to_init_event[addr]:
                if val_event == event:
                    # events with exact contents
                    return addr
            # the address is the same,
            # but the contents do not match, so it needs to be replaced.
            # if len(self._addr_to_init_event[addr]) < 10:
            #     self._addr_to_init_event[addr].append(event)
            self._addr_to_init_event[addr].append(event)
            while self._next_object_id in self._all_object_ids:
                self._next_object_id += 1
            pid = get_field(event, LttngEventFilter.VPID)
            remap_info = IDRemappingInfo(get_field(event, LttngEventFilter.TIMESTAMP),
                                         pid,
                                         self._next_object_id)
            remapping_info_list = self._addr_to_remapping_info.setdefault(addr, {}).setdefault(pid, [])
            insort_right(remapping_info_list, remap_info, key=lambda info: info.timestamp)
            self._all_object_ids.add(self._next_object_id)
            self._next_object_id += 1
            return self._next_object_id - 1

    def get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        if addr in self._addr_to_remapping_info:
            pid = get_field(event, LttngEventFilter.VPID)
            if pid in self._addr_to_remapping_info[addr]:
                remapping_info_list = self._addr_to_remapping_info[addr][pid]
                if len(remapping_info_list) > 0:
                    index = bisect_right(remapping_info_list, get_field(event, LttngEventFilter.TIMESTAMP), key=attrgetter('timestamp'))
                    if index == 0:
                        return addr
                    else:
                        return remapping_info_list[index - 1].remapped_id
            return addr
        else:
            return addr
