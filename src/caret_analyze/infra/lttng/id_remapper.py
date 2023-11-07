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
        # ignore_topics: list[str] = IGNORE_TOPICS

        # reader = ArchitectureReaderFactory.create_instance(
        #     file_type, file_path)
        # loaded = ArchitectureLoaded(reader, ignore_topics)
        self._addr_to_init_event: dict = defaultdict(list)
        self._addr_to_remapping_info: dict = {}
        self._all_object_ids: set = set()
        self._next_object_id = 1

    def register_and_get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        # MYK test
        # self._all_object_ids.add(1)
        # self._all_object_ids.add(2)
        # self._all_object_ids.add(3)
        # while self._next_object_id in self._all_object_ids:
        #     self._next_object_id += 1
        # remap_info = IDRemappingInfo(
        #     get_field(event, '_timestamp'), get_field(event, '_vpid'), self._next_object_id)
        # self._addr_to_remapping_info[addr] = remap_info
        # self._addr_to_remapping_info[addr+1] = remap_info

        # register initialization trace event
        if addr not in self._all_object_ids:
            self._addr_to_init_event[addr].append(event)
            self._all_object_ids.add(addr)
            return addr
        else:
            # 同じアドレスがすでに使われている
            for val_event in self._addr_to_init_event[addr]:
                if val_event == event:
                    # 内容が完全に一致するイベント（記録時の都合で重複出力されたイベント）は
                    # 一つのみを登録するので、これは無視
                    return addr
            # 同じアドレスだが内容が不一致なので置き換えが必要
            self._addr_to_init_event[addr].append(event)
            # self._all_object_ids に self._next_object_id が含まれないことを確認。含まれる場合は、
            # 含まれなくなるまで self._next_object_id を増やす。
            while self._next_object_id in self._all_object_ids:
                self._next_object_id += 1
            remap_info = IDRemappingInfo(get_field(event, '_timestamp'),
                                         get_field(event, '_vpid'),
                                         self._next_object_id)
            self._addr_to_remapping_info.setdefault(addr, []).append(remap_info)
            self._all_object_ids.add(self._next_object_id)
            self._next_object_id += 1
            return self._next_object_id - 1

        # 仕様通りの実装だとコレ
        # if len(self._addr_to_init_event[addr]) == 1:
        #    # 全てのオブジェクト ID を含む Set。元々のオブジェクトアドレスに加えて、置き換え後オブジェクト ID も含む。
        #    # MYK ↑が正しいとすると、ここで self._all_object_ids.add(addr)が必要な気がする
        #    return addr
        # else:
        #    # self._all_object_ids に self._next_object_id が含まれないことを確認。含まれる場合は、
        #       含まれなくなるまで self._next_object_id を増やす。
        #    while self._next_object_id in self._all_object_ids:
        #        self._next_object_id += 1
        #    remap_info = IDRemappingInfo( get_field(event, '_timestamp'),
        #                                 get_field(event, '_vpid'), self._next_object_id)
        #    self._addr_to_remapping_info.setdefault(addr, []).append(remap_info)
        #    self._all_object_ids.add(self._next_object_id)
        #    self._next_object_id += 1
        #    return self._next_object_id - 1

    def get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        if addr in self._addr_to_remapping_info:
            pid = get_field(event, '_vpid')
            timestamp = get_field(event, '_timestamp')
            # pid が event['_pid'] に一致し、timestamp が event['_timestamp'] 以下の要素を検索する
            list_search = \
                [item for item in self._addr_to_remapping_info[addr]
                 if item.pid == pid and item.timestamp <= timestamp]
            if len(list_search) == 0:
                # pidが一致しない場合はaddrを返す
                return addr
            elif len(list_search) == 1:
                # 見つかった要素の remapped_id を返す
                return list_search[0].remapped_id

            # 複数見つかった場合は、その中で timestamp が最も大きい要素の remapped_id を返す
            max_item = max(list_search, key=lambda item: item.timestamp)
            return max_item.remapped_id
        else:
            return addr
