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

# from abc import ABCMeta, abstractmethod, abstractproperty
# from collections.abc import Iterable, Iterator, Sequence, Sized
# from datetime import datetime
# from functools import cached_property
from collections import defaultdict

from logging import getLogger
# import os
# import pickle
# from typing import Any

# import bt2
# import pandas as pd
# from tqdm import tqdm
# from .events_factory import EventsFactory
# from .lttng_event_filter import LttngEventFilter
# from .ros2_tracing.data_model import Ros2DataModel
# from .ros2_tracing.data_model_service import DataModelService
# from .ros2_tracing.processor import get_field, Ros2Handler
from .ros2_tracing.processor import get_field
# from .value_objects import (CallbackGroupId,
#                             PublisherValueLttng,
#                             ServiceCallbackValueLttng,
#                             SubscriptionCallbackValueLttng,
#                             TimerCallbackValueLttng)
# from ..infra_base import InfraBase
# from ...common import ClockConverter
# from ...exceptions import InvalidArgumentError
# from ...record import RecordsInterface
# from ...value_objects import  \
#     CallbackGroupValue, ExecutorValue, NodeValue, NodeValueWithId, Qos, TimerValue

Event = dict[str, int]

logger = getLogger(__name__)


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
        self.addr_to_init_event_: dict = defaultdict(list)
        self.addr_to_remapping_info_: dict = {}
        self.all_object_ids_: set = set()
        self.next_object_id_ = 1

    def register_and_get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        # MYK test
        # self.all_object_ids_.add(1)
        # self.all_object_ids_.add(2)
        # self.all_object_ids_.add(3)
        # while self.next_object_id_ in self.all_object_ids_:
        #     self.next_object_id_ += 1
        # remap_info = IDRemappingInfo(
        #     get_field(event, '_timestamp'), get_field(event, '_vpid'), self.next_object_id_)
        # self.addr_to_remapping_info_[addr] = remap_info
        # self.addr_to_remapping_info_[addr+1] = remap_info

        # register initialization trace event
        if len(self.addr_to_init_event_[addr]) == 0:
            self.addr_to_init_event_[addr].append(event)
            self.all_object_ids_.add(addr)
            return addr
        else:
            # 同じアドレスがすでに使われている
            for val_event in self.addr_to_init_event_[addr]:
                if val_event == event:
                    # 内容が完全に一致するイベント（記録時の都合で重複出力されたイベント）は
                    # 一つのみを登録するので、これは無視
                    return addr
            # 同じアドレスだが内容が不一致なので置き換えが必要
            self.addr_to_init_event_[addr].append(event)
            # self.all_object_ids_ に self.next_object_id_ が含まれないことを確認。含まれる場合は、
            # 含まれなくなるまで self.next_object_id_ を増やす。
            while self.next_object_id_ in self.all_object_ids_:
                self.next_object_id_ += 1
            remap_info = IDRemappingInfo(get_field(event, '_timestamp'),
                                         get_field(event, '_vpid'), self.next_object_id_)
            self.addr_to_remapping_info_.setdefault(addr, []).append(remap_info)
            self.all_object_ids_.add(self.next_object_id_)
            self.next_object_id_ += 1
            return self.next_object_id_ - 1

        # 仕様通りの実装だとコレ
        # if len(self.addr_to_init_event_[addr]) == 1:
        #    # 全てのオブジェクト ID を含む Set。元々のオブジェクトアドレスに加えて、置き換え後オブジェクト ID も含む。
        #    # MYK ↑が正しいとすると、ここで self.all_object_ids_.add(addr)が必要な気がする
        #    return addr
        # else:
        #    # self.all_object_ids_ に self.next_object_id_ が含まれないことを確認。含まれる場合は、
        #       含まれなくなるまで self.next_object_id_ を増やす。
        #    while self.next_object_id_ in self.all_object_ids_:
        #        self.next_object_id_ += 1
        #    remap_info = IDRemappingInfo( get_field(event, '_timestamp'),
        #                                 get_field(event, '_vpid'), self.next_object_id_)
        #    self.addr_to_remapping_info_.setdefault(addr, []).append(remap_info)
        #    self.all_object_ids_.add(self.next_object_id_)
        #    self.next_object_id_ += 1
        #    return self.next_object_id_ - 1

    def get_object_id(
        self,
        addr: int,
        event: dict,
    ) -> int:
        if addr in self.addr_to_remapping_info_:
            pid = get_field(event, '_vpid')
            timestamp = get_field(event, '_timestamp')
            # pid が event['_pid'] に一致し、timestamp が event['_timestamp'] 以下の要素を検索する
            list_search = \
                [item for item in self.addr_to_remapping_info_[addr]
                 if item.pid == pid and item.timestamp <= timestamp]
            if len(list_search) == 0:
                # pidが一致しない場合はaddrを返す
                return addr
            elif len(list_search) == 1:
                # 見つかった要素の remapped_id を返す
                return list_search[0].remapped_id

            # 複数見つかった場合は、その中で timestamp が最も大きい要素の remapped_id を返す
            max_remapped_id = list_search[0].timestamp
            max_remapped_id = \
                [item.remapped_id for item in list_search if item.timestamp > max_remapped_id]
            return max_remapped_id[0]
        else:
            return addr
