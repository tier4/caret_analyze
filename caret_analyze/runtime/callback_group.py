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

from typing import List

from caret_analyze.value_objects import CallbackGroupStructValue, CallbackGroupType

from ..common import Util, CustomDict
from .callback import CallbackBase


class CallbackGroup:

    def __init__(
        self,
        callback_group_info: CallbackGroupStructValue,
        callbacks: List[CallbackBase],
    ) -> None:
        self._val = callback_group_info
        self._callbacks: List[CallbackBase] = callbacks

    @property
    def callback_group_type(self) -> CallbackGroupType:
        """
        Get callback_group_type.

        Returns
        -------
        CallbackGroupType

        """
        return self._val.callback_group_type

    @property
    def callback_group_type_name(self) -> str:
        """
        Get callback_group_type name.

        Returns
        -------
        CallbackGroupType name

        """
        return self._val.callback_group_type_name

    @property
    def callback_group_name(self) -> str:
        return self._val.callback_group_name

    @property
    def node_name(self) -> str:
        """
        Get node name.

        Returns
        -------
        str
            node name

        """
        return self._val.node_name

    @property
    def callbacks(self) -> List[CallbackBase]:
        return self._callbacks

    @property
    def summary(self) -> CustomDict:
        return self._val.summary

    def get_callback(self, callback_name: str) -> CallbackBase:
        return Util.find_one(
            lambda x: x.callback_name == callback_name,
            self._callbacks
        )
