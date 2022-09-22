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

from typing import Optional

from .data_model import Ros2DataModel
from ....exceptions import InvalidCtfDataError


class DataModelService:

    def __init__(self, data: Ros2DataModel) -> None:
        self._data = data

    def get_node_name(self, cbg_addr: int) -> str:
        node_name = self._get_node_name_from_rcl_node_init(cbg_addr)
        if node_name:
            return node_name

        node_name = self._get_node_name_from_get_parameters_srv(cbg_addr)
        if node_name:
            return node_name

        raise InvalidCtfDataError(
            'Failed to identify node name from callback group address.')

    def _get_node_name_from_rcl_node_init(
        self,
        cbg_addr: int
    ) -> Optional[str]:
        raise NotImplementedError

    def _get_node_name_from_get_parameters_srv(
        self,
        cbg_addr: int
    ) -> Optional[str]:
        raise NotImplementedError
