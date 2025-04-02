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

from logging import getLogger, INFO, StreamHandler

from .architecture import Architecture
from .architecture_loaded import NodeValuesLoaded
from .architecture_reader_factory import ArchitectureReaderFactory
from ..common import Util
from ..value_objects import NodePathStructValue


def check_procedure(
    file_type: str,
    file_path: str | list[str],
    app_arch: Architecture,
    node_name: str,
) -> tuple[NodePathStructValue, ...]:
    handler = StreamHandler()
    handler.setLevel(INFO)

    root_logger = getLogger()
    root_logger.addHandler(handler)

    reader = ArchitectureReaderFactory.create_instance(file_type, file_path)
    node = Util.find_one(lambda x: x.node_name == node_name, app_arch._nodes)

    paths = NodeValuesLoaded._search_node_paths(
                                node,
                                reader.get_message_contexts(node),
                                app_arch._max_callback_construction_order_on_path_searching
                            )

    root_logger.removeHandler(handler)
    return tuple(v.to_value() for v in paths)
