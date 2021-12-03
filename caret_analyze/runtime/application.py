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

from __future__ import annotations, unicode_literals

from typing import List, Union

from ..infra.infra_base import InfraBase
from ..architecture import Architecture
from ..infra.lttng.records_provider_lttng import RecordsProviderLttng
from ..infra.interface import RuntimeDataProvider, RecordsProvider
from ..infra.lttng.lttng import Lttng

from ..common import Util
from .callback import CallbackBase
from .communication import Communication
from .callback_group import CallbackGroup
from .executor import Executor
from .node import Node
from .path import Path
from ..exceptions import UnsupportedTypeError


class Application():
    def __init__(
        self,
        architecture: Architecture,
        infra: InfraBase,
    ) -> None:
        from .runtime_loaded import RuntimeLoaded

        provider: Union[RecordsProvider, RuntimeDataProvider]

        if isinstance(infra, Lttng):
            provider = RecordsProviderLttng(infra)
        else:
            raise UnsupportedTypeError('')

        loaded = RuntimeLoaded(architecture, provider)

        self._nodes: List[Node] = loaded.nodes
        self._executors: List[Executor] = loaded.executors
        self._communications: List[Communication] = loaded.communications
        self._paths: List[Path] = loaded.named_paths

    @property
    def executors(self) -> List[Executor]:
        return self._executors

    @property
    def nodes(self) -> List[Node]:
        return self._nodes

    @property
    def communications(self) -> List[Communication]:
        return self._communications

    @property
    def paths(self) -> List[Path]:
        return self._paths

    @property
    def callbacks(self) -> List[CallbackBase]:
        cbs: List[CallbackBase] = []
        for node in self.nodes:
            if node.callbacks is not None:
                cbs += list(node.callbacks)
        return cbs

    def get_path(self, path_name: str) -> Path:
        def is_target_path(path: Path):
            return path.path_name == path_name

        return Util.find_one(is_target_path, self.paths)

    def get_executor(
        self,
        executor_name: str
    ) -> Executor:
        return Util.find_one(lambda x: x.executor_name == executor_name, self.executors)

    @property
    def callback_groups(
        self,
    ) -> List[CallbackGroup]:
        cbgs: List[CallbackGroup] = []
        for node in self.nodes:
            if node.callback_groups is None:
                continue
            cbgs += node.callback_groups
        return cbgs

    def get_callback_group(
        self,
        callback_group_name: str
    ) -> CallbackBase:
        def is_target(x: CallbackGroup):
            return x.callback_group_name == callback_group_name
        return Util.find_one(is_target, self.callback_groups)

    def get_communication(
        self,
        publisher_node_name: str,
        subscription_node_name: str,
        topic_name: str
    ) -> Communication:
        def is_target_comm(comm: Communication):
            return comm.publish_node_name == publisher_node_name and \
                comm.subscribe_node_name == subscription_node_name and \
                comm.topic_name == topic_name

        return Util.find_one(is_target_comm, self.communications)

    @property
    def topic_names(self) -> List[str]:
        return list(set(_.topic_name for _ in self.communications))

    @property
    def executor_names(self) -> List[str]:
        return [_.executor_name for _ in self.executors]

    @property
    def callback_group_names(self) -> List[str]:
        return [_.callback_group_name for _ in self.callback_groups]

    @property
    def path_names(self) -> List[str]:
        return [_.path_name for _ in self.paths]

    @property
    def callback_names(self) -> List[str]:
        return [_.callback_name for _ in self.callbacks]

    def get_node(self, node_name: str) -> Node:
        def is_target_node(node: Node):
            return node.node_name == node_name

        return Util.find_one(is_target_node, self.nodes)

    def get_callback(self, callback_name: str) -> CallbackBase:
        def is_target_callback(callback: CallbackBase):
            return callback.callback_name == callback_name

        return Util.find_one(is_target_callback, self.callbacks)

    @property
    def node_names(self) -> List[str]:
        return [c.node_name for c in self.nodes]
