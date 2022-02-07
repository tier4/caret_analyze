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


from itertools import product
from typing import Dict, List, Optional

from .architecture import Architecture
from .architecture.interface import ArchitectureInterface
from .architecture.interface import IGNORE_TOPICS
from .architecture.interface import PathAlias
from .callback import CallbackBase
from .communication import Communication
from .communication import VariablePassing
from .graph_search import CallbackPathSercher
from .node import Node
from .path import Path
from .record import RecordsContainer
from .util import Util


class Application(ArchitectureInterface):
    def __init__(
        self,
        file_path: str,
        file_type: str,
        records_container: Optional[RecordsContainer],
        ignore_topics: List[str] = IGNORE_TOPICS,
    ) -> None:
        self._arch = Architecture(
            file_path, file_type, records_container, ignore_topics)
        self.path: Dict[str, Path] = self._to_paths(
            self._arch.path_aliases, self._arch)

        self._path_searcher = CallbackPathSercher(
            self.nodes, self.communications, self.variable_passings
        )
        self._set_node_paths(self.nodes)

    @property
    def paths(self) -> List[Path]:
        return list(self.path.values())

    @property
    def nodes(self) -> List[Node]:
        return self._arch.nodes

    @property
    def communications(self) -> List[Communication]:
        return self._arch.communications

    @property
    def variable_passings(self) -> List[VariablePassing]:
        return self._arch.variable_passings

    @property
    def path_aliases(self) -> List[PathAlias]:
        return self._arch.path_aliases

    def export_architecture(self, file_path: str):
        for path_name, path in self.path.items():
            if not self._arch.has_path_alias(path_name):
                self._arch.add_path_alias(path_name, path.callbacks)
        self._arch.export(file_path)

    def _to_path_alias(self, alias: str, path: Path):
        callback_names = [callback.unique_name for callback in path.callbacks]
        return PathAlias(alias, callback_names)

    def search_paths(
        self, start_callback_unique_name: str, end_callback_unique_name: str
    ) -> List[Path]:
        callbacks_paths: List[List[CallbackBase]] = self._path_searcher.search(
            start_callback_unique_name, end_callback_unique_name
        )
        paths: List[Path] = [self._to_path(
            callbacks, self._arch) for callbacks in callbacks_paths]

        # Add callback itself as a path.
        if start_callback_unique_name == end_callback_unique_name:
            callback = Util.find_one(
                self.callbacks, lambda x: x.unique_name == start_callback_unique_name)
            assert callback is not None
            path = self._to_path([callback], self._arch)
            paths.append(path)

        return paths

    def _to_path(self, callbacks: List[CallbackBase], arch: Architecture) -> Path:
        return Path(callbacks, arch.communications, arch.variable_passings)

    def _to_callback(self, unique_name: str) -> CallbackBase:
        callback = Util.find_one(
            self.callbacks, lambda x: x.unique_name == unique_name)
        assert callback is not None
        return callback

    def _set_node_paths(self, nodes) -> None:
        for node in nodes:
            for start_callback, end_callback in product(node.callbacks, node.callbacks):
                node.paths += self.search_paths(
                    start_callback.unique_name, end_callback.unique_name
                )

    def _to_paths(self, path_aliases: List[PathAlias], arch: Architecture) -> Dict[str, Path]:
        path: Dict[str, Path] = {}
        for alias in path_aliases:
            callbacks: List[CallbackBase] = [
                self._to_callback(name) for name in alias.callback_names
            ]
            path[alias.path_name] = self._to_path(callbacks, arch)
        return path

    @property
    def callbacks(self) -> List[CallbackBase]:
        return Util.flatten([list(node.callbacks) for node in self.nodes])
