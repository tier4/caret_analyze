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

from multimethod import multimethod as singledispatchmethod

from .unified_architecture_reader import UnifiedArchitectureReader
from ..infra.lttng import Lttng
from ..infra.lttng.architecture_reader_lttng import ArchitectureReaderLttng
from ..infra.lttng.lttng import LttngEventFilter
from ..infra.yaml.architecture_reader_yaml import ArchitectureReaderYaml


class ArchitectureReaderFactory:

    @singledispatchmethod
    def create_instance(self, args) -> None:
        raise NotImplementedError('')

    @staticmethod
    @create_instance.register
    def _create_instance(file_type: str, file_path: str):
        if file_type in ['yaml', 'yml']:
            return ArchitectureReaderYaml(file_path)
        elif file_type in ['lttng', 'ctf']:
            lttng = Lttng(file_path, event_filters=[
                          LttngEventFilter.init_pass_filter()])
            return ArchitectureReaderLttng(lttng)

        raise ValueError(f'unsupported file_type: {file_type}')

    @staticmethod
    @create_instance.register
    def _create_instance_with_sub(
        file_type_main: str, file_path_main: str,
        file_type_sub: str, file_path_sub: str,
    ):
        main_reader = ArchitectureReaderFactory._create_instance(file_type_main, file_path_main)
        sub_reader = ArchitectureReaderFactory._create_instance(file_type_sub, file_path_sub)
        return UnifiedArchitectureReader(main_reader, sub_reader)
