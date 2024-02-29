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

from ..infra.lttng.architecture_reader_lttng import ArchitectureReaderLttng
from ..infra.yaml.architecture_reader_yaml import ArchitectureReaderYaml


class ArchitectureReaderFactory:

    @staticmethod
    def create_instance(file_type: str, file_path: str):
        if file_type in ['yaml', 'yml']:
            return ArchitectureReaderYaml(file_path)
        elif file_type in ['lttng', 'ctf']:
            return ArchitectureReaderLttng(file_path)

        raise ValueError(f'unsupported file_type: {file_type}')
