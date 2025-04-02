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

from ..infra import (ArchitectureReaderLttng,
                     ArchitectureReaderYaml)


class ArchitectureReaderFactory:

    @staticmethod
    def create_instance(file_type: str, file_path: str | list[str]):
        """
        Create an instance of ArchitectureReaderFactory.

        Parameters
        ----------
        file_type : str
            File type.
            File type allows 'yaml','yml','lttng','ctf'.
        file_path : str | list[str]
            File path.

        Raises
        ------
        ValueError
            Unsupported file_type or multiple yaml files are provided.

        """
        if file_type in ['yaml', 'yml']:
            if not isinstance(file_path, str):
                raise ValueError(
                    'Cannot create architecture object when multiple yaml files are provided.')
            return ArchitectureReaderYaml(file_path)
        elif file_type in ['lttng', 'ctf']:
            return ArchitectureReaderLttng(file_path)

        raise ValueError(f'unsupported file_type: {file_type}')
