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


from typing import List

from caret_analyze.record import Records, RecordsInterface
from caret_analyze.runtime.path_base import PathBase

from pytest_mock import MockerFixture


class PathSample(PathBase):

    def __init__(self) -> None:
        super().__init__()

    def _to_records_core(self) -> RecordsInterface:
        return Records()

    @property
    def column_names(self) -> List[str]:
        return []


class TestPathBase:

    def test_cache(self, mocker: MockerFixture):
        path = PathSample()

        records_mock = mocker.Mock(spec=Records)
        mocker.patch.object(path, '_to_records_core', return_value=records_mock)

        path.to_records()
        assert path._to_records_core.call_count == 1  # type: ignore

        path.to_records()
        assert path._to_records_core.call_count == 1  # type: ignore

        path.clear_cache()
        path.to_records()
        assert path._to_records_core.call_count == 2  # type: ignore
