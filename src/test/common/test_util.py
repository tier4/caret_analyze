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

from caret_analyze.architecture.architecture import Architecture
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.infra.lttng import Lttng
from caret_analyze.runtime.application import Application
from caret_analyze.runtime.callback import CallbackBase
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.executor import Executor
from caret_analyze.runtime.node import Node
from caret_analyze.runtime.path import Path
from caret_analyze.runtime.runtime_loaded import RuntimeLoaded
from caret_analyze.common.util import Util
import pytest

class TestUtil:
    def test_find_similar_one(self, mocker):
        app_mock = mocker.Mock(spec=Application)
        path_mock1 = mocker.Mock(spec=Path)
        path_mock2 = mocker.Mock(spec=Path)
        mocker.patch.object(path_mock1, 'path_name', "/AAA/BBB/CCC")
        mocker.patch.object(path_mock2, 'path_name', "/XXX/YYY/ZZZ")
        target_name = "/AAA/BBB/CCC"
        mocker.patch.object(app_mock, 'paths', [])
        app_mock.paths.append(path_mock1)
        app_mock.paths.append(path_mock2)

        assert Util.find_similar_one(target_name, app_mock.paths, lambda x: x.path_name) == path_mock1

        target_name = "/AAA/BBB/CC"
        # assert Util.find_similar_one(target_name, app_mock.paths, lambda x: x.path_name) == 

        # assert Util.find_similar_one(target_name, path_mock, lambda x: x.path_name) == path_mock
        


