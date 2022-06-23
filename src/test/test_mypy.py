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

import os

from ament_mypy.main import main

import pytest


reason = '[mypy test is to be officially supported in v0.4 or later.]'


@pytest.mark.skipif('GITHUB_ACTION' in os.environ, reason=reason)
@pytest.mark.mypy
@pytest.mark.linter
def test_mypy():
    rc = main()
    assert rc == 0, 'Found code style errors / warnings'
