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

from collections import UserList
from typing import List, Optional


class UniqueList(UserList):

    def __init__(self, init=None):
        super().__init__(None)
        init = init or []
        for i in init:
            self.append(i)

    def append(self, i):
        if i in self.data:
            return
        self.data.append(i)

    def __add__(self, other):
        return self.data + other

    def __iadd__(self, other):
        for i in other:
            self.append(i)
        return self


class Columns(UniqueList):

    def __init__(self, init: Optional[List[str]] = None):
        super().__init__(init=init)

    def as_list(self) -> List[str]:
        return list(self)
