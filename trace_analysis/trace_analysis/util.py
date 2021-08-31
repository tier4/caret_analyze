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

from __future__ import annotations

from typing import List, Any, Tuple, Callable, Optional, Iterable
from collections import UserList
import os


class Util:
    @classmethod
    def flatten(cls, x: List[List[Any]]) -> List[Any]:
        import itertools

        return list(itertools.chain.from_iterable(x))

    @classmethod
    def ext(cls, path: str) -> str:
        import os

        _, ext = os.path.splitext(path)
        return ext[1:]

    @classmethod
    def find_one(self, items: Iterable[Any], f: Callable[[Any], bool]) -> Optional[Any]:
        try:
            return next(filter(f, items))
        except StopIteration:
            return None

    @classmethod
    def ns_to_ms(cls, x: float) -> float:
        return x * 1.0e-6

    @classmethod
    def get_ext(cls, path: str) -> str:
        return os.path.basename(path).split(".")[-1]

    @classmethod
    def to_ns_and_name(cls, nodename: str) -> Tuple[str, str]:
        strs = nodename.split("/")
        ns = "/".join(strs[:-1]) + "/"
        name = strs[-1]
        return ns, name


class Singleton(object):
    def __new__(cls, *args, **kargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class UniqueList(UserList):
    def __init__(self, init=None):
        super().__init__(init)

    def append(self, i):
        if i in self.data:
            return
        self.data.append(i)

    def __add__(self, other):
        return self.data + other.data

    def __iadd__(self, other):
        for i in other:
            self.append(i)
        return self
