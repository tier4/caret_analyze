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

import os
from typing import Any, Callable, Iterable, List, Optional, Tuple

from ..exceptions import ItemNotFoundError, MultipleItemFoundError


class Util:

    @staticmethod
    def flatten(x: Iterable[Iterable[Any]]) -> List[Any]:
        import itertools

        return list(itertools.chain.from_iterable(x))

    @staticmethod
    def filter_items(f: Callable[[Any], bool], x: Optional[Iterable[Any]]) -> List[Any]:
        if x is None:
            return []
        return list(filter(f, x))

    @staticmethod
    def num_digit(i: int) -> int:
        return len(str(abs(i)))

    @staticmethod
    def ext(path: str) -> str:
        import os

        _, ext = os.path.splitext(path)
        return ext[1:]

    @staticmethod
    def find_one(condition: Callable[[Any], bool], items: Optional[Iterable[Any]]) -> Any:
        """
        Get a single item that matches the condition.

        Parameters
        ----------
        condition : Callable[[Any], bool]
        items : Optional[Iterable[Any]]

        Returns
        -------
        Any
            condition matched single item.

        Raises
        ------
        ItemNotFoundError
            Failed to find item that match the condition.
        MultipleItemFoundError
            Failed to identify item that match the condition.

        """
        if items is None:
            raise ItemNotFoundError('Failed find item.')

        filtered = Util.filter_items(condition, items)
        if len(filtered) == 0:
            raise ItemNotFoundError('Failed find item.')
        if len(filtered) >= 2:
            raise MultipleItemFoundError('Failed to identify item.')

        return filtered[0]

    @staticmethod
    def ns_to_ms(x: float) -> float:
        return x * 1.0e-6

    @staticmethod
    def get_ext(path: str) -> str:
        return os.path.basename(path).split('.')[-1]

    @staticmethod
    def to_ns_and_name(nodename: str) -> Tuple[str, str]:
        strs = nodename.split('/')
        ns = '/'.join(strs[:-1]) + '/'
        name = strs[-1]
        return ns, name
