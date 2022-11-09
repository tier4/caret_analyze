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

from collections import UserList
from typing import Any, Iterable, List, Optional


class UniqueList(UserList):
    """An ordered list with no duplicate values."""

    def __init__(
        self,
        init: Optional[Iterable[Any]] = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        init : _type_, optional
            initial value, by default None.
            If there are duplicate values, only the first value is inserted.

        """
        super().__init__(None)
        init = init or []
        for i in init:
            self.append(i)

    def append(self, i: Any):
        """
        Append new data.

        Parameters
        ----------
        i : _type_
            Data to append.
            If there are duplicate values, only the first value is inserted.

        """
        if i in self.data:
            return
        self.data.append(i)

    def __add__(self, other: Iterable[Any]) -> UniqueList:
        """
        Add other data.

        Parameters
        ----------
        other : Iterable[Any]
            Data to add.

        Returns
        -------
        UniqueList
            List with data added.
        Notes
        -----
            The types do not match.
            Generating a duplicate UniqueList may cause a bug.
            It might be better to have this function Duplicate.
        """
        return self.data + other

    def __iadd__(self, other: Iterable[Any]) -> UniqueList:
        """
        Add other data.

        Parameters
        ----------
        other : Iterable[Any]
            Data to add.

        Returns
        -------
        UniqueList
            List with data added.
        """
        for i in other:
            self.append(i)
        return self

    def as_list(self) -> List[Any]:
        """
        Get data as python list.

        Returns
        -------
        List[Any]
            data

        """
        return self.data
