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
from collections.abc import Iterable
from typing import Any


class UniqueList(UserList):
    """An ordered list without duplicate values."""

    def __init__(
        self,
        init: Iterable[Any] | None = None,
    ) -> None:
        """
        Construct an instance.

        Parameters
        ----------
        init : Any, optional
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
        i : Any
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
            Updated list with added data.

        """
        #  TODO(hsgwa): fix to "self + other"
        return self.data + other  # type: ignore

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
            Updated list with added data.

        """
        for i in other:
            self.append(i)
        return self

    def as_list(self) -> list[Any]:
        """
        Get data as Python list.

        Returns
        -------
        list[Any]
            data

        """
        return self.data
