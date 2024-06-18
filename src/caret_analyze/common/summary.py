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

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections import UserDict
from typing import Any

from yaml import dump


class Summary(UserDict):
    """
    Summary about value objects and runtime data objects.

    Note:
    ----
    The class is used to get an overview of the instance
    without any effect to __eq__ and  __hash__.
    Users can get an overview in dictionary form or check the overview in the standard output.

    """

    def pprint(self):
        print((self))

    def __hash__(self):
        """Return zero to ignore [override]."""
        return 0

    def __eq__(self, other: Any):
        """Return True to ignore [override]."""
        return True

    def __str__(self) -> str:
        """Return yaml-format string."""
        return dump(self._convert_safe(self.data))

    @staticmethod
    def _convert_safe(obj: Any) -> list | dict:
        """
        Convert a object to a object convertible to YAML.

        Parameters
        ----------
        obj : Any
            Object to convert.

        Returns
        -------
        list | dict
            converted object.

        """
        if isinstance(obj, tuple) or isinstance(obj, list):
            return [Summary._convert_safe(_) for _ in obj]

        if isinstance(obj, Summary) or isinstance(obj, dict):
            dic = {}
            for k, v in obj.items():
                k_ = Summary._convert_safe(k)
                v_ = Summary._convert_safe(v)
                dic[k_] = v_
            return dic

        return obj


class Summarizable(metaclass=ABCMeta):
    """Abstract base class that have summary property."""

    @property
    @abstractmethod
    def summary(self) -> Summary:
        """
        Get summary.

        Returns
        -------
        Summary
            summary info.

        """
        pass
