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

from abc import ABCMeta, abstractproperty
from collections import UserDict
from typing import Any, Dict, List, Union


class Summary(UserDict):
    """Temporary dictionary to use when outputting summaries."""

    def pprint(self):
        print((self))

    def __hash__(self):
        # ignore hash value
        return 0

    def __eq__(self, other: Any):
        # ignore equal
        return True

    def __str__(self) -> str:
        from yaml import dump
        return dump(self._convert_safe(self.data))

    @staticmethod
    def _convert_safe(obj) -> Union[List, Dict]:
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

    @abstractproperty
    def summary(self) -> Summary:
        pass
