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

import inspect

from typing import Any, Dict


class ValueObject():
    """Value object base class."""

    def __eq__(self, right):
        if type(self) != type(right):
            return False

        for attr in self.__generate_public_attrs():
            # assert getattr(self,  attr) == getattr(right, attr)
            if getattr(self,  attr) != getattr(right, attr):
                return False
        return True

    def __hash__(self):
        hash_value = 17

        hash_value += hash_value * 31 + hash(self.__class__)

        for attr in self.__generate_public_attrs():
            v = getattr(self,  attr)
            hash_value += hash_value * 31 + hash(v)

        return hash_value

    def __str__(self) -> str:
        from yaml import dump
        d = self._to_dict()
        return dump(d)

    def _to_dict(self) -> Dict:
        d: Dict[Any, Any] = {}
        for attr in self.__generate_public_attrs():
            value = getattr(self, attr)
            if isinstance(value, ValueObject):
                d[attr] = value._to_dict()
            else:
                if isinstance(value, tuple):
                    d[attr] = list(value)
                else:
                    d[attr] = value
        return d

    def __generate_public_attrs(self):
        attrs = inspect.getmembers(self)

        # ignore private variables and Constant variables
        attrs = list(filter(
            lambda x: x[0][0] != '_' and x[0][0].islower(), attrs
        ))
        for attr in attrs:
            key, value = attr[0], attr[1]
            # ignore callable
            if callable(value):
                continue
            yield key
