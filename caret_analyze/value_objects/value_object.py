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


class ValueObject():
    """Value object base class."""

    def __eq__(self, right):
        if type(self) != type(right):
            return False

        for p in self.__dict__.keys():
            if getattr(self,  p) != getattr(right, p):
                return False
        return True

    def __hash__(self):
        hash_value = 17

        hash_value += hash_value * 31 + hash(self.__class__)

        for p in self.__dict__.keys():
            v = getattr(self,  p)
            hash_value += hash_value * 31 + hash(v)

        return hash_value
