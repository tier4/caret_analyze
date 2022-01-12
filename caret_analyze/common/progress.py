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

from tqdm import tqdm


class Progress:
    enable = False

    @classmethod
    def tqdm(cls, it, *args):
        if cls.enable:
            return tqdm(it, *args)
        return it

    @classmethod
    def records_label(cls, label: str) -> str:
        if cls.enable:
            return label
        return ''
