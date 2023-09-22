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

from collections.abc import Iterable
from typing import Any

from tqdm import tqdm


class Progress:
    """
    Class that manages the progress bar.

    Set Progress.enable = True if to display progress bar.
    """

    enable = False

    @classmethod
    def tqdm(cls, it, *args) -> Iterable[Any]:
        """
        Progress bar for python iterators.

        Parameters
        ----------
        it : _type_
            iterator

        Returns
        -------
        Iterable[Any]
            iterator.
            progress bar is enabled if Progress.enable == True, disabled otherwise.

        """
        if cls.enable:
            return tqdm(it, *args)
        return it
