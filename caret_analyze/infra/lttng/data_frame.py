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

# from collections import UserList
# from typing import List, Optional

from typing import Dict, List, Union, Optional

import pandas as pd
import numpy as np


DataType = Union[int, str]


class TracePointData:

    def __init__(self, columns: List[str]):
        self._columns = columns
        self._dicts: List[Dict[str, Union[str, int]]] = []
        self._data: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        assert self._data is not None
        return self._data

    def append(self, row: Dict[str, Union[str, int]]) -> None:

        columns_diff = set(row.keys()) ^ set(self.columns)
        assert len(columns_diff) == 0
        self._dicts.append(row)

    def replace(
        self,
        column_name: str,
        replace_map: Dict[DataType, DataType],
    ) -> None:
        for d in self._dicts:
            while d[column_name] in replace_map:
                old = d[column_name]
                d[column_name] = replace_map[old]

    @property
    def columns(self) -> List[str]:
        return self._columns

    def finalize(self) -> None:
        if len(self._dicts) == 0:
            data = pd.DataFrame.from_dict(
                {c: [None] for c in self.columns},
                dtype=object
            )
        else:
            data = pd.DataFrame.from_dict(self._dicts, dtype=object)
        self._data = data
        del self._dicts
