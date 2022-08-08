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

from typing import Union

import pandas as pd

from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...runtime import Publisher, Subscription


class PubSubPeriodPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        super().__init__(pub_subs)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass  # TODO


class PubSubFrequencyPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        super().__init__(pub_subs)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass  # TODO
