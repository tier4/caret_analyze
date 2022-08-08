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

import pandas as pd

from .communication_info_interface import CommunicationTimeSeriesPlot
from ...runtime import Communication


class CommunicationLatencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass  # TODO


class CommunicationPeriodPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass  # TODO


class CommunicationFrequencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass  # TODO
