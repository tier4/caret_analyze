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

from abc import ABCMeta, abstractmethod
from typing import Optional
from ...exceptions import UnsupportedTypeError


class TimeSeriesPlot(metaclass=ABCMeta):

    def show(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        if 'system_time':
            self._show_with_system_time()
        elif 'sim_time':
            self._show_with_sim_time()
        elif 'index':
            self._show_with_index()

        raise NotImplementedError('')

    @abstractmethod
    def to_dataframe(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'

        self._validate_xaxis_type(xaxis_type)
        self._to_dataframe_core(xaxis_type)

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str):
        pass

    @abstractmethod
    def _show_with_index(self):
        pass

    @abstractmethod
    def _show_with_sim_time(self):
        pass

    @abstractmethod
    def _show_with_system_time(self):
        pass

    def _validate_xaxis_type(self, xaxis_type: Optional[str]):
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )