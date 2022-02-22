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
from typing import List, Optional, Union

from ..exceptions import UnsupportedTypeError
from ..runtime import Application, CallbackBase, CallbackGroup, Executor, Node


CallbacksType = Union[Application, Executor,
                      Node, CallbackGroup, List[CallbackBase]]


class TimeSeriesPlot(metaclass=ABCMeta):

    def show(self, xaxis_type: Optional[str] = None):
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xasix_type(xaxis_type)

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

        self._validate_xasix_type(xaxis_type)
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

    def _validate_xasix_type(self, xaxis_type: Optional[str]):
        if xaxis_type in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )


class Plot:

    @staticmethod
    def create_callback_frequency_plot(callbacks: CallbacksType) -> TimeSeriesPlot:
        return CallbackFrequencyPlot(callbacks)

    @staticmethod
    def create_callback_jitter_plot(callbacks: CallbacksType) -> TimeSeriesPlot:
        return CallbackJitterPlot(callbacks)

    @staticmethod
    def create_callback_latency_plot(callbacks: CallbacksType) -> TimeSeriesPlot:
        return CallbackLatencyPlot(callbacks)


class CallbackLatencyPlot(TimeSeriesPlot):

    def _to_dataframe_core(self, xaxis_type: str):
        raise NotImplementedError('')
        pass

    def _show_with_index(self):
        raise NotImplementedError('')
        pass

    def _show_with_sim_time(self):
        raise NotImplementedError('')
        pass

    def _show_with_system_time(self):
        raise NotImplementedError('')
        pass


class CallbackJitterPlot(TimeSeriesPlot):

    def _to_dataframe_core(self, xaxis_type: str):
        raise NotImplementedError('')
        pass

    def _show_with_index(self):
        raise NotImplementedError('')
        pass

    def _show_with_sim_time(self):
        raise NotImplementedError('')
        pass

    def _show_with_system_time(self):
        raise NotImplementedError('')
        pass


class CallbackFrequencyPlot(TimeSeriesPlot):

    def _to_dataframe_core(self, xaxis_type: str):
        raise NotImplementedError('')
        pass

    def _show_with_index(self):
        raise NotImplementedError('')
        pass

    def _show_with_sim_time(self):
        raise NotImplementedError('')
        pass

    def _show_with_system_time(self):
        raise NotImplementedError('')
        pass


#  Test Senario
# callbacks= []
# plot = Plot.create_callback_frequency_plot(callbacks)
# plot.to_dataframe()
# plot.show()
