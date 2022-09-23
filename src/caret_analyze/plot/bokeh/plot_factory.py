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

from logging import getLogger
from typing import List, Union

from .callback_info import (CallbackFrequencyPlot,
                            CallbackLatencyPlot,
                            CallbackPeriodPlot)
from .callback_info_interface import TimeSeriesPlot
from .communication_info import (CommunicationFrequencyPlot,
                                 CommunicationLatencyPlot,
                                 CommunicationPeriodPlot)
from .communication_info_interface import CommunicationTimeSeriesPlot
from .pub_sub_info import PubSubFrequencyPlot, PubSubPeriodPlot
from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...runtime import (Application, CallbackBase, CallbackGroup,
                        Communication, Executor, Node,
                        Path, Publisher, Subscription)


CallbacksType = Union[Application, Path, Executor, Node,
                      CallbackGroup, CallbackBase, List[CallbackBase]]

logger = getLogger(__name__)


class Plot:

    @staticmethod
    def create_callback_frequency_plot(
        callbacks: CallbacksType
    ) -> TimeSeriesPlot:
        """
        Get CallbackFrequencyPlot instance.

        Parameters
        ----------
        callbacks : CallbacksType
            CallbacksType = Union[Application,
                                  Path,
                                  Executor,
                                  Node,
                                  CallbackGroup,
                                  CallbackBase
                                  List[CallbackBase]].
            Instances that have callbacks or a list of callbacks.

        Returns
        -------
        CallbackFrequencyPlot

        """
        return CallbackFrequencyPlot(callbacks)

    @staticmethod
    def create_callback_jitter_plot(
        callbacks: CallbacksType
    ) -> TimeSeriesPlot:
        logger.warning('create_callback_jitter_plot is deprecated.'
                       'Use create_callback_period_plot')
        return Plot.create_callback_period_plot(callbacks)

    @staticmethod
    def create_callback_period_plot(
        callbacks: CallbacksType
    ) -> TimeSeriesPlot:
        """
        Get CallbackPeriodPlot instance.

        Parameters
        ----------
        callbacks : CallbacksType
            CallbacksType = Union[Application,
                                  Path,
                                  Executor,
                                  Node,
                                  CallbackGroup,
                                  CallbackBase
                                  List[CallbackBase]].
            Instances that have callbacks or a list of callbacks.

        Returns
        -------
        CallbackPeriodPlot

        """
        return CallbackPeriodPlot(callbacks)

    @staticmethod
    def create_callback_latency_plot(
        callbacks: CallbacksType
    ) -> TimeSeriesPlot:
        """
        Get CallbackLatencyPlot instance.

        Parameters
        ----------
        callbacks : CallbacksType
            CallbacksType = Union[Application,
                                  Path,
                                  Executor,
                                  Node,
                                  CallbackGroup,
                                  CallbackBase
                                  List[CallbackBase]].
            Instances that have callbacks or a list of callbacks.

        Returns
        -------
        CallbackLatencyPlot

        """
        return CallbackLatencyPlot(callbacks)

    @staticmethod
    def create_publish_subscription_period_plot(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PubSubTimeSeriesPlot:
        return PubSubPeriodPlot(*pub_subs)

    @staticmethod
    def create_publish_subscription_frequency_plot(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PubSubTimeSeriesPlot:
        return PubSubFrequencyPlot(*pub_subs)

    @staticmethod
    def create_communication_latency_plot(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationLatencyPlot(*communications)

    @staticmethod
    def create_communication_frequency_plot(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationFrequencyPlot(*communications)

    @staticmethod
    def create_communication_period_plot(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationPeriodPlot(*communications)
