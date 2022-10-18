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
from typing import Collection, Union

from matplotlib.path import Path

from multimethod import multimethod as singledispatchmethod

from .callback_info import (CallbackFrequencyPlot,
                            CallbackLatencyPlot,
                            CallbackPeriodPlot)
from .callback_info_interface import TimeSeriesPlot
from .communication_info import (CommunicationFrequencyPlot,
                                 CommunicationLatencyPlot,
                                 CommunicationPeriodPlot)
from .communication_info_interface import CommunicationTimeSeriesPlot
from .histogram import ResponseTimePlot
from .pub_sub_info import PubSubFrequencyPlot, PubSubPeriodPlot
from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...exceptions import InvalidArgumentError
from ...runtime import (CallbackBase, Communication, Publisher, Subscription)

logger = getLogger(__name__)


class Plot:

    @singledispatchmethod
    def create_callback_frequency_plot(arg) -> TimeSeriesPlot:
        """
        Get CallbackFrequencyPlot instance.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        CallbackFrequencyPlot

        """
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_callback_frequency_plot.register
    def _create_callback_frequency_plot(
        callbacks: Collection[CallbackBase]
    ) -> TimeSeriesPlot:
        return CallbackFrequencyPlot(callbacks)

    @staticmethod
    @create_callback_frequency_plot.register
    def _create_callback_frequency_plot_tuple(
        *callbacks: CallbackBase
    ) -> TimeSeriesPlot:
        return CallbackFrequencyPlot(callbacks)

    @singledispatchmethod
    def create_callback_period_plot(arg) -> TimeSeriesPlot:
        """
        Get CallbackPeriodPlot instance.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        CallbackPeriodPlot

        """
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_callback_period_plot.register
    def _create_callback_period_plot(
        callbacks: Collection[CallbackBase]
    ) -> TimeSeriesPlot:
        return CallbackPeriodPlot(callbacks)

    @staticmethod
    @create_callback_period_plot.register
    def _create_callback_period_plot_tuple(
        *callbacks: CallbackBase
    ) -> TimeSeriesPlot:
        return CallbackPeriodPlot(callbacks)

    @singledispatchmethod
    def create_callback_jitter_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_callback_jitter_plot.register
    def _create_callback_jitter_plot(
        callbacks: Collection[CallbackBase]
    ) -> TimeSeriesPlot:
        logger.warning('create_callback_jitter_plot is deprecated.'
                       'Use create_callback_period_plot')
        return Plot.create_callback_period_plot(callbacks)

    @staticmethod
    @create_callback_jitter_plot.register
    def _create_callback_jitter_plot_tuple(
        *callbacks: CallbackBase
    ) -> TimeSeriesPlot:
        logger.warning('create_callback_jitter_plot is deprecated.'
                       'Use create_callback_period_plot')
        return Plot.create_callback_period_plot(callbacks)

    @singledispatchmethod
    def create_callback_latency_plot(arg) -> TimeSeriesPlot:
        """
        Get CallbackLatencyPlot instance.

        Parameters
        ----------
        callbacks : Collection[CallbackBase]
            Target callbacks.
            This also accepts multiple CallbackBase inputs by unpacking.

        Returns
        -------
        CallbackLatencyPlot

        """
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_callback_latency_plot.register
    def _create_callback_latency_plot(
        callbacks: Collection[CallbackBase]
    ) -> TimeSeriesPlot:
        return CallbackLatencyPlot(callbacks)

    @staticmethod
    @create_callback_latency_plot.register
    def _create_callback_latency_plot_tuple(
        *callbacks: CallbackBase
    ) -> TimeSeriesPlot:
        return CallbackLatencyPlot(callbacks)

    @singledispatchmethod
    def create_publish_subscription_period_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_publish_subscription_period_plot.register
    def _create_publish_subscription_period_plot(
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> PubSubTimeSeriesPlot:
        return PubSubPeriodPlot(pub_subs)

    @staticmethod
    @create_publish_subscription_period_plot.register
    def _create_publish_subscription_period_plot_tuple(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PubSubTimeSeriesPlot:
        return PubSubPeriodPlot(pub_subs)

    @singledispatchmethod
    def create_publish_subscription_frequency_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_publish_subscription_frequency_plot.register
    def _create_publish_subscription_frequency_plot(
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> PubSubTimeSeriesPlot:
        return PubSubFrequencyPlot(pub_subs)

    @staticmethod
    @create_publish_subscription_frequency_plot.register
    def _create_publish_subscription_frequency_plot_tuple(
        *pub_subs: Union[Publisher, Subscription]
    ) -> PubSubTimeSeriesPlot:
        return PubSubFrequencyPlot(pub_subs)

    @singledispatchmethod
    def create_communication_latency_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_communication_latency_plot.register
    def _create_communication_latency_plot(
        communications: Collection[Communication]
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationLatencyPlot(communications)

    @staticmethod
    @create_communication_latency_plot.register
    def _create_communication_latency_plot_tuple(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationLatencyPlot(communications)

    @singledispatchmethod
    def create_communication_frequency_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_communication_frequency_plot.register
    def _create_communication_frequency_plot(
        communications: Collection[Communication]
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationFrequencyPlot(communications)

    @staticmethod
    @create_communication_frequency_plot.register
    def _create_communication_frequency_plot_tuple(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationFrequencyPlot(communications)

    @singledispatchmethod
    def create_communication_period_plot(arg) -> TimeSeriesPlot:
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_communication_period_plot.register
    def _create_communication_period_plot(
        communications: Collection[Communication]
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationPeriodPlot(communications)

    @staticmethod
    @create_communication_period_plot.register
    def _create_communication_period_plot_tuple(
        *communications: Communication
    ) -> CommunicationTimeSeriesPlot:
        return CommunicationPeriodPlot(communications)

    @singledispatchmethod
    def create_response_time_histogram_plot(arg) -> TimeSeriesPlot:
        """
        Get ResponseTimePlot instance.

        Parameters
        ----------
        path : Collection[Path]
            Target path.
            This also accepts multiple path inputs by unpacking.

        Returns
        -------
        ResponseTimePlot

        """
        raise InvalidArgumentError(f'Unknown argument type: {arg}')

    @staticmethod
    @create_response_time_histogram_plot.register
    def _create_response_time_histogram_plot(
        paths: Collection[Path]
    ) -> ResponseTimePlot:
        return ResponseTimePlot(paths)

    @staticmethod
    @create_response_time_histogram_plot.register
    def _create_response_time_histogram_plot_tuple(
        *paths: Path
    ) -> ResponseTimePlot:
        return ResponseTimePlot(paths)
