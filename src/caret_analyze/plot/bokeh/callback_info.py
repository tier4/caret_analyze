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

import pandas as pd

from .callback_info_interface import CallbackTimeSeriesPlot
from .plot_util import (add_top_level_column, convert_df_to_sim_time,
                        get_freq_with_timestamp)
from ...runtime import (Application, CallbackBase, CallbackGroup,
                        Executor, Node, Path)

logger = getLogger(__name__)

CallbacksType = Union[Application, Path, Executor, Node,
                      CallbackGroup, CallbackBase, List[CallbackBase]]


class CallbackLatencyPlot(CallbackTimeSeriesPlot):
    """
    Class that provides API for callback latency.

    This class provides the API to visualize the latency per unit of time
    for each callback and to obtain it in the pandas DataFrame format.
    """

    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_latency_df = pd.DataFrame()
        for i, cb in enumerate(self._callbacks, 1):
            try:
                latency_df = self._create_latency_df(xaxis_type, cb)
                concat_latency_df = pd.concat([concat_latency_df, latency_df],
                                              axis=1)
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_latency_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'callback_start_timestamp [ns]', 'latency [ms]'])
                    empty_df = add_top_level_column(empty_df, cb.callback_name)
                    concat_latency_df = pd.concat([
                        concat_latency_df, empty_df], axis=1)
                if len(concat_latency_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(logger, 'latency', cb)

        return concat_latency_df

    def _create_latency_df(
        self,
        xaxis_type: str,
        callback: CallbackBase
    ) -> pd.DataFrame:
        df = callback.to_dataframe()
        if xaxis_type == 'sim_time':
            convert_df_to_sim_time(self._get_converter(), df)

        latency_df = pd.DataFrame(data={
            'callback_start_timestamp [ns]': df.iloc[:, 0],
            'latency [ms]': (df.iloc[:, 1] - df.iloc[:, 0]) * 10**(-6)
        })
        latency_df = add_top_level_column(latency_df, callback.callback_name)

        return latency_df


class CallbackPeriodPlot(CallbackTimeSeriesPlot):
    """
    Class that provides API for callback period.

    This class provides the API to visualize the period per unit of time
    for each callback and to obtain it in the pandas DataFrame format.
    """

    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_period_df = pd.DataFrame()
        for i, cb in enumerate(self._callbacks, 1):
            try:
                period_df = self._create_period_df(xaxis_type, cb)
                concat_period_df = pd.concat([concat_period_df, period_df],
                                             axis=1)
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_period_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'callback_start_timestamp [ns]', 'period [ms]'])
                    empty_df = add_top_level_column(empty_df, cb.callback_name)
                    concat_period_df = pd.concat([
                        concat_period_df, empty_df], axis=1)
                if len(concat_period_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(logger, 'period', cb)

        return concat_period_df

    def _create_period_df(
        self,
        xaxis_type: str,
        callback: CallbackBase
    ) -> pd.DataFrame:
        df = callback.to_dataframe()
        if xaxis_type == 'sim_time':
            convert_df_to_sim_time(self._get_converter(), df)

        period_df = pd.DataFrame(data={
            'callback_start_timestamp [ns]': df.iloc[:, 0],
            'period [ms]': df.iloc[:, 0].diff() * 10**(-6)
        })
        period_df = period_df.drop(period_df.index[0])
        period_df = add_top_level_column(period_df, callback.callback_name)

        return period_df


class CallbackFrequencyPlot(CallbackTimeSeriesPlot):
    """
    Class that provides API for callback execution frequency.

    This class provides the API to visualize the execution frequency
    per unit of time for each callback and to obtain it
    in the pandas DataFrame format.
    """

    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(
        self,
        xaxis_type: str
    ) -> pd.DataFrame:
        concat_frequency_df = pd.DataFrame()
        for i, cb in enumerate(self._callbacks, 1):
            try:
                frequency_df = self._create_frequency_df(xaxis_type, cb)
                concat_frequency_df = pd.concat(
                    [concat_frequency_df, frequency_df],
                    axis=1
                )
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_frequency_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'callback_start_timestamp [ns]', 'frequency [Hz]'])
                    empty_df = add_top_level_column(empty_df, cb.callback_name)
                    concat_frequency_df = pd.concat([
                        concat_frequency_df, empty_df], axis=1)
                if len(concat_frequency_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(logger, 'frequency', cb)

        return concat_frequency_df

    def _create_frequency_df(
        self,
        xaxis_type: str,
        callback: CallbackBase
    ) -> pd.DataFrame:
        df = callback.to_dataframe()
        if xaxis_type == 'sim_time':
            convert_df_to_sim_time(self._get_converter(), df)

        initial_timestamp = self._get_earliest_timestamp()
        ts_series, freq_series = get_freq_with_timestamp(df.iloc[:, 0],
                                                         initial_timestamp)
        frequency_df = pd.DataFrame(data={
            'callback_start_timestamp [ns]': ts_series,
            'frequency [Hz]': freq_series
        })
        frequency_df = add_top_level_column(frequency_df,
                                            callback.callback_name)

        return frequency_df

    def _get_earliest_timestamp(
        self
    ) -> int:
        first_timestamps = []
        for cb in self._callbacks:
            df = cb.to_dataframe()
            if len(df) == 0:
                continue
            first_timestamps.append(df.iloc[0, 0])

        return min(first_timestamps)
