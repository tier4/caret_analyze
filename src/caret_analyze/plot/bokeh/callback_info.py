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

from typing import List, Union

import pandas as pd

from .callback_info_interface import TimeSeriesPlot
from .plot_util import convert_df_to_sim_time, get_preprocessing_frequency
from ...runtime import Application, CallbackBase, CallbackGroup, Executor, Node


CallbacksType = Union[Application, Executor,
                      Node, CallbackGroup, List[CallbackBase]]


class CallbackLatencyPlot(TimeSeriesPlot):
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
        latency_table = self._concate_cb_latency_table()
        if(xaxis_type == 'sim_time'):
            convert_df_to_sim_time(self._get_converter(), latency_table)

        cb_names = [c.callback_name for c in self._callbacks]
        latency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([
                cb_names,
                ['callback_start_timestamp [ns]', 'latency [ms]']]))
        for cb_name in cb_names:
            latency_df[(cb_name, 'callback_start_timestamp [ns]')] = \
                    latency_table[cb_name + '/callback_start_timestamp']
            latency_df[(cb_name, 'latency [ms]')] = (
                    latency_table[cb_name + '/callback_end_timestamp'] -
                    latency_table[cb_name + '/callback_start_timestamp']
                ) * 10**(-6)

        return latency_df


class CallbackJitterPlot(TimeSeriesPlot):
    """
    Class that provides API for callback jitter.

    This class provides the API to visualize the jitter per unit of time
    for each callback and to obtain it in the pandas DataFrame format.
    """

    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[
            :, latency_table.columns.str.contains('/callback_start_timestamp')]
        latency_table.columns = [c.replace('/callback_start_timestamp', '')
                                 for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            convert_df_to_sim_time(self._get_converter(), latency_table)

        jitter_df = pd.DataFrame(columns=pd.MultiIndex.from_product([
                latency_table.columns,
                ['callback_start_timestamp [ns]', 'period [ms]']]))
        for cb_name in latency_table.columns:
            jitter_df[(cb_name, 'callback_start_timestamp [ns]')] = \
                    latency_table[cb_name]
            jitter_df[(cb_name,
                       'period [ms]')] = latency_table[cb_name].diff()*10**(-6)
        jitter_df = jitter_df.drop(jitter_df.index[0])
        jitter_df = jitter_df.reset_index(drop=True)

        return jitter_df


class CallbackFrequencyPlot(TimeSeriesPlot):
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

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[
            :, latency_table.columns.str.contains('/callback_start_timestamp')]
        latency_table.columns = [c.replace('/callback_start_timestamp', '')
                                 for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            convert_df_to_sim_time(self._get_converter(), latency_table)

        # TODO: Emit an exception when latency_table size is 0.
        earliest_timestamp = latency_table.iloc[0].min()
        frequency_df = get_preprocessing_frequency(
            earliest_timestamp,
            timestamp_df=latency_table,
            l2_left_column_name='callback_start_timestamp [ns]'
        )

        return frequency_df
