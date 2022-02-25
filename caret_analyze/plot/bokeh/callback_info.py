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

from subprocess import call
from typing import List, Union, Tuple
import pandas as pd
from bokeh.plotting import figure, show
from bokeh.palettes import d3

from caret_analyze.runtime import callback

from .callback_info_interface import TimeSeriesPlot
from ...runtime import Application, CallbackBase, CallbackGroup, Executor, Node


CallbacksType = Union[Application, Executor,
                      Node, CallbackGroup, List[CallbackBase]]


class CallbackLatencyPlot(TimeSeriesPlot):
    def __init__(
        self,
        target : CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)

        callback_names = [c.callback_name for c in self._callbacks]
        latency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([callback_names, ['callback_start_timestamp [ns]', 'latency [ms]']]))
        for callback_name in callback_names:
            latency_df[(callback_name, 'callback_start_timestamp [ns]')] = latency_table[callback_name + '/callback_start_timestamp']
            latency_df[(callback_name, 'latency [ms]')] = (latency_table[callback_name + '/callback_end_timestamp'] - latency_table[callback_name + '/callback_start_timestamp']) * 10**(-6)
            
        return latency_df

    def _show_core(self, xaxis_type: str):
        latency_df = self._to_dataframe_core(xaxis_type)
        self._show_from_multi_index_df(latency_df, xaxis_type, 'Time-line of callbacks latency')


class CallbackJitterPlot(TimeSeriesPlot):
    def __init__(
        self,
        target : CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[:, latency_table.columns.str.contains('callback_start_timestamp')]
        latency_table.columns = [c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)

        jitter_df = pd.DataFrame(columns=pd.MultiIndex.from_product([latency_table.columns, ['callback_start_timestamp [ns]', 'jitter [ms]']]))
        for callback_name in latency_table.columns:
            jitter_df[(callback_name, 'callback_start_timestamp [ns]')] = latency_table[callback_name]
            jitter_df[(callback_name, 'jitter [ms]')] = latency_table[callback_name].diff() * 10**(-6)
        jitter_df = jitter_df.drop(jitter_df.index[0])
        jitter_df = jitter_df.reset_index(drop=True)
            
        return jitter_df

    def _show_core(self, xaxis_type: str):
        jitter_df = self._to_dataframe_core(xaxis_type)
        self._show_from_multi_index_df(jitter_df, xaxis_type, 'Time-line of callbacks jitter')


class CallbackFrequencyPlot(TimeSeriesPlot):
    def __init__(
        self,
        target : CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[:, latency_table.columns.str.contains('/callback_start_timestamp')]
        latency_table.columns = [c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)
        
        earliest_timestamp = latency_table.iloc[0].min()  # get earlist time of the target callback timestamp (Optional)
        frequency_df = self._get_preprocessing_frequency(latency_table, earliest_timestamp)
            
        return frequency_df

    def _show_core(self, xaxis_type: str):
        frequency_df = self._to_dataframe_core(xaxis_type)
        self._show_from_multi_index_df(frequency_df, xaxis_type, 'Time-line of callbacks frequency')
    
    def _get_cb_freq_with_timestamp(self, timestamp_df, initial_timestamp, callback_name) -> Tuple[list, list]:
        timestamp_list = []
        frequency_list = []
        diff_base = -1

        for timestamp in timestamp_df[callback_name].dropna():
            diff = timestamp - initial_timestamp
            if int(diff*10**(-9)) == diff_base:
                frequency_list[-1] += 1
            else:
                timestamp_list.append(initial_timestamp + len(timestamp_list)*10**(9))
                frequency_list.append(1)
                diff_base = int(diff*10**(-9))
                
        return timestamp_list, frequency_list

    def _get_preprocessing_frequency(self, timestamp_df, *initial_timestamp) -> pd.DataFrame:
        frequency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([timestamp_df.columns, ['callback_start_timestamp [ns]', 'frequency [Hz]']]))
        
        if len(initial_timestamp) != len(timestamp_df.columns):
            if len(initial_timestamp) == 1:
                initial_timestamp = [initial_timestamp[0] for i in range(len(timestamp_df.columns))]
            else:
                initial_timestamp = [timestamp_df.iloc(0).mean for i in range(len(timestamp_df.columns))]
        
        for initial, callback_name in zip(initial_timestamp, timestamp_df.columns):
            timestamp, frequency = self._get_cb_freq_with_timestamp(timestamp_df, initial, callback_name)
            frequency_df[(callback_name, 'callback_start_timestamp [ns]')] = timestamp              
            frequency_df[(callback_name, 'frequency [Hz]')] = frequency
        
        return frequency_df