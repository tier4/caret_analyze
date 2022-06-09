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

from typing import List, Tuple, Union

import pandas as pd

from .callback_info_interface import TimeSeriesPlot
from ...runtime import Application, CallbackBase, CallbackGroup, Executor, Node


CallbacksType = Union[Application, Executor,
                      Node, CallbackGroup, List[CallbackBase]]


class CallbackLatencyPlot(TimeSeriesPlot):
    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)

        cb_names = [c.callback_name for c in self._callbacks]
        latency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([
                cb_names,
                ['callback_start_timestamp [ns]', 'latency [ms]']]))
        for cb_name in cb_names:
            latency_df[(cb_name, 'callback_start_timestamp [ns]')] = latency_table[
                    cb_name + '/callback_start_timestamp']
            latency_df[(cb_name, 'latency [ms]')] = (
                    latency_table[cb_name + '/callback_end_timestamp'] -
                    latency_table[cb_name + '/callback_start_timestamp']
                ) * 10**(-6)

        return latency_df


class CallbackJitterPlot(TimeSeriesPlot):
    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[:, latency_table.columns.str.contains(
                '/callback_start_timestamp')]
        latency_table.columns = [
                c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)

        jitter_df = pd.DataFrame(columns=pd.MultiIndex.from_product([
                latency_table.columns,
                ['callback_start_timestamp [ns]', 'period [ms]']]))
        for cb_name in latency_table.columns:
            jitter_df[(cb_name, 'callback_start_timestamp [ns]')] = latency_table[cb_name]
            jitter_df[(cb_name, 'period [ms]')] = latency_table[cb_name].diff() * 10**(-6)
        jitter_df = jitter_df.drop(jitter_df.index[0])
        jitter_df = jitter_df.reset_index(drop=True)

        return jitter_df


class CallbackFrequencyPlot(TimeSeriesPlot):
    def __init__(
        self,
        target: CallbacksType
    ) -> None:
        super().__init__(target)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        latency_table = self._concate_cb_latency_table()
        latency_table = latency_table.loc[:, latency_table.columns.str.contains(
                '/callback_start_timestamp')]
        latency_table.columns = [
                c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(latency_table)

        # TODO: Emit an exception when latency_table size is 0.
        earliest_timestamp = latency_table.iloc[0].min()
        frequency_df = self._get_preprocessing_frequency(latency_table, earliest_timestamp)

        return frequency_df

    def _get_cb_freq_with_timestamp(
        self,
        timestamp_df,
        initial_timestamp,
        callback_name
    ) -> Tuple[list, list]:
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
        frequency_df = pd.DataFrame()

        if len(initial_timestamp) != len(timestamp_df.columns):
            if len(initial_timestamp) == 1:
                initial_timestamp = [
                    initial_timestamp[0] for i in range(len(timestamp_df.columns))]
            else:
                # TODO: Emit an exception when latency_table size is 0.
                initial_timestamp = [
                    timestamp_df.iloc(0).mean for i in range(len(timestamp_df.columns))]

        for initial, callback_name in zip(initial_timestamp, timestamp_df.columns):
            timestamp, frequency = self._get_cb_freq_with_timestamp(
                    timestamp_df, initial, callback_name)
            ts_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
                [[callback_name], ['callback_start_timestamp [ns]']]))
            fq_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
                [[callback_name], ['frequency [Hz]']]))
            # adding lists to dataframe
            ts_df[(callback_name, 'callback_start_timestamp [ns]')] = timestamp
            fq_df[(callback_name, 'frequency [Hz]')] = frequency
            # adding dataframe to 'return dataframe'
            frequency_df = pd.concat([frequency_df, ts_df], axis=1)
            frequency_df = pd.concat([frequency_df, fq_df], axis=1)

        return frequency_df
