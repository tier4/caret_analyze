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
import numpy as np

import pandas as pd

from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...runtime import Publisher, Subscription


class PubSubPeriodPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        self._pub_subs = pub_subs

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pub_sub_df_dict = self._create_pub_sub_df_dict()
        if(xaxis_type == 'sim_time'):
            for pub_sub_df in pub_sub_df_dict.values():
                self._df_convert_to_sim_time(pub_sub_df)

        concated_period_df = pd.DataFrame()
        for topic_name, pub_sub_df in pub_sub_df_dict.items():
            for column_name in pub_sub_df.columns:
                period_df = pd.DataFrame(columns=[
                    np.array([topic_name, topic_name]),
                    np.array([f'{column_name} [ns]', 'period [ms]'])
                ])
                period_df[(topic_name, f'{column_name} [ns]')] = \
                    pub_sub_df[column_name]
                period_df[(topic_name, 'period [ms]')] = \
                    pub_sub_df[column_name].diff() * 10**(-6)
                concated_period_df = pd.concat([concated_period_df, period_df],
                                               axis=1)
        concated_period_df.drop(concated_period_df.index[0], inplace=True)
        concated_period_df.reset_index(drop=True, inplace=True)

        return concated_period_df


class PubSubFrequencyPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        self._pub_subs = pub_subs

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pub_sub_df_dict = self._create_pub_sub_df_dict()
        if(xaxis_type == 'sim_time'):
            for pub_sub_df in pub_sub_df_dict.values():
                self._df_convert_to_sim_time(pub_sub_df)
        
        concated_frequency_df = pd.DataFrame()
        for topic_name, pub_sub_df in pub_sub_df_dict.items():
            earliest_timestamp = pub_sub_df.iloc[0].min()
            frequency_df = self._get_preprocessing_frequency(
                pub_sub_df,
                topic_name,
                earliest_timestamp
            )
            concated_frequency_df = pd.concat(
                [concated_frequency_df, frequency_df],
                axis=1
            )
    
        return concated_frequency_df

    def _get_freq_with_timestamp(
        self,
        timestamp_df: pd.DataFrame,
        initial_timestamp: int,
        column_name: str
    ) -> Tuple[List[float], List[int]]:
        timestamp_list: List[float] = []
        frequency_list: List[int] = []
        diff_base = -1

        for timestamp in timestamp_df[column_name].dropna():
            diff = timestamp - initial_timestamp
            if int(diff*10**(-9)) == diff_base:
                frequency_list[-1] += 1
            else:
                timestamp_list.append(initial_timestamp
                                      + len(timestamp_list)*10**(9))
                frequency_list.append(1)
                diff_base = int(diff*10**(-9))

        return timestamp_list, frequency_list

    def _get_preprocessing_frequency(
        self,
        timestamp_df: pd.DataFrame,
        l1_column_name: str,
        *initial_timestamps: int,
    ) -> pd.DataFrame:
        # Calculate initial timestamp
        if len(initial_timestamps) != len(timestamp_df.columns):
            if len(initial_timestamps) == 1:
                initial_timestamps = [initial_timestamps[0] for _ in
                                      range(len(timestamp_df.columns))]
            else:
                # TODO: Emit an exception when latency_table size is 0.
                initial_timestamps = [timestamp_df.iloc(0).mean for _ in
                                      range(len(timestamp_df.columns))]

        # Create frequency DataFrame
        frequency_df = pd.DataFrame()
        for initial, column_name in zip(initial_timestamps,
                                        timestamp_df.columns):
            timestamp, frequency = self._get_freq_with_timestamp(timestamp_df,
                                                                 initial,
                                                                 column_name)
            ts_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
                [[l1_column_name], [column_name]]))
            fq_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
                [[l1_column_name], ['frequency [Hz]']]))
            # adding lists to dataframe
            ts_df[(l1_column_name, column_name)] = timestamp
            fq_df[(l1_column_name, 'frequency [Hz]')] = frequency
            # adding dataframe to 'return dataframe'
            frequency_df = pd.concat([frequency_df, ts_df], axis=1)
            frequency_df = pd.concat([frequency_df, fq_df], axis=1)

        return frequency_df
