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

from typing import Union

import numpy as np

import pandas as pd

from .plot_util import convert_df_to_sim_time, get_preprocessing_frequency
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
            converter = self._pub_subs[0]._provider.get_sim_time_converter()
            for pub_sub_df in pub_sub_df_dict.values():
                convert_df_to_sim_time(converter, pub_sub_df)

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
            converter = self._pub_subs[0]._provider.get_sim_time_converter()
            for pub_sub_df in pub_sub_df_dict.values():
                convert_df_to_sim_time(converter, pub_sub_df)

        concated_frequency_df = pd.DataFrame()
        for topic_name, pub_sub_df in pub_sub_df_dict.items():
            earliest_timestamp = pub_sub_df.iloc[0].min()
            frequency_df = get_preprocessing_frequency(
                earliest_timestamp,
                timestamp_df=pub_sub_df,
                top_column_name=topic_name
            )
            concated_frequency_df = pd.concat(
                [concated_frequency_df, frequency_df],
                axis=1
            )

        return concated_frequency_df
