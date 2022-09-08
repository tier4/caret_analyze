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
from typing import Union

import pandas as pd

from .plot_util import (add_top_level_column, convert_df_to_sim_time,
                        get_freq_with_timestamp)
from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...runtime import Publisher, Subscription

logger = getLogger(__name__)


class PubSubPeriodPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        self._pub_subs = pub_subs

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_period_df = pd.DataFrame()
        for pub_sub in self._pub_subs:
            period_df = self._create_period_df(xaxis_type, pub_sub)
            concat_period_df = pd.concat([concat_period_df, period_df],
                                         axis=1)

        return concat_period_df.sort_index(level=0, axis=1,
                                           sort_remaining=False)

    def _create_period_df(
        self,
        xaxis_type: str,
        pub_sub: Union[Publisher, Subscription]
    ) -> pd.DataFrame:
        df = pub_sub.to_dataframe()
        if xaxis_type == 'sim_time':
            convert_df_to_sim_time(self._get_converter(), df)

        period_df = pd.DataFrame(data={
            self._get_ts_column_name(pub_sub): df.iloc[:, 0],
            'period [ms]': df.iloc[:, 0].diff() * 10**(-6)
        })
        if len(period_df) == 0:
            logger.warning(
                'Since no timestamp is recorded, '
                'the period cannot be calculated. '
                f'pub_sub_summary: {pub_sub.summary}'
            )
        else:
            period_df = period_df.drop(period_df.index[0])
        period_df = add_top_level_column(period_df, pub_sub.topic_name)

        return period_df


class PubSubFrequencyPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        *pub_subs: Union[Publisher, Subscription]
    ) -> None:
        self._pub_subs = pub_subs

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_frequency_df = pd.DataFrame()
        for pub_sub in self._pub_subs:
            frequency_df = self._create_frequency_df(xaxis_type, pub_sub)
            concat_frequency_df = pd.concat(
                [concat_frequency_df, frequency_df],
                axis=1
            )

        return concat_frequency_df.sort_index(level=0, axis=1,
                                              sort_remaining=False)

    def _create_frequency_df(
        self,
        xaxis_type: str,
        pub_sub: Union[Publisher, Subscription]
    ) -> pd.DataFrame:
        df = pub_sub.to_dataframe()
        if xaxis_type == 'sim_time':
            convert_df_to_sim_time(self._get_converter(), df)

        initial_timestamp = self._get_earliest_timestamp()
        ts_series, freq_series = get_freq_with_timestamp(df.iloc[:, 0],
                                                         initial_timestamp)
        frequency_df = pd.DataFrame(data={
            self._get_ts_column_name(pub_sub): ts_series,
            'frequency [Hz]': freq_series
        })
        frequency_df = add_top_level_column(frequency_df,
                                            pub_sub.topic_name)

        return frequency_df

    def _get_earliest_timestamp(
        self
    ) -> int:
        first_timestamps = []
        for pub_sub in self._pub_subs:
            df = pub_sub.to_dataframe()
            if len(df) == 0:
                logger.warning(
                    'Since no timestamp is recorded, '
                    'the frequency cannot be calculated. '
                    f'pub_sub_summary: {pub_sub.summary}'
                )
                continue
            first_timestamps.append(df.iloc[0, 0])

        return min(first_timestamps)
