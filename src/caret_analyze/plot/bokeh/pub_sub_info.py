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
from typing import Collection, Tuple, Union

import pandas as pd

from .plot_util import add_top_level_column, convert_df_to_sim_time
from .pub_sub_info_interface import PubSubTimeSeriesPlot
from ...record import Frequency
from ...runtime import Publisher, Subscription

logger = getLogger(__name__)


class PubSubPeriodPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> None:
        super().__init__(pub_subs)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_period_df = pd.DataFrame()
        for i, pub_sub in enumerate(self._pub_subs, 1):
            try:
                period_df = self._create_period_df(xaxis_type, pub_sub)
                concat_period_df = pd.concat([concat_period_df, period_df],
                                             axis=1)
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_period_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        self._get_ts_column_name(pub_sub), 'period [ms]'])
                    empty_df = add_top_level_column(
                        empty_df, pub_sub.topic_name)
                    concat_period_df = pd.concat([
                        concat_period_df, empty_df], axis=1)
                if len(concat_period_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(
                        logger, 'period', pub_sub)

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
        period_df = period_df.drop(period_df.index[0])
        period_df = add_top_level_column(period_df, pub_sub.topic_name)

        return period_df


class PubSubFrequencyPlot(PubSubTimeSeriesPlot):

    def __init__(
        self,
        pub_subs: Collection[Union[Publisher, Subscription]]
    ) -> None:
        super().__init__(pub_subs)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concat_frequency_df = pd.DataFrame()
        for i, pub_sub in enumerate(self._pub_subs, 1):
            try:
                frequency_df = self._create_frequency_df(xaxis_type, pub_sub)
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
                        self._get_ts_column_name(pub_sub), 'frequency [Hz]'])
                    empty_df = add_top_level_column(
                        empty_df, pub_sub.topic_name)
                    concat_frequency_df = pd.concat([
                        concat_frequency_df, empty_df], axis=1)
                if len(concat_frequency_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(
                        logger, 'frequency', pub_sub)

        return concat_frequency_df.sort_index(level=0, axis=1,
                                              sort_remaining=False)

    def _create_frequency_df(
        self,
        xaxis_type: str,
        pub_sub: Union[Publisher, Subscription]
    ) -> pd.DataFrame:
        # The frequency should be calculated at system time, so no conversion should be done here.
        # df = pub_sub.to_dataframe()
        # if xaxis_type == 'sim_time':
        #     convert_df_to_sim_time(self._get_converter(), df)

        min_time, max_time = self._get_earliest_timestamp()
        frequency = Frequency(pub_sub.to_records())
        frequency_records = frequency.to_records(base_timestamp=min_time, until_timestamp=max_time)
        frequency_df = frequency_records.to_dataframe()

        frequency_df.rename(
            columns={
                frequency_df.columns[0]: self._get_ts_column_name(pub_sub),
                frequency_df.columns[1]: 'frequency [Hz]',
            },
            inplace=True
        )

        if xaxis_type == 'sim_time':
            # Convert only the column of measured time to simtime.
            converter = self._get_converter()
            for i in range(len(frequency_df)):
                frequency_df.iat[i, 0] = converter.convert(frequency_df.iat[i, 0])

        frequency_df = add_top_level_column(frequency_df,
                                            pub_sub.topic_name)

        return frequency_df

    def _get_earliest_timestamp(
        self
    ) -> Tuple[int, int]:
        # TODO(hsgwa): Duplication of source code. Migrate to records.
        first_timestamps = []
        last_timestamps = []
        for pub_sub in self._pub_subs:
            df = pub_sub.to_dataframe()
            if len(df) == 0:
                continue
            first_timestamps.append(df.iloc[0, 0])
            last_timestamps.append(df.iloc[-1, 0])

        return min(first_timestamps), max(last_timestamps)
