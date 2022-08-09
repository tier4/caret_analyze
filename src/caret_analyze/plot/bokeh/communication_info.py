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

from typing import List, Tuple

import numpy as np

import pandas as pd

from .communication_info_interface import CommunicationTimeSeriesPlot
from ...runtime import Communication


class CommunicationLatencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        self._communications = list(*communications)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concated_latency_df = pd.DataFrame()
        for comm in self._communications:
            comm_name = self._get_comm_name(comm)
            comm_df = comm.to_dataframe()
            if(xaxis_type == 'sim_time'):
                self._df_convert_to_sim_time(comm_df)
            latency_df = pd.DataFrame(columns=[
                np.array([comm_name, comm_name]),
                np.array(['rclcpp_publish_timestamp [ns]', 'latency [ms]'])
            ])
            latency_df[(comm_name, 'rclcpp_publish_timestamp [ns]')] = \
                comm_df.iloc[:, 0]
            latency_df[(comm_name, 'latency [ms]')] = \
                (comm_df.iloc[:, 3] - comm_df.iloc[:, 0]) * 10**(-6)
            concated_latency_df = pd.concat([concated_latency_df, latency_df],
                                            axis=1)

        return concated_latency_df


class CommunicationPeriodPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        self._communications = list(*communications)

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        rclcpp_pub_ts_df = self._create_rclcpp_pub_ts_df()
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(rclcpp_pub_ts_df)

        period_df = pd.DataFrame(columns=pd.MultiIndex.from_product([
            rclcpp_pub_ts_df.columns,
            ['rclcpp_publish_timestamp [ns]', 'period [ms]']
        ]))
        for comm_name in rclcpp_pub_ts_df.columns:
            period_df[(comm_name, 'rclcpp_publish_timestamp [ns]')] = \
                rclcpp_pub_ts_df[comm_name]
            period_df[(comm_name, 'period [ms]')] = \
                rclcpp_pub_ts_df[comm_name].diff() * 10**(-6)
        period_df = period_df.drop(period_df.index[0])
        period_df = period_df.reset_index(drop=True)

        return period_df


class CommunicationFrequencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        self._communications = list(*communications)

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
        l2_left_column_name: str,
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
                [[column_name], [l2_left_column_name]]))
            fq_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
                [[column_name], ['frequency [Hz]']]))
            # adding lists to dataframe
            ts_df[(column_name, l2_left_column_name)] = timestamp
            fq_df[(column_name, 'frequency [Hz]')] = frequency
            # adding dataframe to 'return dataframe'
            frequency_df = pd.concat([frequency_df, ts_df], axis=1)
            frequency_df = pd.concat([frequency_df, fq_df], axis=1)

        return frequency_df

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        rclcpp_pub_ts_df = self._create_rclcpp_pub_ts_df()
        if(xaxis_type == 'sim_time'):
            self._df_convert_to_sim_time(rclcpp_pub_ts_df)

        # TODO: Emit an exception when latency_table size is 0.
        earliest_timestamp = rclcpp_pub_ts_df.iloc[0].min()
        frequency_df = self._get_preprocessing_frequency(
            rclcpp_pub_ts_df,
            'rclcpp_publish_timestamp [ns]',
            earliest_timestamp
        )

        return frequency_df
