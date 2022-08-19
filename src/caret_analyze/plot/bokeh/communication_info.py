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

import numpy as np

import pandas as pd

from .communication_info_interface import CommunicationTimeSeriesPlot
from .plot_util import convert_df_to_sim_time, get_preprocessing_frequency
from ...runtime import Communication


class CommunicationLatencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        *communications: Communication
    ) -> None:
        self._communications = communications

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        concated_latency_df = pd.DataFrame()
        for comm in self._communications:
            comm_df = comm.to_dataframe()
            if(xaxis_type == 'sim_time'):
                convert_df_to_sim_time(self._get_converter(), comm_df)
            comm_name = self._get_comm_name(comm)

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
        self._communications = communications

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        rclcpp_pub_ts_df = self._create_rclcpp_pub_ts_df()
        if(xaxis_type == 'sim_time'):
            convert_df_to_sim_time(self._get_converter(), rclcpp_pub_ts_df)

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
        self._communications = communications

    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        rclcpp_pub_ts_df = self._create_rclcpp_pub_ts_df()
        if(xaxis_type == 'sim_time'):
            convert_df_to_sim_time(self._get_converter(), rclcpp_pub_ts_df)

        # TODO: Emit an exception when latency_table size is 0.
        earliest_timestamp = rclcpp_pub_ts_df.iloc[0].min()
        frequency_df = get_preprocessing_frequency(
            earliest_timestamp,
            timestamp_df=rclcpp_pub_ts_df,
            l2_left_column_name='rclcpp_publish_timestamp [ns]'
        )

        return frequency_df
