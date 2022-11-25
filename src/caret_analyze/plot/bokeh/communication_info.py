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
from typing import Collection, Tuple

import pandas as pd

from .communication_info_interface import CommunicationTimeSeriesPlot
from .plot_util import add_top_level_column, convert_df_to_sim_time
from ...record import Frequency
from ...runtime import Communication

logger = getLogger(__name__)


class CommunicationLatencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        communications: Collection[Communication]
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(
        self,
        xaxis_type: str
    ) -> pd.DataFrame:
        concat_latency_df = pd.DataFrame()
        for i, comm in enumerate(self._communications, 1):
            try:
                latency_df = self._create_latency_df(xaxis_type, comm)
                concat_latency_df = pd.concat([concat_latency_df, latency_df],
                                              axis=1)
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_latency_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'rclcpp_publish_timestamp [ns]', 'latency [ms]'])
                    empty_df = add_top_level_column(
                        empty_df, self._get_comm_name(comm))
                    concat_latency_df = pd.concat([
                        concat_latency_df, empty_df], axis=1)
                if len(concat_latency_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(logger, 'latency', comm)

        return concat_latency_df

    def _create_latency_df(
        self,
        xaxis_type: str,
        communication: Communication
    ) -> pd.DataFrame:
        df = communication.to_dataframe()
        if xaxis_type == 'sim_time':
            # If it is converted to simtime first, latency is calculated in simtime.
            # Latency should be calculated in system time.
            convert_df_to_sim_time(self._get_converter(), df)

        latency_df = pd.DataFrame(data={
            'rclcpp_publish_timestamp [ns]': df.iloc[:, 0],
            'latency [ms]': (df.iloc[:, -1] - df.iloc[:, 0]) * 10**(-6)
        })
        latency_df = add_top_level_column(latency_df,
                                          self._get_comm_name(communication))

        return latency_df


class CommunicationPeriodPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        communications: Collection[Communication]
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(
        self,
        xaxis_type: str
    ) -> pd.DataFrame:
        concat_period_df = pd.DataFrame()
        for i, comm in enumerate(self._communications, 1):
            try:
                period_df = self._create_period_df(xaxis_type, comm)
                concat_period_df = pd.concat([concat_period_df, period_df],
                                             axis=1)
            except IndexError:
                pass
            finally:
                if i*2 != len(concat_period_df.columns):
                    # Concatenate empty DataFrame
                    empty_df = pd.DataFrame(columns=[
                        'rclcpp_publish_timestamp [ns]', 'period [ms]'])
                    empty_df = add_top_level_column(
                        empty_df, self._get_comm_name(comm))
                    concat_period_df = pd.concat([
                        concat_period_df, empty_df], axis=1)
                if len(concat_period_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(logger, 'period', comm)

        return concat_period_df

    def _create_period_df(
        self,
        xaxis_type: str,
        communication: Communication
    ) -> pd.DataFrame:
        df = communication.to_dataframe()
        if xaxis_type == 'sim_time':
            # If it is converted to simtime first, latency is calculated in simtime.
            # Latency should be calculated in system time.
            convert_df_to_sim_time(self._get_converter(), df)

        period_df = pd.DataFrame(data={
            'rclcpp_publish_timestamp [ns]': df.iloc[:, 0],
            'period [ms]': df.iloc[:, 0].diff() * 10**(-6)
        })
        period_df = period_df.drop(period_df.index[0])
        period_df = add_top_level_column(period_df,
                                         self._get_comm_name(communication))

        return period_df


class CommunicationFrequencyPlot(CommunicationTimeSeriesPlot):

    def __init__(
        self,
        communications: Collection[Communication]
    ) -> None:
        super().__init__(communications)

    def _to_dataframe_core(
        self,
        xaxis_type: str
    ) -> pd.DataFrame:
        concat_frequency_df = pd.DataFrame()
        for i, comm in enumerate(self._communications, 1):
            try:
                frequency_df = self._create_frequency_df(xaxis_type, comm)
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
                        'rclcpp_publish_timestamp [ns]', 'frequency [Hz]'])
                    empty_df = add_top_level_column(
                        empty_df, self._get_comm_name(comm))
                    concat_frequency_df = pd.concat([
                        concat_frequency_df, empty_df], axis=1)
                if len(concat_frequency_df.iloc[:, -1]) == 0:
                    self._output_table_size_zero_warn(
                        logger, 'frequency', comm)

        return concat_frequency_df

    def _create_frequency_df(
        self,
        xaxis_type: str,
        communication: Communication
    ) -> pd.DataFrame:
        # df = callback.to_dataframe()
        # The frequency should be calculated at system time, so no conversion should be done here.
        # if xaxis_type == 'sim_time':
        #     convert_df_to_sim_time(self._get_converter(), df)

        min_time, max_time = self._get_timestamp_range()
        frequency = Frequency(communication.to_records())
        frequency_records = frequency.to_records(base_timestamp=min_time, until_timestamp=max_time)
        frequency_df = frequency_records.to_dataframe()

        frequency_df.rename(
            columns={
                frequency_df.columns[0]: 'rclcpp_publish_timestamp [ns]',
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
                                            self._get_comm_name(communication))

        return frequency_df

    def _get_timestamp_range(
        self
    ) -> Tuple[int, int]:
        # TODO(hsgwa): Duplication of source code. Migrate to records.
        first_timestamps = []
        last_timestamps = []
        for comm in self._communications:
            df = comm.to_dataframe()
            if len(df) == 0:
                continue
            first_timestamps.append(df.iloc[0, 0])
            last_timestamps.append(df.iloc[-1, 0])

        return min(first_timestamps), max(last_timestamps)
