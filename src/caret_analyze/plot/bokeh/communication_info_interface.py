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

from abc import ABCMeta, abstractmethod
from typing import Optional

import pandas as pd

from ...exceptions import UnsupportedTypeError
from ...runtime import Communication


class CommunicationTimeSeriesPlot(metaclass=ABCMeta):

    def show(
        self,
        xaxis_type: Optional[str] = None,
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        export_path: Optional[str] = None
    ) -> None:
        pass  # TODO

    def to_dataframe(
        self,
        xaxis_type: Optional[str] = None
    ) -> pd.DataFrame:
        xaxis_type = xaxis_type or 'system_time'
        self._validate_xaxis_type(xaxis_type)

        return self._to_dataframe_core(xaxis_type)

    def _validate_xaxis_type(self, xaxis_type: Optional[str]):
        if xaxis_type not in ['system_time', 'sim_time', 'index']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time/index]'
            )

    @abstractmethod
    def _to_dataframe_core(self, xaxis_type: str) -> pd.DataFrame:
        pass

    def _get_comm_name(
        self,
        comm: Communication
    ) -> str:
        return (f'{comm.summary["publish_node"]}|'
                f'{comm.summary["topic_name"]}|'
                f'{comm.summary["subscirbe_node"]}')

    def _create_rclcpp_pub_ts_df(
        self
    ) -> pd.DataFrame:
        rclcpp_pub_ts_df = pd.DataFrame()
        for comm in self._communications:
            pub_ts_series = comm.to_dataframe().iloc[:, 0]
            pub_ts_series.rename(self._get_comm_name(comm), inplace=True)
            rclcpp_pub_ts_df = pd.concat([rclcpp_pub_ts_df, pub_ts_series],
                                         axis=1)

        return rclcpp_pub_ts_df

    def _df_convert_to_sim_time(
        self,
        latency_table: pd.DataFrame
    ) -> None:
        converter = self._communications[0]._callback_subscription[0]._provider.get_sim_time_converter()
        for c in range(len(latency_table.columns)):
            for i in range(len(latency_table)):
                latency_table.iat[i, c] = converter.convert(
                        latency_table.iat[i, c])
