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

from typing import List, Optional, Tuple

import pandas as pd

from ...common import ClockConverter
from ...exceptions import UnsupportedTypeError


def convert_df_to_sim_time(
    converter: ClockConverter,
    target_df: pd.DataFrame
) -> None:
    for c in range(len(target_df.columns)):
        for i in range(len(target_df)):
            target_df.iat[i, c] = converter.convert(target_df.iat[i, c])


def validate_xaxis_type(
    xaxis_type: str
) -> None:
    if xaxis_type not in ['system_time', 'sim_time', 'index']:
        raise UnsupportedTypeError(
            f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
            'supported xaxis_type: [system_time/sim_time/index]'
        )


def get_fig_args(
    xaxis_type: str,
    title: str,
    y_axis_label: str,
    ywheel_zoom: bool
) -> dict:
    fig_args = {'frame_height': 270,
                'frame_width': 800,
                'y_axis_label': y_axis_label,
                'title': title}

    if xaxis_type == 'system_time':
        fig_args['x_axis_label'] = 'system time [s]'
    elif xaxis_type == 'sim_time':
        fig_args['x_axis_label'] = 'simulation time [s]'
    else:
        fig_args['x_axis_label'] = xaxis_type

    if(ywheel_zoom):
        fig_args['active_scroll'] = 'wheel_zoom'
    else:
        fig_args['tools'] = ['xwheel_zoom', 'xpan', 'save', 'reset']
        fig_args['active_scroll'] = 'xwheel_zoom'

    return fig_args


# Frequency
def get_preprocessing_frequency(
    *initial_timestamps: int,
    timestamp_df: pd.DataFrame,
    l2_left_column_name: Optional[str] = None,
    top_column_name: Optional[str] = None
) -> pd.DataFrame:
    def get_freq_with_timestamp(
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
        timestamp, frequency = get_freq_with_timestamp(timestamp_df,
                                                       initial,
                                                       column_name)

        top_column = top_column_name or column_name
        l2_left_column = l2_left_column_name or column_name
        ts_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
            [[top_column], [l2_left_column]]))
        fq_df = pd.DataFrame(columns=pd.MultiIndex.from_product(
            [[top_column], ['frequency [Hz]']]))
        # adding lists to dataframe
        ts_df[(top_column, l2_left_column)] = timestamp
        fq_df[(top_column, 'frequency [Hz]')] = frequency
        # adding dataframe to 'return dataframe'
        frequency_df = pd.concat([frequency_df, ts_df], axis=1)
        frequency_df = pd.concat([frequency_df, fq_df], axis=1)

    return frequency_df
