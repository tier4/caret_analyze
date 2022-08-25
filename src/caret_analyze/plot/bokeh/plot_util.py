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


def add_top_level_column(
    target_df: pd.DataFrame,
    column_name: str
) -> pd.DataFrame:
    return pd.concat([target_df], keys=[column_name], axis=1)


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


def get_freq_with_timestamp(
    source_ts_series: pd.Series,
    initial_timestamp: int
) -> Tuple[pd.Series, pd.Series]:
    timestamp_list: List[float] = []
    frequency_list: List[int] = []
    diff_base = -1

    for timestamp in source_ts_series.dropna():
        diff = timestamp - initial_timestamp
        if int(diff*10**(-9)) == diff_base:
            frequency_list[-1] += 1
        else:
            timestamp_list.append(initial_timestamp
                                  + len(timestamp_list)*10**(9))
            frequency_list.append(1)
            diff_base = int(diff*10**(-9))

    return pd.Series(timestamp_list), pd.Series(frequency_list)
