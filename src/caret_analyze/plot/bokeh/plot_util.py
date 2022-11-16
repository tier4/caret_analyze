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

from typing import Dict, List, Sequence, Tuple

from bokeh.colors import Color, RGB

import colorcet as cc

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


class PlotColorSelector:

    def __init__(self) -> None:
        self._palette: Sequence[Color] = \
            [self._from_rgb(*rgb) for rgb in cc.glasbey_bw_minc_20]
        self._color_map: Dict[str, Color] = {}

    def get_color(
        self,
        plot_object_name: str
    ) -> Color:
        if plot_object_name not in self._color_map:
            color_index = len(self._color_map) % len(self._palette)
            self._color_map[plot_object_name] = self._palette[color_index]

        return self._color_map[plot_object_name]

    @staticmethod
    def _from_rgb(r: float, g: float, b: float) -> RGB:
        r_ = int(r*255)
        g_ = int(g*255)
        b_ = int(b*255)

        return RGB(r_, g_, b_)
