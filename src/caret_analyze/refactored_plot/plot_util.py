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

from typing import Dict, Sequence

from bokeh.colors import Color, RGB

import colorcet as cc


# TODO: refactor
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
