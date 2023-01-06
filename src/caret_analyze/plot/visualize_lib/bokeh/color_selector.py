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

from __future__ import annotations

from abc import abstractmethod
from typing import Dict, Sequence

from bokeh.colors import Color, RGB

import colorcet as cc


class ColorSelectorFactory:
    """Factory class to create an instance of ColorSelector."""

    @staticmethod
    def create_instance(coloring_rule: str):
        if coloring_rule == 'unique':
            return ColorSelectorUnique()

        if coloring_rule == 'callback':
            return ColorSelectorCallback()

        if coloring_rule == 'callback_group':
            return ColorSelectorCbg()

        if coloring_rule == 'node':
            return ColorSelectorNode()


class ColorSelectorInterface:
    """Interface class of ColorSelector."""

    def __init__(self) -> None:
        self._palette: Sequence[Color] = \
            [self._from_rgb(*rgb) for rgb in cc.glasbey_bw_minc_20]
        self._color_map: Dict[str, Color] = {}

    @staticmethod
    def _from_rgb(r: float, g: float, b: float) -> Color:
        r_ = int(r*255)
        g_ = int(g*255)
        b_ = int(b*255)
        return RGB(r_, g_, b_)

    def get_color(
        self,
        node_name: str = '',
        cbg_name: str = '',
        callback_name: str = ''
    ) -> Color:
        """Get color.

        Parameters
        ----------
        node_name : str, optional
            node name, by default ''
        cbg_name : str, optional
            callback group name, by default ''
        callback_name : str, optional
            callback name, by default ''

        Returns
        -------
        Color

        """
        color_hash = self._get_color_hash(node_name, cbg_name, callback_name)

        if color_hash not in self._color_map:
            color_index = len(self._color_map) % len(self._palette)
            self._color_map[color_hash] = self._palette[color_index]

        return self._color_map[color_hash]

    @abstractmethod
    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> str:
        raise NotImplementedError()


class ColorSelectorUnique(ColorSelectorInterface):

    def __init__(self) -> None:
        super().__init__()
        self._color_index = 0

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> str:
        self._color_index += 1
        return str(self._color_index)


class ColorSelectorCallback(ColorSelectorInterface):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> str:
        return callback_name


class ColorSelectorCbg(ColorSelectorInterface):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> str:
        return cbg_name


class ColorSelectorNode(ColorSelectorInterface):

    def _get_color_hash(
        self,
        node_name: str,
        cbg_name: str,
        callback_name: str
    ) -> str:
        return node_name
