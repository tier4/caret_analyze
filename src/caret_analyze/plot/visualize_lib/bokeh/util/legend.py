# Copyright 2021 TIER IV, Inc.
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

from collections import defaultdict
from logging import getLogger
from typing import Any

from bokeh.models.annotations import Legend
from bokeh.models.renderers import GlyphRenderer

from .....runtime import CallbackBase, Communication, Path, Publisher, Subscription

TargetTypes = (CallbackBase | Communication | Path | (Publisher | Subscription))

logger = getLogger(__name__)


class LegendManager:
    """Class that manages legend in Bokeh figure."""

    def __init__(self) -> None:
        self._legend_count_map: dict[str, int] = defaultdict(int)
        self._legend_items: list[tuple[str, list[GlyphRenderer]]] = []
        self._legend: dict[Any, str] = {}

    def add_legend(
        self,
        target_object: Any,
        renderer: GlyphRenderer | list[GlyphRenderer],
        legend_word: str | None = None,
    ) -> None:
        """
        Store a legend of the input object internally.

        Parameters
        ----------
        target_object : Any
            Instance of any class.
        renderer : bokeh.models.GlyphRenderer
            Instance of renderer.
        legend_word : str | None
            Sentence of the legend.
            If None, the class name of
            the target object is used.

        """
        label = self.get_label(target_object, legend_word)
        if not isinstance(renderer, list):
            renderer = [renderer]
        self._legend_items.append((label, renderer))
        self._legend[target_object] = label

    def create_legends(
        self,
        max_legends: int = 20,
        full_legends: bool = False,
        location: str = 'top_right',
        separate: int | None = None
    ) -> list[Legend]:
        """
        Create legends.

        Parameters
        ----------
        max_legends : int, optional
            Maximum number of legends to display, by default 20.
        full_legends : bool, optional
            Display all legends even if they exceed max_legends, by default False.
        location : str
            Specify the position where you want the legend to be displayed.
            Specify bottom_left only if you want it to appear at the bottom left.
        separate : int | None
            Maximum number of legends to display on one column.

        Returns
        -------
        list[Legend]
            List of Legend instances separated by location argument.

        """
        if separate is not None:
            separate_num = separate
        elif location == 'top_right':
            separate_num = 10
        else:
            separate_num = len(self._legend_items)

        legends: list[Legend] = []
        for i in range(0, len(self._legend_items)+separate_num, separate_num):
            if not full_legends and i >= max_legends and len(self._legend_items) > i:
                logger.warning(
                    f'The maximum number of legends drawn by default is {max_legends}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            legends.append(Legend(items=self._legend_items[i:i+separate_num], location=location))
        return legends

    def get_label(
        self,
        target_object: Any,
        word: str | None = None,
    ) -> str:
        """
        Get label name of target object.

        Parameters
        ----------
        target_object : Any
            Target object.
        word : str | None
            Sentence of the legend.
            If None, the class name of
            the target object is used.

        Returns
        -------
        str
            Label name of target object.

        """
        if word is not None:
            label = word
            self._legend[target_object] = label
            return label

        if target_object in self._legend:
            return self._legend[target_object]

        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._legend_count_map[class_name]}'
        self._legend_count_map[class_name] += 1
        self._legend[target_object] = label

        return label
