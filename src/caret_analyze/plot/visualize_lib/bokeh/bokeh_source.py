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

from collections import defaultdict
from logging import getLogger
from typing import Any, Dict, List, Tuple

from bokeh.models import GlyphRenderer, Legend
from bokeh.plotting import Figure

logger = getLogger(__name__)


class LegendManager:
    """Class that manages legend in Bokeh figure."""

    def __init__(self) -> None:
        self._legend_count_map: Dict[str, int] = defaultdict(int)
        self._legend_items: List[Tuple[str, List[GlyphRenderer]]] = []
        self._legend: Dict[Any, str] = {}

    def add_legend(
        self,
        target_object: Any,
        renderer: GlyphRenderer
    ) -> None:
        """
        Store a legend of the input object internally.

        Parameters
        ----------
        target_object : Any
            Instance of any class.
        renderer : bokeh.models.GlyphRenderer
            Instance of renderer.

        """
        label = self.get_label(target_object)
        self._legend_items.append((label, [renderer]))
        self._legend[target_object] = label

    def draw_legends(
        self,
        figure: Figure,
        max_legends: int = 20,
        full_legends: bool = False
    ) -> None:
        """
        Add legends to the input figure.

        Parameters
        ----------
        figure : Figure
            Target figure.
        max_legends : int, optional
            Maximum number of legends to display, by default 20.
        full_legends : bool, optional
            Display all legends even if they exceed max_legends, by default False.

        """
        for i in range(0, len(self._legend_items)+10, 10):
            if not full_legends and i >= max_legends:
                logger.warning(
                    f'The maximum number of legends drawn by default is {max_legends}. '
                    'If you want all legends to be displayed, '
                    'please specify the `full_legends` option to True.'
                )
                break
            figure.add_layout(Legend(items=self._legend_items[i:i+10]), 'right')

        figure.legend.click_policy = 'hide'

    def get_label(self, target_object: Any) -> str:
        if target_object in self._legend:
            return self._legend[target_object]

        class_name = type(target_object).__name__
        label = f'{class_name.lower()}{self._legend_count_map[class_name]}'
        self._legend_count_map[class_name] += 1
        self._legend[target_object] = label

        return label
