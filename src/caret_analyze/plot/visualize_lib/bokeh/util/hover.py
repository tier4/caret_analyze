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

from logging import getLogger
from typing import Any, Dict, List, Union

from bokeh.models import HoverTool

from .legend import LegendManager
from .....exceptions import InvalidArgumentError
from .....runtime import (CallbackBase, Communication, Path, Publisher, Subscription,
                          SubscriptionCallback, TimerCallback)

TargetTypes = Union[CallbackBase, Communication, Path, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class HoverKeys:
    """Hover keys."""

    _SUPPORTED_GRAPH_TYPE = [
        'callback_scheduling_bar',
        'callback_scheduling_rect',
        'timeseries',
        'stacked_bar',
        'message_flow'
    ]

    def __init__(self, graph_type: str, target_object: TargetTypes) -> None:
        self._validate(graph_type, target_object)
        self._graph_type = graph_type
        self._target_object = target_object

    def _validate(self, graph_type: str, target_object: Any) -> None:
        if graph_type not in self._SUPPORTED_GRAPH_TYPE:
            raise InvalidArgumentError(
                f"'graph_type' must be [{'/'.join(self._SUPPORTED_GRAPH_TYPE)}]."
            )

        if graph_type == 'callback_scheduling' and not isinstance(target_object, CallbackBase):
            raise InvalidArgumentError(
                "'target_object' must be CallbackBase in callback scheduling graph."
            )

        if (graph_type == 'timeseries' and not isinstance(
                target_object, (CallbackBase, Communication, Publisher, Subscription))):
            raise InvalidArgumentError(
                "'target_object' must be [CallbackBase/Communication/Publisher/Subscription]"
                'in timeseries graph.'
            )

        if graph_type == 'message_flow' and not isinstance(target_object, Path):
            raise InvalidArgumentError(
                "'target_object' must be Path in message flow.")

        if (graph_type == 'stacked_bar' and not isinstance(target_object, Path)):
            raise InvalidArgumentError(
                "'target_object' must be Path in stacked bar graph."
            )

    def to_list(self) -> List[str]:
        """
        Get hover keys as a list.

        Returns
        -------
        List[str]
            Hover keys.

        """
        if self._graph_type == 'callback_scheduling_bar':
            hover_keys = ['legend_label', 'node_name', 'callback_name',
                          'callback_type', 'callback_param', 'symbol']

        if self._graph_type == 'callback_scheduling_rect':
            hover_keys = ['legend_label', 'callback_start', 'callback_end', 'latency']

        if self._graph_type == 'timeseries':
            if isinstance(self._target_object, CallbackBase):
                hover_keys = ['legend_label', 'node_name', 'callback_name', 'callback_type',
                              'callback_param', 'symbol']
            elif isinstance(self._target_object, Communication):
                hover_keys = ['legend_label', 'topic_name',
                              'publish_node_name', 'subscribe_node_name']
            elif isinstance(self._target_object, (Publisher, Subscription)):
                hover_keys = ['legend_label', 'node_name', 'topic_name']

        if self._graph_type == 'message_flow':
            hover_keys = ['t_start', 't_end', 'latency', 't_offset', 'desc']

        if self._graph_type == 'stacked_bar':
            hover_keys = ['legend_label', 'path_name']

        return hover_keys


class HoverCreator:
    """Class to create HoverTool for bokeh graph."""

    def __init__(self, hover_keys: HoverKeys) -> None:
        self._hover_keys = hover_keys

    def create(self, options: dict = {}) -> HoverTool:
        """
        Create HoverTool based on the hover keys.

        Parameters
        ----------
        options : dict, optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
        tips_str = '<div style="width:400px; word-wrap: break-word;">'
        for k in self._hover_keys.to_list():
            tips_str += f'@{k} <br>'
        tips_str += '</div>'

        return HoverTool(
            tooltips=tips_str, point_policy='follow_mouse', toggleable=False, **options
        )


class HoverSource:
    """Hover source."""

    def __init__(self, legend_manager: LegendManager, hover_keys: HoverKeys) -> None:
        self._legend_manager = legend_manager
        self._hover_keys = hover_keys

    def generate(self, target_object: Any) -> Dict[str, str]:
        hover_values: Dict[str, Any] = {}
        for k in self._hover_keys.to_list():
            if hasattr(target_object, k):
                hover_values[k] = [f'{k} = {getattr(target_object, k)}']
            else:
                hover_values[k] = [self.get_non_property_data(target_object, k)]

        return hover_values

    def get_non_property_data(self, target_object: Any, key: str) -> str:
        """
        Get non-property data from target object.

        Parameters
        ----------
        target_object : Any
            Target object.
        key : str
            Hover key.

        Returns
        -------
        str
            Non-property data.

        Raises
        ------
        NotImplementedError
            'key' not in [callback_param/legend_label].

        """
        if key == 'callback_param':
            if isinstance(target_object, TimerCallback):
                description = f'period_ns = {target_object.period_ns}'
            elif isinstance(target_object, SubscriptionCallback):
                description = f'subscribe_topic_name = {target_object.subscribe_topic_name}'

        elif key == 'legend_label':
            label = self._legend_manager.get_label(target_object)
            description = f'legend_label = {label}'

        else:
            raise NotImplementedError()

        return description
