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

from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Any, Dict, List, Optional, Union

from bokeh.models import HoverTool

from .....exceptions import InvalidArgumentError
from .....runtime import (CallbackBase, Communication, Path, Publisher, Subscription,
                          SubscriptionCallback, TimerCallback)

TargetTypes = Union[CallbackBase, Communication, Path, Union[Publisher, Subscription]]

logger = getLogger(__name__)


class HoverKeysFactory:
    """Factory class to create an instance of HoverKeysBase."""

    _SUPPORTED_GRAPH_TYPE = [
        'callback_scheduling_bar',
        'callback_scheduling_rect',
        'timeseries',
        'stacked_bar',
        'message_flow'
    ]

    @staticmethod
    def create_instance(graph_type: str, target_object: TargetTypes) -> HoverKeysBase:
        """
        Create HoverKeysBase instance.

        Parameters
        ----------
        graph_type : str
            graph type to be plotted.
        target_object : TargetTypes
            target object.

        Returns
        -------
        HoverKeysBase
            HoverKeys instances according to graph type.

        Raises
        ------
        InvalidArgumentError
            graph type not in
            [callback_scheduling_bar/callback_scheduling_rect/timeseries/stacked_bar/message_flow]

        """
        # Validate
        if graph_type not in HoverKeysFactory._SUPPORTED_GRAPH_TYPE:
            raise InvalidArgumentError(
                f"'graph_type' must be [{'/'.join(HoverKeysFactory._SUPPORTED_GRAPH_TYPE)}]."
            )

        # Create HoverKeys instance
        hover_keys: HoverKeysBase
        if graph_type == 'callback_scheduling_bar':
            hover_keys = CallbackSchedBarKeys(target_object)

        elif graph_type == 'callback_scheduling_rect':
            hover_keys = CallbackSchedRectKeys(target_object)

        elif graph_type == 'timeseries':
            hover_keys = TimeSeriesKeys(target_object)

        elif graph_type == 'stacked_bar':
            hover_keys = StackedBarKeys(target_object)

        elif graph_type == 'message_flow':
            hover_keys = MessageFlowKeys(target_object)

        return hover_keys


class HoverKeysBase(metaclass=ABCMeta):
    """Base class for HoverKeys."""

    def __init__(self, target_object: TargetTypes) -> None:
        self._validate(target_object)
        self._target_object = target_object

    @abstractmethod
    def _validate(self, target_object: Any) -> None:
        """
        Validate target object.

        Parameters
        ----------
        target_object : Any
            target object

        Raises
        ------
        InvalidArgumentError
            type of target object is invalid

        """
        raise NotImplementedError()

    @abstractmethod
    def to_list(self) -> List[str]:
        """
        Return all hover keys as a list of strings.

        Returns
        -------
        List[str]
            all hover keys

        """
        raise NotImplementedError()

    def create_hover(self, options: Dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the hover keys.

        Parameters
        ----------
        options : Dict[str, Any], optional
            Additional options, by default {}

        Returns
        -------
        HoverTool

        """
        tips_str = '<div style="width:400px; word-wrap: break-word;">'
        for k in self.to_list():
            tips_str += f'@{k} <br>'
        tips_str += '</div>'

        return HoverTool(
            tooltips=tips_str, point_policy='follow_mouse', toggleable=False, **options
        )


class CallbackSchedBarKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, CallbackBase):
            raise InvalidArgumentError(
                "'target_object' must be CallbackBase in callback scheduling graph."
            )

    def to_list(self) -> List[str]:
        return ['legend_label', 'node_name', 'callback_name',
                'callback_type', 'callback_param', 'symbol']


class CallbackSchedRectKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, CallbackBase):
            raise InvalidArgumentError(
                "'target_object' must be CallbackBase in callback scheduling graph."
            )

    def to_list(self) -> List[str]:
        return ['legend_label', 'callback_start', 'callback_end', 'latency']


class TimeSeriesKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, (CallbackBase, Communication, Publisher, Subscription)):
            raise InvalidArgumentError(
                "'target_object' must be [CallbackBase/Communication/Publisher/Subscription]"
                'in timeseries graph.'
            )

    def to_list(self) -> List[str]:
        hover_keys: List[str]
        if isinstance(self._target_object, CallbackBase):
            hover_keys = ['legend_label', 'node_name', 'callback_name', 'callback_type',
                          'callback_param', 'symbol']
        elif isinstance(self._target_object, Communication):
            hover_keys = ['legend_label', 'topic_name',
                          'publish_node_name', 'subscribe_node_name']
        elif isinstance(self._target_object, (Publisher, Subscription)):
            hover_keys = ['legend_label', 'node_name', 'topic_name']

        return hover_keys


class MessageFlowKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, Path):
            raise InvalidArgumentError("'target_object' must be Path in message flow.")

    def to_list(self) -> List[str]:
        return ['t_start', 't_end', 'latency', 't_offset', 'desc']


class StackedBarKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, Path):
            raise InvalidArgumentError("'target_object' must be Path in stacked bar graph.")

    def to_list(self) -> List[str]:
        return ['path_name']


class HoverSource:
    """Hover source."""

    def __init__(self, hover_keys: HoverKeysBase) -> None:
        self._hover_keys = hover_keys

    def generate(
        self,
        target_object: Any,
        additional_hover_dict: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        """
        Generate hover source for ColumnDataSource.

        Parameters
        ----------
        target_object : Any
            target object
        additional_hover_dict : Optional[Dict[str, str]], optional
            values corresponding to HoverKeys when you enter the hover values yourself,
            by default None

        Returns
        -------
        Dict[str, str]
            hover source

        """
        hover_values: Dict[str, Any] = {}
        for k in self._hover_keys.to_list():
            if hasattr(target_object, k):
                hover_values[k] = [f'{k} = {getattr(target_object, k)}']
            elif additional_hover_dict and k in additional_hover_dict.keys():
                hover_values[k] = [additional_hover_dict[k]]
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

        else:
            raise NotImplementedError()

        return description
