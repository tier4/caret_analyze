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

from abc import ABCMeta, abstractmethod
from logging import getLogger
from typing import Any

from bokeh import __version__ as bokeh_version
from bokeh.models import HoverTool

from packaging import version

from .....exceptions import InvalidArgumentError
from .....runtime import CallbackBase, Communication, Path, Publisher, Subscription

TargetTypes = (CallbackBase | Communication | Path | (Publisher | Subscription)) | Path

logger = getLogger(__name__)


class HoverKeysFactory:
    """Factory class to create an instance of HoverKeysBase."""

    _SUPPORTED_GRAPH_TYPE = [
        'callback_scheduling_bar',
        'callback_scheduling_rect',
        'timeseries',
        'stacked_bar',
        'message_flow_line',
        'message_flow_rect'
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

        elif graph_type == 'message_flow_line':
            hover_keys = MessageFlowLineKeys(target_object)

        elif graph_type == 'message_flow_rect':
            hover_keys = MessageFlowRectKeys(target_object)

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
    def to_list(self) -> list[str]:
        """
        Return all hover keys as a list of strings.

        Returns
        -------
        list[str]
            all hover keys

        Raises
        ------
        NotImplementedError
            This module is not implemented.

        """
        raise NotImplementedError()

    def create_hover(self, options: dict[str, Any] = {}) -> HoverTool:
        """
        Create HoverTool based on the hover keys.

        Parameters
        ----------
        options : dict[str, Any], optional
            Additional options, by default {}

        Returns
        -------
        HoverTool
            Created HoverTool

        """
        tips_str = '<div style="width:400px; word-wrap: break-word;">'
        for k in self.to_list():
            if k == 'y':
                tips_str += f'response time = @{k} <br>'
            else:
                tips_str += f'@{k} <br>'
        tips_str += '</div>'

        if version.parse(bokeh_version) >= version.parse('3.4.0'):
            return HoverTool(
                tooltips=tips_str, point_policy='follow_mouse', visible=False, **options
            )
        else:
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

    def to_list(self) -> list[str]:
        """
        Get CallbackSchedBarKeys.

        Returns
        -------
        list[str]
            Key lists

        """
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

    def to_list(self) -> list[str]:
        """
        Get CallbackSchedRectKeys.

        Returns
        -------
        list[str]
            Key lists

        """
        return ['legend_label', 'callback_start', 'callback_end', 'latency']


class TimeSeriesKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, (CallbackBase,
                                          Communication,
                                          Publisher,
                                          Subscription,
                                          Path)):
            raise InvalidArgumentError(
                "'target_object' must be [CallbackBase/Communication/Publisher/Subscription/Path]"
                'in timeseries graph.'
            )

    def to_list(self) -> list[str]:
        """
        Get TimeSeriesKeys.

        Returns
        -------
        list[str]
            Key lists

        """
        hover_keys: list[str]
        if isinstance(self._target_object, CallbackBase):
            hover_keys = ['legend_label', 'node_name', 'callback_name', 'callback_type',
                          'callback_param', 'symbol']
        elif isinstance(self._target_object, Communication):
            hover_keys = ['legend_label', 'topic_name',
                          'publish_node_name', 'subscribe_node_name']
        elif isinstance(self._target_object, (Publisher, Subscription)):
            hover_keys = ['legend_label', 'node_name', 'topic_name']
        elif isinstance(self._target_object, Path):
            hover_keys = ['legend_label', 'node_names', 'y']

        return hover_keys


class MessageFlowLineKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, Path):
            raise InvalidArgumentError("'target_object' must be Path in message flow.")

    def to_list(self) -> list[str]:
        """
        Get MessageFlowLineKeys.

        Returns
        -------
        list[str]
            Key lists

        """
        return ['t_start', 't_end', 'latency', 't_offset', 'index']


class MessageFlowRectKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, Path):
            raise InvalidArgumentError("'target_object' must be Path in message flow.")

    def to_list(self) -> list[str]:
        """
        Get MessageFlowRectKeys.

        Returns
        -------
        list[str]
            Key lists

        """
        return ['t_start', 't_end', 'latency', 't_offset',
                'callback_type', 'callback_param', 'symbol']


class StackedBarKeys(HoverKeysBase):

    def __init__(self, target_object: TargetTypes) -> None:
        super().__init__(target_object)

    def _validate(self, target_object: Any) -> None:
        if not isinstance(target_object, Path):
            raise InvalidArgumentError("'target_object' must be Path in stacked bar graph.")

    def to_list(self) -> list[str]:
        """
        Get StackedBarKeys.

        Returns
        -------
        list[str]
            Key lists

        """
        return ['label', 'latency']


class HoverSource:
    """Hover source."""

    def __init__(self, hover_keys: HoverKeysBase) -> None:
        self._hover_keys = hover_keys

    def generate(
        self,
        target_object: Any,
        additional_hover_dict: dict[str, str] | None = None
    ) -> dict[str, str]:
        """
        Generate hover source for ColumnDataSource.

        Parameters
        ----------
        target_object : Any
            target object
        additional_hover_dict : dict[str, str] | None, optional
            values corresponding to HoverKeys when you enter the hover values yourself,
            by default None

        Returns
        -------
        dict[str, str]
            hover source

        """
        hover_values: dict[str, Any] = {}
        for k in self._hover_keys.to_list():
            if hasattr(target_object, k):
                hover_values[k] = [f'{k} = {getattr(target_object, k)}']
            elif additional_hover_dict and k in additional_hover_dict.keys():
                hover_values[k] = [additional_hover_dict[k]]

        return hover_values
