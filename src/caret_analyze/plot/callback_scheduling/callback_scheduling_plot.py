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
from typing import List, Sequence, Union

from bokeh.plotting import Figure

import pandas as pd

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...common import UniqueList
from ...exceptions import InvalidArgumentError, UnsupportedTypeError
from ...runtime import Application, CallbackGroup, Executor, Node, Path

CallbackGroupTypes = Union[Application, Executor,
                           Path, Node, CallbackGroup, Sequence[CallbackGroup]]

logger = getLogger(__name__)


class CallbackSchedulingPlot(PlotBase):

    def __init__(
        self,
        target_objects: CallbackGroupTypes,
        visualize_lib: VisualizeLibInterface,
    ) -> None:
        self._target_objects = target_objects
        self._visualize_lib = visualize_lib
        self._callback_groups = self._get_callback_groups(self._target_objects)

    def to_dataframe(self, xaxis_type: str = 'system_time') -> pd.DataFrame:
        logger.warning("'to_dataframe' method is not implemented in CallbackSchedulingPlot.")
        return pd.DataFrame()

    def figure(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False,
        coloring_rule: str = 'callback',
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> Figure:
        self._validate_xaxis_type(xaxis_type)
        if coloring_rule not in ['callback', 'callback_group', 'node']:
            raise UnsupportedTypeError(
                f'Unsupported coloring_rule. coloring_rule = {coloring_rule}. '
                'supported coloring_rule: [callback/callback_group/node]'
            )

        return self._visualize_lib.callback_scheduling(
            self._callback_groups, xaxis_type, ywheel_zoom, full_legends,
            coloring_rule, lstrip_s, rstrip_s
        )

    @staticmethod
    def _get_callback_groups(target_objects: CallbackGroupTypes) -> List[CallbackGroup]:
        callback_groups: List[CallbackGroup]

        if isinstance(target_objects, (Application, Executor, Node)):
            if target_objects.callback_groups is None:
                raise InvalidArgumentError('target.callback_groups is None')
            callback_groups = target_objects.callback_groups

        elif isinstance(target_objects, Path):
            _callback_groups = UniqueList()
            for comm in target_objects.communications:
                for cbg in comm.publish_node.callback_groups or []:
                    _callback_groups.append(cbg)
            for cbg in target_objects.communications[-1].subscribe_node.callback_groups or []:
                _callback_groups.append(cbg)
            if len(_callback_groups) == 0:
                raise InvalidArgumentError('target.callback_groups is None')
            callback_groups = _callback_groups.as_list()

        elif isinstance(target_objects, CallbackGroup):
            callback_groups = [target_objects]

        else:  # Sequence[CallbackGroup]
            callback_groups = list(target_objects)

        return callback_groups

    def _validate_xaxis_type(self, xaxis_type: str) -> None:
        if xaxis_type not in ['system_time', 'sim_time']:
            raise UnsupportedTypeError(
                f'Unsupported xaxis_type. xaxis_type = {xaxis_type}. '
                'supported xaxis_type: [system_time/sim_time]'
            )
