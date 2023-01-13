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
from typing import Sequence, Union

from ..plot_base import PlotBase
from ..visualize_lib import VisualizeLibInterface
from ...runtime import Application, CallbackGroup, Executor, Node, Path

CallbackGroupTypes = Union[Application, Executor,
                           Path, Node, CallbackGroup, Sequence[CallbackGroup]]

logger = getLogger(__name__)


class CallbackSchedulingPlot(PlotBase):
    """Class that provides API for callback scheduling plot."""

    def __init__(
        self,
        target_objects: CallbackGroupTypes,
        visualize_lib: VisualizeLibInterface,
        lstrip_s: float,
        rstrip_s: float
    ) -> None:
        self._visualize_lib = visualize_lib
        self._callback_groups = self._get_callback_groups(target_objects)
        self._lstrip_s = lstrip_s
        self._rstrip_s = rstrip_s
