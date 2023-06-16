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

from collections.abc import Sequence

from .callback_scheduling_plot import CallbackSchedulingPlot
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...runtime import Application, CallbackGroup, Executor, Node, Path

CallbackGroupTypes = (Application | Executor | Path | Node |
                      CallbackGroup | Sequence[CallbackGroup])


class CallbackSchedulingPlotFactory:
    """Factory class to create an instance of CallbackSchedulingPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        target_objects: CallbackGroupTypes,
        visualize_lib: VisualizeLibInterface,
        lstrip_s: float = 0,
        rstrip_s: float = 0
    ) -> CallbackSchedulingPlot:
        """
        Create instance.

        Parameters
        ----------
        target_objects : CallbackGroupTypes
            CallbackGroupTypes = (Application | Executor | Path | Node |
                                  CallbackGroup | Sequence[CallbackGroup])
        visualize_lib : VisualizeLibInterface
            Instance of VisualizeLibInterface used for visualization.
        lstrip_s : float, optional
            Start time of cropping range, by default 0.
        rstrip_s: float, optional
            End point of cropping range, by default 0.

        Returns
        -------
        CallbackSchedulingPlot

        """
        return CallbackSchedulingPlot(target_objects, visualize_lib, lstrip_s, rstrip_s)
