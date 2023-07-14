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

from .histogram import ResponseTimeHistPlot
from ..visualize_lib import VisualizeLibInterface
from ...common import type_check_decorator
from ...exceptions import UnsupportedTypeError
from ...runtime import Path


class ResponseTimeHistPlotFactory:
    """Factory class to create an instance of ResponseTimeHistPlot."""

    @staticmethod
    @type_check_decorator
    def create_instance(
        visualize_lib: VisualizeLibInterface,
        target: Sequence[Path],
        case: str = 'best-to-worst',
        binsize_ns: int = 10000000,
    ) -> ResponseTimeHistPlot:
        if case not in ['best-to-worst', 'best', 'worst']:
            raise UnsupportedTypeError(
                f'Unsupported "case". case = {case}.'
                'supported "case": [best-to-worst/best/worst]'
            )

        return ResponseTimeHistPlot(visualize_lib, target, case, binsize_ns)
