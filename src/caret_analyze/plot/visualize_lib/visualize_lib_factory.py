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

from .bokeh import Bokeh
from .visualize_lib_interface import VisualizeLibInterface
from ...exceptions import UnsupportedTypeError


class VisualizeLibFactory:
    """Factory class to create an instance of VisualizeLibInterface."""

    @staticmethod
    def create_instance(use_package: str = 'bokeh') -> VisualizeLibInterface:
        """
        Create an instance of VisualizeLibInterface.

        Parameters
        ----------
        use_package : str, optional
            Library used for visualization, by default 'bokeh'.

        Returns
        -------
        VisualizeLibInterface
            Created instance of VisualizeLibInterface.

        Raises
        ------
        NotImplementedError
            Specified use_package is not implemented.
        UnsupportedTypeError
            Argument use_package is not "bokeh" or "graphviz".

        """
        if use_package == 'bokeh':
            return Bokeh()
        elif use_package == 'graphviz':
            raise NotImplementedError()
        else:
            raise UnsupportedTypeError(
                'Unsupported use_package specified. '
                'Supported use_package: [bokeh/graphviz]'
            )
