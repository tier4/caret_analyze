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

from abc import ABCMeta, abstractmethod

from bokeh.plotting import Figure, save, show
from bokeh.resources import CDN

import pandas as pd


class PlotBase(metaclass=ABCMeta):

    @abstractmethod
    def to_dataframe(
        self,
        xaxis_type: str = 'system_time'
    ) -> pd.DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def figure(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> Figure:
        raise NotImplementedError()

    def show(
        self,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> None:
        p = self.figure(xaxis_type, ywheel_zoom, full_legends)
        show(p)

    def save(
        self,
        export_path: str,
        title: str,
        xaxis_type: str = 'system_time',
        ywheel_zoom: bool = True,
        full_legends: bool = False
    ) -> None:
        p = self.figure(xaxis_type, ywheel_zoom, full_legends)
        save(p, export_path, title=title, resources=CDN)
