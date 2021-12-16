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
from bokeh.palettes import d3
from typing import Dict, List, Optional
from bokeh.application.application import Callback
from abc import ABCMeta, abstractmethod
import numpy as np
import pandas as pd
import  bokeh as bokeh
from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.plotting import ColumnDataSource, figure
from bokeh.models import DataSource,RangeTool,HoverTool
from bokeh.io import curdoc, show
from bokeh.palettes import Paired8 as colors
from ...exceptions import InvalidArgumentError
from bokeh.palettes import Bokeh8
from bokeh.resources import CDN
from ...record.data_frame_shaper import Clip, Strip
from ...runtime.node import Node
from ...runtime.executor import Executor
from ...runtime.callback_group import CallbackGroup
from ...architecture import Architecture
from typing import  Union


def callback_sched(
    target: Union[Node,CallbackGroup,Executor],
    lstrip_s: float=0,
    rstrip_s: float=0):
     if isinstance(target, Node):
        callback_sched_node(target,lstrip_s,rstrip_s)
     elif isinstance(target, CallbackGroup):
        callback_sched_cbg(target,lstrip_s,rstrip_s)
     elif isinstance(target, Executor):
        callback_sched_exec(target,lstrip_s,rstrip_s)
        
# モジュール内では以下を定義
def callback_sched_node(target:Node,lstrip_s,rstrip_s):
    node_name=target.node_name
     # get dataframes for callback
    dataframes_cb = []
    dataframes_cb=get_dataframe(target,dataframes_cb)
    # print(type(dataframes))
    # clip = None
    # if lstrip_s > 0 or rstrip_s > 0:
    #     strip = Strip(lstrip_s, rstrip_s)
    #     clip = strip.to_clip(dataframes_cb)
    #     dataframes_cb = clip.execute(dataframes_cb)
    sched_plot(node_name,target,dataframes_cb)
    
def callback_sched_cbg(target:CallbackGroup,lstrip_s,rstrip_s):
    cbg_name=target.callback_group_name
     # get dataframes for callback
    dataframes_cb = []
    dataframes_cb=get_dataframe(target, dataframes_cb)
    sched_plot(cbg_name,target,dataframes_cb)

def callback_sched_exec(target:Executor,lstrip_s,rstrip_s):
    executor_name=target.executor_name
    # get dataframes for callback
    dataframes_cb = []
    dataframes_cb=get_dataframe(target, dataframes_cb)
    sched_plot_cbg(executor_name,target, dataframes_cb)


def get_dataframe(target,dataframe):
    
    for callback in target.callbacks:
        dataframe.append(callback.to_dataframe())
    return dataframe

def get_start_and_end(dataframe):
    startpoints = []
    endpoints = []
    for item in dataframe.itertuples():
        startpoints.append(item._1*1.0e-6)
        endpoints.append(item._2*1.0e-6)
    return startpoints, endpoints

def sched_plot(target_name:str, target, dataframe):
    colors = d3["Category20"][20]
    p = figure(x_axis_label='Time [ms]', y_axis_label='', title=f'Time-line of callbacks in {target_name}', width=1200)

    top = .0
    bottom = top - 0.2
    counter = 0
    for callback, df_cb in zip(target.callbacks, dataframe):
        startpoints, endpoints = get_start_and_end(df_cb)
        p.quad(left=startpoints, right=endpoints,
           top=top, bottom=bottom,
           color=colors[counter],
           legend_label=f"{callback.callback_name}")
        counter += 1
        top -= 0.3
        bottom = top - 0.2
    p.yaxis.visible = False
    p.legend.location = "bottom_left"
    p.legend.click_policy="hide"
    p.add_layout(p.legend[0], "right") # 凡例をグラフの外に出す（右側）

    show(p)
    
def sched_plot_cbg(target_name:str, target, dataframe):
    colors = d3["Category20"][20]
    p = figure(x_axis_label='Time [ms]', y_axis_label='', title=f'Time-line of callbacks in {target_name}', width=1200)
    
    top = .0
    bottom = top - 0.2
    counter = 0

    for callback_group in target.callback_groups:
        for callback, df_cb in zip(callback_group.callbacks, dataframe):
            startpoints, endpoints = get_start_and_end(df_cb)
            p.quad(left=startpoints, right=endpoints,
                top=top, bottom=bottom,
                color=colors[counter],
                legend_label=f"{callback.callback_name} ({callback_group.callback_group_name})")
            top -= 0.3
            bottom = top - 0.2
        counter += 1
        
    p.yaxis.visible = False
    p.legend.location = "bottom_left"
    p.legend.click_policy="hide"
    p.add_layout(p.legend[0], "right") # 凡例をグラフの外に出す（右側）
    
    show(p)
    
