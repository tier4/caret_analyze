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

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.palettes import Bokeh8
from bokeh.plotting import ColumnDataSource, figure
from bokeh.resources import CDN
from bokeh.io import curdoc, show
from bokeh.palettes import Paired8 as colors
from ...exceptions import InvalidArgumentError


def callback_sched(app: Architecture,
                   target_name: str = None,
                   type: str= None,
                   show_callback_group:Optional[bool]=False,
                   duration_s: Optional[float] = None):
     if type == 'node':
        callback_sched_node(app,target_name, duration_s)
     elif type == 'cbg':
        callback_sched_cbg(app,target_name, duration_s)
     elif type == 'executor':
        callback_sched_exec(app,target_name,show_callback_group,duration_s)
     else:
        raise InvalidArgumentError('type must be [ node / cbg / executor ]')

    # # 以下のように、型を見て分岐するイメージ。
	# if isinstance(target, Node):
    #      callback_sched_node(target, duration)
    # # elif isinstance(target, CallbackGroup):
    # #      callback_sched_cbg(target, duration)
    # # else: isinstance(target, Executor):
    # #      callback_sched_exec(target, duration)

# モジュール内では以下を定義
def callback_sched_node(app,target_name, duration_s: Optional[float] = None):
    target_node=app.get_node(target_name)
    dataframes_cb = []
    dataframes_cb=get_dataframe(target_node,dataframes_cb)
    sched_plot(target_name,target_node,dataframes_cb)
    
def callback_sched_cbg(app,target_name, duration_s: Optional[float] = None):
    target_cbg=app.get_callback_group(target_name) 
     # get dataframes for callback
    dataframes_cb = []
    dataframes_cb=get_dataframe(target_cbg,dataframes_cb)
    sched_plot(target_name,target_cbg,dataframes_cb)

def callback_sched_exec(app,target_name,show_callback_group:Optional[bool]=False, duration_s: Optional[float] = None):
    executor = app.get_executor(target_name) # executorの取得
    # get dataframes for callback
    dataframes_cb = []
    dataframes_cb=get_dataframe(executor,dataframes_cb)
    if show_callback_group:
        sched_plot_cbg(target_name,executor,dataframes_cb)
    else:
        sched_plot(target_name,executor,dataframes_cb)


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

def sched_plot(target_name,target,dataframe,duration_s: Optional[float] = None):
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
    
def sched_plot_cbg(target_name,target,dataframe,duration_s: Optional[float] = None):
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
    
