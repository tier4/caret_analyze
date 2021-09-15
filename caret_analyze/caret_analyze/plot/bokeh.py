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

from typing import Dict, List, Optional

from bokeh.io import save, show
from bokeh.models import CrosshairTool
from bokeh.palettes import Bokeh8
from bokeh.plotting import figure
import numpy as np
from bokeh.resources import CDN

from ..callback import SubscriptionCallback
from ..path import Path
from ..util import Util


def message_flow(
    path: Path, export_path: Optional[str] = None,
    granularity: Optional[str] = None, treat_drop_as_delay=True
):
    class ColumnNameParser:

        def __init__(self, column_name):
            split_text = column_name.split('/')
            tracepoint_name = split_text[-2]
            self.tracepoint_name = tracepoint_name[:-len('_timestamp')]
            self.unique_name = '/'.join(split_text[:-2])
            self.node_name = '/'.join(split_text[:-3])

    granularity = granularity or 'raw'
    assert granularity in ['raw', 'callback', 'node']

    fig = figure(plot_width=1000, plot_height=400)

    df = path.to_dataframe(treat_drop_as_delay=treat_drop_as_delay)

    # カラムの削除
    # 購読側のtopic_nameから扱うため、逆順で処理
    drop_columns = []
    if granularity in ['callback', 'node']:
        for column_name in df.columns:
            parser = ColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name
            if tracepoint_name not in ['on_data_available', 'callback_start', 'callback_end']:
                drop_columns.append(column_name)

    df.drop(drop_columns, axis=1, inplace=True)
    drop_columns = []

    if granularity == 'node':
        for i, column_name in enumerate(df.columns):
            parser = ColumnNameParser(column_name)
            tracepoint_name = parser.tracepoint_name

            is_next_same_node = False
            is_before_same_node = False
            if i+1 < len(df.columns):
                parser_ = ColumnNameParser(df.columns[i+1])
                is_next_same_node = parser.node_name == parser_.node_name
            if i-1 >= 0:
                parser_ = ColumnNameParser(df.columns[i-1])
                is_before_same_node = parser.node_name == parser_.node_name

            if is_next_same_node and is_before_same_node:
                drop_columns.append(column_name)
    df.drop(drop_columns, axis=1, inplace=True)

    def rename_columns(df, granularity):
        renames = {}
        if granularity == 'raw':
            topic_name = ''
            for column_name in df.columns[::-1]:
                parser = ColumnNameParser(column_name)
                tracepoint_name = parser.tracepoint_name
                unique_name = parser.unique_name

                callback = Util.find_one(
                    path.callbacks, lambda x: x.unique_name == unique_name)
                assert callback is not None
                if 'subscription_callback' in unique_name and tracepoint_name == 'callback_start':
                    assert isinstance(callback, SubscriptionCallback)
                    topic_name = callback.topic_name

                if tracepoint_name == 'on_data_available':
                    renames[column_name] = f'{topic_name}/{tracepoint_name}'
                elif tracepoint_name in ['dds_write', 'rcl_publish', 'rclcpp_publish',
                                         'rclcpp_intra_publish']:
                    renames[column_name] = f'{topic_name}/{tracepoint_name}'
                else:
                    renames[column_name] = f'{unique_name}/{tracepoint_name}'

        elif granularity == 'callback':
            for column_name in df.columns:
                parser = ColumnNameParser(column_name)
                if parser.tracepoint_name == 'callback_start':
                    renames[column_name] = parser.unique_name + ' [start]'
                if parser.tracepoint_name == 'callback_end':
                    renames[column_name] = parser.unique_name + ' [end]'
        elif granularity == 'node':
            for column_name in df.columns:
                parser = ColumnNameParser(column_name)
                if parser.tracepoint_name == 'callback_start':
                    renames[column_name] = parser.node_name + ' [start]'
                if parser.tracepoint_name == 'callback_end':
                    renames[column_name] = parser.node_name + ' [end]'
        df.rename(columns=renames, inplace=True)

    rename_columns(df, granularity)

    # カラムの圧縮
    y_list: List[int] = []
    column_names: Dict[int, str] = {}
    for i, column_name in enumerate(df.columns):
        if i == 0:
            y_list.append(0)
        else:
            y_list.append(y_list[-1]-1)
        column_names[y_list[-1]] = column_name

    y = np.array(y_list)
    for i, row in df.iterrows():
        x = row.values
        fig.line(
            x=x,  # X軸
            y=y,  # Y軸
            line_width=1.5,  # 線の幅
            line_color=Bokeh8[i % 8],  # 線の色
            line_alpha=1,  # 線の彩度
        )

    fig.yaxis.ticker = y
    tick_labels = {}
    for y_value in y_list:
        tick_labels[str(y_value)] = column_names[y_value]
    fig.yaxis.major_label_overrides = tick_labels

    fig.add_tools(CrosshairTool(line_alpha=0.4))

    if export_path is None:
        show(fig)
    else:
        save(fig, export_path, title='time vs tracepoint', resources=CDN)
