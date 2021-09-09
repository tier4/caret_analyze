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

from typing import Optional
from ..path import Path
from ..callback import CallbackBase, SubscriptionCallback
from ..util import Util

import numpy as np
from bokeh.io import show
from bokeh.plotting import figure
from bokeh.palettes import Bokeh8
from bokeh.models import CrosshairTool


def message_flow(path: Path, granularity: Optional[str] = None):
    granularity = granularity or "raw"
    assert granularity in ["raw", "callback", "node"]

    fig = figure(plot_width=1000, plot_height=400)

    df = path.to_dataframe()
    renames = {}

    callback: Optional[CallbackBase] = None

    # 購読側のtopic_nameから扱うため、逆順で処理
    if granularity == "raw":
        topic_name = ""
        for column_name in df.columns[::-1]:
            split_text = column_name.split("/")
            tracepoint_name = split_text[-2]
            unique_name = "/".join(split_text[:-2])

            callback = Util.find_one(path.callbacks, lambda x: x.unique_name == unique_name)
            assert callback is not None
            if tracepoint_name == "on_data_available_timestamp":
                assert isinstance(callback, SubscriptionCallback)
                topic_name = callback.topic_name
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
            elif tracepoint_name == "dds_write_timestamp":
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
    elif granularity == "callback":
        topic_name = ""
        for column_name in df.columns[::-1]:
            split_text = column_name.split("/")
            tracepoint_name = split_text[-2]
            unique_name = "/".join(split_text[:-2])
            if tracepoint_name not in [
                "on_data_available_timestamp",
                "dds_write_timestamp",
                "callback_start_timestamp",
                "callback_end_timestamp",
            ]:
                df.drop(column_name, axis=1, inplace=True)

            callback = Util.find_one(path.callbacks, lambda x: x.unique_name == unique_name)
            assert callback is not None
            if tracepoint_name == "on_data_available_timestamp":
                assert isinstance(callback, SubscriptionCallback)
                topic_name = callback.topic_name
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
            elif tracepoint_name == "dds_write_timestamp":
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
            elif tracepoint_name in ["callback_start_timestamp", "callback_end_timestamp"]:
                renames[column_name] = unique_name
    elif granularity == "node":
        topic_name = ""
        columns_inverted = df.columns[::-1]
        for i, column_name in enumerate(columns_inverted):
            split_text = column_name.split("/")
            tracepoint_name = split_text[-2]
            unique_name = "/".join(split_text[:-2])
            node_name = "/".join(split_text[:-3])

            is_last_tracepoint_callback_end = False
            is_next_tracepoint_callback_start = False
            if i + 1 < len(columns_inverted):
                is_next_tracepoint_callback_start = (
                    columns_inverted[i + 1].split("/")[-2] == "callback_start_timestamp"
                )
            elif i - 1 >= 0:
                is_last_tracepoint_callback_end = (
                    columns_inverted[i - 1].split("/")[-2] == "callback_end_timestamp"
                )

            if (
                tracepoint_name
                not in [
                    "on_data_available_timestamp",
                    "dds_write_timestamp",
                    "callback_start_timestamp",
                    "callback_end_timestamp",
                ]
                or is_last_tracepoint_callback_end
                and tracepoint_name == "callback_start_timestamp"
                or is_next_tracepoint_callback_start
                and tracepoint_name == "callback_end_timestamp"
            ):
                df.drop(column_name, axis=1, inplace=True)

            callback = Util.find_one(path.callbacks, lambda x: x.unique_name == unique_name)
            assert callback is not None
            if tracepoint_name == "on_data_available_timestamp":
                assert isinstance(callback, SubscriptionCallback)
                topic_name = callback.topic_name
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
            elif tracepoint_name == "dds_write_timestamp":
                renames[column_name] = f"{topic_name}/{tracepoint_name}"
            elif tracepoint_name in ["callback_start_timestamp", "callback_end_timestamp"]:
                renames[column_name] = node_name

    df.rename(columns=renames, inplace=True)

    y = np.arange(len(df.T)) * -1
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
    for y_value, column_name in zip(y, df.columns):
        tick_labels[str(y_value)] = column_name
    fig.yaxis.major_label_overrides = tick_labels

    fig.add_tools(CrosshairTool(line_alpha=0.4))
    show(fig)
