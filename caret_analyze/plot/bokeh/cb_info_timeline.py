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

import pandas_bokeh
from bokeh.plotting import figure, show
from bokeh.palettes import d3


def show_cb_info_df_timeline(cb_info_df, title='', use_timestamp=True):
    colors = d3['Category20'][20]
    p = figure(height=300, width=1000, title=title)
    
    if(use_timestamp):
        for i, callback_name in enumerate(cb_info_df.columns.get_level_values(0).to_list()):
            p.line('timestamp',
                   'value', 
                   source = cb_info_df.loc[:, (callback_name,)],
                   line_color = colors[i],
                   legend_label = callback_name)
    else:
        for i, callback_name in enumerate(cb_info_df.columns.get_level_values(0).to_list()):
            p.line(x = cb_info_df.index,
                   y = cb_info_df.loc[:, (callback_name, 'value')].to_list(),
                   line_color = colors[i],
                   legend_label = callback_name)
        
    p.add_layout(p.legend[0], 'right')
    show(p)