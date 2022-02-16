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

from ...runtime import Application, Executor, Node, CallbackGroup, CallbackBase
from typing import Union, List
import pandas as pd


def concate_cb_latency_table(target: Union[Application, Executor, Node, CallbackGroup, List[CallbackBase]]) -> pd.DataFrame:
    callbacks = []
    if(type(target) == list):
        callbacks = target 
    else:
        callbacks = target.callbacks
    
    callbacks_latency_table = pd.DataFrame()
    for callback in callbacks:
        callback_latency_table = callback.to_dataframe() * 10**(-9)  # [ns] -> [s]
        callbacks_latency_table = pd.concat([callbacks_latency_table, callback_latency_table], axis=1)
    
    return callbacks_latency_table


def get_cb_freq_with_timestamp(timestamp_df, initial_timestamp, callback_name='') -> pd.DataFrame:
    timestamp_list = []
    frequency_list = []
    diff_base = -1

    for timestamp in timestamp_df[callback_name].dropna():
        diff = (timestamp - initial_timestamp).tolist()
        if int(diff) == diff_base:
            frequency_list[-1] += 1
        else:
            for i in range(int(diff) - diff_base):
                timestamp_list.append(initial_timestamp + len(timestamp_list))
                frequency_list.append(1)
            diff_base = int(diff)
            
    return timestamp_list, frequency_list


def get_preprocessing_frequency(timestamp_df, *initial_timestamp) -> pd.DataFrame:
    frequency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([timestamp_df.columns, ['timestamp', 'value']]))
    
    if len(initial_timestamp) != len(timestamp_df.columns):
        if len(initial_timestamp) == 1:
            initial_timestamp = [initial_timestamp[0] for i in range(len(timestamp_df.columns))]
        else:
            initial_timestamp = [timestamp_df.iloc(0).mean for i in range(len(timestamp_df.columns))]
    
    for initial, callback_name in zip(initial_timestamp, timestamp_df.columns):
        timestamp, value = get_cb_freq_with_timestamp(timestamp_df, initial, callback_name)
        frequency_df[(callback_name, 'timestamp')] = timestamp              
        frequency_df[(callback_name, 'value')] = value
    
    return frequency_df


def get_callback_frequency_df(target: Union[Application, Executor, Node, CallbackGroup, List[CallbackBase]]) -> pd.DataFrame:
    latency_table = concate_cb_latency_table(target)
    latency_table = latency_table.loc[:, latency_table.columns.str.contains('callback_start_timestamp')]
    latency_table.columns = [c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
    
    earliest_timestamp = latency_table.iloc[0].min()  # get earlist time of the target callback timestamp (Optional)
    callback_frequency_df = get_preprocessing_frequency(latency_table, earliest_timestamp)
        
    return callback_frequency_df


def get_callback_jitter_df(target: Union[Application, Executor, Node, CallbackGroup, List[CallbackBase]]) -> pd.DataFrame:
    latency_table = concate_cb_latency_table(target)
    latency_table = latency_table.loc[:, latency_table.columns.str.contains('callback_start_timestamp')]
    latency_table.columns = [c.replace('/callback_start_timestamp', '') for c in latency_table.columns]
    
    callback_jitter_df = pd.DataFrame(columns=pd.MultiIndex.from_product([latency_table.columns, ['timestamp', 'value']]))
    for callback_name in latency_table.columns:
        callback_jitter_df[(callback_name, 'timestamp')] = latency_table[callback_name]
        callback_jitter_df[(callback_name, 'value')] = latency_table[callback_name].diff()
    callback_jitter_df = callback_jitter_df.drop(callback_jitter_df.index[0])  # 0 行目の 'value' が pd.NA になるため削除
        
    return callback_jitter_df.dropna()  # 最も低周期の callback (index の長さが最も短い列) に合わせる


def get_callback_latency_df(target: Union[Application, Executor, Node, CallbackGroup, List[CallbackBase]]) -> pd.DataFrame:
    latency_table = concate_cb_latency_table(target)
    callback_names = []
    if(type(target) == list):
        callback_names = [c.callback_name for c in target]
    else:
        callback_names = target.callback_names
    
    callback_latency_df = pd.DataFrame(columns=pd.MultiIndex.from_product([callback_names, ['timestamp', 'value']]))
    for callback_name in callback_names:
        callback_latency_df[(callback_name, 'timestamp')] = latency_table[callback_name + '/callback_start_timestamp']  # start_timestamp で良いか要確認
        callback_latency_df[(callback_name, 'value')] = latency_table[callback_name + '/callback_end_timestamp'] - latency_table[callback_name + '/callback_start_timestamp']
        
    return callback_latency_df.dropna()  # 最も低周期の callback (index の長さが最も短い列) に合わせる


def get_callbacks_from_names_list(app : Application , callback_names_list : List[str]) -> List[CallbackBase]:
    callbacks = []
    
    for callback_name in callback_names_list:
        try:
            callbacks.append(app.get_callback(callback_name))
        except:
            print(f"[Error] Failed find '{callback_name}'.")
        
    return callbacks