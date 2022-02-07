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

# from caret_analyze.application import Application
# from caret_analyze.callback import CallbackBase, SubscriptionCallback
# from caret_analyze.trace.lttng import Lttng
# from caret_analyze.value_objects.callback_info import (
#     SubscriptionCallbackStructInfo, TimerCallbackStructInfo)


# class TestTimerCallback:

#     def test_to_callback_name(self):
#         assert TimerCallbackStructInfo.to_indexed_callback_name(0) == 'timer_callback_0'
#         assert TimerCallbackStructInfo.to_indexed_callback_name(1) == 'timer_callback_1'

#     def test_to_dataframe(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')
#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[1]
#         callback = node.callbacks[0]

#         callback.records_container = lttng

#         df = callback.to_dataframe()
#         columns_expect = {'callback_start_timestamp', 'callback_end_timestamp'}
#         assert set(df.columns) == columns_expect

#     def test_to_timeseries_and_to_histogram(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')
#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[1]
#         callback = node.callbacks[0]

#         t, latencies = callback.to_timeseries()
#         assert len(t) == 5 and len(latencies) == 5

#         latencies, hist = callback.to_histogram(binsize_ns=100000)
#         assert len(latencies) == 5 and len(hist) == 6


# class TestSubscriptionCallback:

#     def test_to_callback_name(self):
#         assert SubscriptionCallbackStructInfo.to_indexed_callback_name(
#             0) == 'subscription_callback_0'
#         assert SubscriptionCallbackStructInfo.to_indexed_callback_name(
#             1) == 'subscription_callback_1'

#     def test_to_dataframe(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')
#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[0]
#         callback = node.callbacks[0]

#         callback.records_container = lttng

#         df = callback.to_dataframe()
#         columns_expect = {'callback_start_timestamp', 'callback_end_timestamp'}
#         assert set(df.columns) == columns_expect

#     def test_to_timeseries_and_to_histogram(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')
#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[0]
#         callback = node.callbacks[0]

#         t, latencies = callback.to_timeseries()
#         assert len(t) == 5 and len(latencies) == 5

#         latencies, hist = callback.to_histogram(binsize_ns=100000)
#         assert len(latencies) == 4 and len(hist) == 5

#     def test_column_names(self):
#         columns = CallbackBase._column_names

#         callback = SubscriptionCallback(None, '', '', '', '')
#         assert callback.column_names == columns
