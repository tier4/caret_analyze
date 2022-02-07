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
# from caret_analyze.trace.lttng import Lttng


# class TestNode:

#     def test_init_satus(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')
#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[0]

#         assert len(node.variable_passings) == 0

#     def test_search_paths(self):
#         lttng = Lttng('sample/lttng_samples/end_to_end_sample/cyclonedds')
#         app = Application(
#             'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml', 'yaml', lttng)
#         node = next(filter(lambda x: x.node_name == '/message_driven_node', app.nodes))
#         assert len(node.paths) == len(app.paths) + len(node.callbacks)

#     def test_compose_callback_duration(self):
#         lttng = Lttng('sample/lttng_samples/talker_listener/')

#         app = Application(
#             'sample/lttng_samples/talker_listener/architecture.yaml', 'yaml', lttng)
#         node = app.nodes[0]
#         callback = node.callbacks[0]

#         df = callback.to_dataframe()
#         columns_exepct = {'callback_start_timestamp', 'callback_end_timestamp'}
#         assert set(df.columns) == columns_exepct

#         t, latencies = callback.to_timeseries()
#         bin_latencies, hist = callback.to_histogram()

#         assert len(t) == 5 and len(latencies) == 5
#         assert len(bin_latencies) == 1 and len(hist) == 2
