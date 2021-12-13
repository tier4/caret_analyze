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
# from caret_analyze.callback import SubscriptionCallback
# from caret_analyze.communication import Communication
# from caret_analyze.communication import DDSLatency
# from caret_analyze.communication import VariablePassing
# from caret_analyze.trace.lttng import Lttng
# from caret_analyze.value_objects.publisher_info import PublisherInfo

# import pytest


# class TestCommunication:

#     @pytest.mark.parametrize(
#         'trace_dir, arch_path, expect', [
#             (
#                 'sample/lttng_samples/end_to_end_sample/fastrtps',
#                 'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml',
#                 {
#                     'callback_start_timestamp',
#                     'dds_write_timestamp',
#                     'on_data_available_timestamp',
#                     'rcl_publish_timestamp',
#                     'rclcpp_publish_timestamp',
#                 }
#             ),
#             (
#                 'sample/lttng_samples/end_to_end_sample/cyclonedds',
#                 'sample/lttng_samples/end_to_end_sample/architecture_modified.yaml',
#                 {
#                     'callback_start_timestamp',
#                     'dds_write_timestamp',
#                     'rcl_publish_timestamp',
#                     'rclcpp_publish_timestamp',
#                 }
#             )
#         ]
#     )
#     def test_to_dataframe(self, trace_dir, arch_path, expect):
#         lttng = Lttng(trace_dir)
#         app = Application(arch_path, 'yaml', lttng)
#         comm = app.communications[0]

#         df = comm.to_dataframe()
#         assert set(df.columns) == expect

#     @pytest.mark.parametrize(
#         'trace_dir, comm_idx, is_intra_process',
#         [
#             ('sample/lttng_samples/talker_listener/', 0, False),
#             ('sample/lttng_samples/cyclic_pipeline_intra_process', 0, True),
#         ],
#     )
#     def test_is_intra_process(self, trace_dir, comm_idx, is_intra_process):
#         lttng = Lttng(trace_dir)
#         app = Application(trace_dir, 'lttng', lttng)
#         comm = app.communications[comm_idx]
#         assert comm.is_intra_process == is_intra_process

#     @pytest.mark.parametrize(
#         'is_intra_process, columns',
#         [
#             (True, Communication.column_names_intra_process),
#             (False, Communication.column_names_inter_process),
#         ],
#     )
#     def test_column_names(self, is_intra_process: bool, columns: List[str]):
#         callback = SubscriptionCallback(None, '', '', '', '')
#         pub = PublisherInfo('node_name', 'topic_name', 'callback_name')
#         comm = Communication(None, callback, callback, pub)

#         comm.is_intra_process = is_intra_process
#         if is_intra_process:
#             assert comm.column_names == Communication.column_names_intra_process
#         else:
#             comm.rmw_implementation = 'rmw_fastrtps_cpp'
#             expect = Communication.column_names_inter_process_dds_latency_support
#             assert comm.column_names == expect
#             comm.rmw_implementation = 'rmw_cyclonedds_cpp'
#             assert comm.column_names == Communication.column_names_inter_process

#     @pytest.mark.parametrize(
#         'trace_dir',
#         [
#             ('sample/lttng_samples/talker_listener/'),
#             ('sample/lttng_samples/cyclic_pipeline_intra_process'),
#             ('sample/lttng_samples/end_to_end_sample/fastrtps'),
#             ('sample/lttng_samples/end_to_end_sample/cyclonedds'),
#         ],
#     )
#     def test_to_timeseries(
#         self, trace_dir
#     ):
#         lttng = Lttng(trace_dir)
#         app = Application(trace_dir, 'lttng', lttng)
#         comm = app.communications[0]

#         t, latencies = comm.to_timeseries()
#         records_len = len(comm.to_records().data)
#         assert len(t) == records_len and len(latencies) == records_len

#     def test_to_dds_latency_fastrtps(self):
#         lttng = Lttng('sample/lttng_samples/end_to_end_sample/fastrtps')
#         app = Application(
#             'sample/lttng_samples/end_to_end_sample/architecture.yaml', 'yaml', lttng)
#         comm = app.communications[0]

#         latency = comm.to_dds_latency()

#         t, latencies = latency.to_timeseries()
#         records_len = len(comm.to_records().data)
#         assert len(t) == records_len and len(latencies) == records_len

#     def test_to_dds_latency_cyclonedds(self):
#         lttng = Lttng('sample/lttng_samples/end_to_end_sample/cyclonedds')
#         app = Application(
#             'sample/lttng_samples/end_to_end_sample/architecture.yaml', 'yaml', lttng)
#         comm = app.communications[0]

#         with pytest.raises(AssertionError):
#             comm.to_dds_latency()


# class TestDDSLatency:

#     def test_column_names(self):
#         columns = DDSLatency._column_names
#         Communication.column_names_intra_process

#         pub = PublisherInfo('node_name', 'topic_name', 'callback_name')
#         callback = SubscriptionCallback(None, '', '', '', '')
#         comm = DDSLatency(None, callback, callback, pub)
#         assert comm.column_names == columns


# class TestVariablePassingLatency:

#     def test_column_names(self):
#         columns = VariablePassing._column_names
#         Communication.column_names_intra_process

#         callback = SubscriptionCallback(None, '', '', '', '')
#         comm = VariablePassing(None, callback, callback)

#         assert comm.column_names == columns
