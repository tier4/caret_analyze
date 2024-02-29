# Copyright 2021 TIER IV, Inc.
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

from typing import NamedTuple


class TracePoint(NamedTuple):
    CALLBACK_START_TIMESTAMP: str = 'callback_start_timestamp'
    CALLBACK_END_TIMESTAMP: str = 'callback_end_timestamp'
    RCLCPP_PUBLISH_TIMESTAMP: str = 'rclcpp_publish_timestamp'
    RCL_PUBLISH_TIMESTAMP: str = 'rcl_publish_timestamp'
    DDS_WRITE_TIMESTAMP: str = 'dds_write_timestamp'
    ON_DATA_AVAILABLE_TIMESTAMP: str = 'on_data_available_timestamp'
    RCLCPP_INTRA_PUBLISH_TIMESTAMP: str = 'rclcpp_intra_publish_timestamp'


TRACE_POINT = TracePoint()
