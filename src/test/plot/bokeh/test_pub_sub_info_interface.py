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

from caret_analyze.plot.bokeh.pub_sub_info_interface import PubSubTimeSeriesPlot
from caret_analyze.runtime.publisher import Publisher


class TestPubSubTimeSeriesPlot:

    def test_get_ts_column_name_no_callback_name_case(self, mocker):
        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_mock, 'callback_names', [])

        assert (PubSubTimeSeriesPlot._get_ts_column_name(pub_mock)
                == '/rclcpp_publish_timestamp [ns]')

    def test_get_ts_column_name_normal_case(self, mocker):
        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_mock, 'callback_names', ['cb0', 'cb1'])

        assert (PubSubTimeSeriesPlot._get_ts_column_name(pub_mock)
                == 'cb0/rclcpp_publish_timestamp [ns]')
