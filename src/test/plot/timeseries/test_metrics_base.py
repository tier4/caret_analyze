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

from caret_analyze.plot.metrics_base import MetricsBase
from caret_analyze.runtime.callback import CallbackBase
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.publisher import Publisher
from caret_analyze.runtime.subscription import Subscription


class TestMetricsBase:

    def test_get_ts_column_name_pub_no_callback_name(self, mocker):
        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_mock, 'callback_names', [])

        assert (MetricsBase._get_ts_column_name(pub_mock)
                == 'rclcpp_publish_timestamp [ns]')

    def test_get_ts_column_name_pub_normal(self, mocker):
        pub_mock = mocker.Mock(spec=Publisher)
        mocker.patch.object(pub_mock, 'callback_names', ['cb0', 'cb1'])

        assert (MetricsBase._get_ts_column_name(pub_mock)
                == 'cb0/rclcpp_publish_timestamp [ns]')

    def test_get_ts_column_name_sub(self, mocker):
        sub_mock = mocker.Mock(spec=Subscription)
        mocker.patch.object(sub_mock, 'column_names', ['node/start', 'node/end'])

        assert (MetricsBase._get_ts_column_name(sub_mock)
                == 'node/start [ns]')

    def test_get_ts_column_name_callback(self, mocker):
        cb_mock = mocker.Mock(spec=CallbackBase)
        mocker.patch.object(cb_mock, 'column_names', ['node/start', 'node/end'])

        assert MetricsBase._get_ts_column_name(cb_mock) == 'start [ns]'

    def test_get_ts_column_name_communication(self, mocker):
        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(comm_mock, 'column_names', ['node/start', 'node/end'])

        assert MetricsBase._get_ts_column_name(comm_mock) == 'start [ns]'
