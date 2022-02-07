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

from caret_analyze.infra import RecordsProvider, RuntimeDataProvider
from caret_analyze.runtime import Subscription
from caret_analyze.value_objects import Qos, SubscriptionStructValue

from pytest_mock import MockerFixture


class TestSubscription:

    def test_qos_records_provider(self, mocker: MockerFixture):
        val_mock = mocker.Mock(spec=SubscriptionStructValue)
        provider_mock = mocker.Mock(spec=RecordsProvider)
        sub = Subscription(val_mock, provider_mock)
        assert sub.qos is None

    def test_qos_runtime_data_provider(self, mocker: MockerFixture):
        val_mock = mocker.Mock(spec=SubscriptionStructValue)
        provider_mock = mocker.Mock(spec=RuntimeDataProvider)

        qos_mock = mocker.Mock(spec=Qos)
        mocker.patch.object(provider_mock, 'get_qos', return_value=qos_mock)

        sub = Subscription(val_mock, provider_mock)
        assert sub.qos == qos_mock
