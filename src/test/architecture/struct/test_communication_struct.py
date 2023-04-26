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

from caret_analyze.architecture.struct import (CommunicationStruct,
                                               PublisherStruct, SubscriptionStruct)


class TestCommunicationStruct:

    def test_get_sub_pub(self, mocker):
        sub_mock = mocker.Mock(spec=SubscriptionStruct)
        pub_mock = mocker.Mock(spec=PublisherStruct)

        mocker.patch.object(sub_mock, 'topic_name', 'topic')
        mocker.patch.object(sub_mock, 'construction_order', 1)
        mocker.patch.object(pub_mock, 'construction_order', 2)
        com_struct = CommunicationStruct(None, None, pub_mock, sub_mock, None, None)
        assert com_struct.subscription_construction_order == 1
        assert com_struct.publisher_construction_order == 2
