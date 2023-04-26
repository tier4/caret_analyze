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

from caret_analyze.architecture.struct import (NodeStruct,
                                               PublisherStruct, SubscriptionStruct)


class TestNodeStruct:

    def test_get_sub_pub(self, mocker):
        pub_mock = mocker.Mock(spec=PublisherStruct)
        sub_mock1 = mocker.Mock(spec=SubscriptionStruct)
        sub_mock2 = mocker.Mock(spec=SubscriptionStruct)
        sub_mock3 = mocker.Mock(spec=SubscriptionStruct)
        pub_mock1 = mocker.Mock(spec=PublisherStruct)
        pub_mock2 = mocker.Mock(spec=PublisherStruct)

        mocker.patch.object(pub_mock1, 'topic_name', 'topic1')
        mocker.patch.object(pub_mock2, 'topic_name', 'topic2')
        mocker.patch.object(pub_mock2, 'construction_order', 1)
        mocker.patch.object(pub_mock, 'construction_order', 0)
        mocker.patch.object(sub_mock1, 'topic_name', 'topic1')
        mocker.patch.object(sub_mock2, 'topic_name', 'topic2')
        mocker.patch.object(sub_mock2, 'construction_order', 0)
        mocker.patch.object(sub_mock3, 'topic_name', 'topic2')
        mocker.patch.object(sub_mock3, 'construction_order', 1)
        sub_mock0 = [sub_mock1, sub_mock2, sub_mock3]
        pub_mock0 = [pub_mock1, pub_mock2]
        node_struct = NodeStruct(None, pub_mock0, sub_mock0, None, None, None, None, None)
        pubs = node_struct.get_publisher('topic2', 1)
        assert pubs.topic_name == 'topic2'
        assert pubs.construction_order == 1

        subs = node_struct.get_subscription_from_construction_order('topic2', 1)
        assert subs.topic_name == 'topic2'
        assert subs.construction_order == 1
