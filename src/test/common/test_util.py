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


from caret_analyze.common.util import Util
from caret_analyze.exceptions import ItemNotFoundError
from caret_analyze.runtime.application import Application
from caret_analyze.runtime.communication import Communication
from caret_analyze.runtime.node import Node
import pytest


class TestUtil:

    def test_find_similar_one(self, mocker):
        app_mock = mocker.Mock(spec=Application)
        node = mocker.Mock(spec=Node)
        mocker.patch.object(node, 'node_name', '/AAA/BBB/CCC')
        mocker.patch.object(app_mock, 'nodes', [])
        app_mock.nodes.append(node)

        def key(x):
            return x.node_name

        target_name = '/AAA/BBB/CCC'
        with pytest.raises(ItemNotFoundError):
            Util.find_similar_one(target_name, [])
        assert Util.find_similar_one(target_name, [target_name]) == target_name

        with pytest.raises(ItemNotFoundError):
            Util.find_similar_one(target_name, ['/AAA/b'])

        assert Util.find_similar_one(target_name,
                                     app_mock.nodes,
                                     key) == node

        target_name = '/AAA/BBB/DDD'
        with pytest.raises(ItemNotFoundError) as e:
            Util.find_similar_one(target_name,
                                  app_mock.nodes,
                                  key,
                                  0.4)
        assert '/AAA/BBB/CCC' in str(e.value)

    def test_find_similar_one_multi_keys(self, mocker):
        app_mock = mocker.Mock(spec=Application)
        comm_mock = mocker.Mock(spec=Communication)
        mocker.patch.object(comm_mock, 'publish_node_name', 'pub_node')
        mocker.patch.object(comm_mock, 'subscribe_node_name', 'sub_node')
        mocker.patch.object(comm_mock, 'topic_name', 'topic')
        mocker.patch.object(comm_mock, 'publisher_construction_order', 1)
        mocker.patch.object(comm_mock, 'subscription_construction_order', 0)
        mocker.patch.object(app_mock, 'communications', [])
        app_mock.communications.append(comm_mock)

        target_names = {'publisher_node_name': 'pub_node',
                        'subscription_node_name': 'sub_node',
                        'topic_name': 'topic',
                        'publisher_construction_order': 1,
                        'subscription_construction_order': 0}

        def keys(x):
            return {'publisher_node_name': x.publish_node_name,
                    'subscription_node_name': x.subscribe_node_name,
                    'topic_name': x.topic_name,
                    'publisher_construction_order': x.publisher_construction_order,
                    'subscription_construction_order': x.subscription_construction_order}

        with pytest.raises(ItemNotFoundError):
            Util.find_similar_one_multi_keys(target_names, [])

        assert Util.find_similar_one_multi_keys(
            target_names, [target_names]) == target_names

        assert Util.find_similar_one_multi_keys(target_names,
                                                app_mock.communications,
                                                keys) == comm_mock

        target_names = {'publisher_node_name': 'pub_node',
                        'subscription_node_name': 'sub_node',
                        'topic_name': 'topic',
                        'publisher_construction_order': 0,
                        'subscription_construction_order': 0}
        with pytest.raises(ItemNotFoundError) as e:
            Util.find_similar_one_multi_keys(target_names,
                                             app_mock.communications,
                                             keys)
        assert 'publisher_construction_order' in str(e.value)
