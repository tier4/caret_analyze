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

from logging import getLogger, WARNING

from caret_analyze.value_objects import (PublisherStructValue,
                                         SubscriptionCallbackStructValue,
                                         TimerCallbackStructValue)
from caret_analyze.value_objects.message_context import CallbackChain


class TestCallbackChain:

    def test_verify(self, mocker, caplog):
        sub_callback_mock = mocker.Mock(spec=SubscriptionCallbackStructValue)
        timer_callback_mock = mocker.Mock(spec=TimerCallbackStructValue)
        publisher_mock = mocker.Mock(spec=PublisherStructValue)

        mocker.patch.object(timer_callback_mock, 'callback_name',
                            '/node_name/callback_0')
        mocker.patch.object(publisher_mock, 'topic_name',
                            'publisher_topic_name')

        caplog.set_level(WARNING)
        logger = getLogger('caret_analyze.value_objects.message_context')
        logger.addHandler(caplog.handler)

        # True case
        callback_chain = CallbackChain(
            '/node_name', {}, None, publisher_mock,
            (sub_callback_mock, timer_callback_mock)
        )
        caplog.clear()
        assert callback_chain.verify() is True
        assert len(caplog.record_tuples) == 0

        # No association between callback and publisher
        callback_chain = CallbackChain(
            '/node_name', {}, None, publisher_mock, None
        )
        mocker.patch.object(
            publisher_mock, 'summary',
            {'topic_name': 'publisher_topic_name', 'callbacks': ()}
        )
        caplog.clear()
        assert callback_chain.verify() is False
        assert ('callback-chain is empty. '
                'The callback is not associated with the publisher. '
                'publisher topic name:') in caplog.text

        # No setting variable_passing
        callback_chain = CallbackChain(
            '/node_name', {}, None, publisher_mock, None
        )
        mocker.patch.object(
            publisher_mock, 'summary',
            {'topic_name': 'publisher_topic_name',
             'callbacks': ('/node_name/callback_0',)}
        )
        caplog.clear()
        assert callback_chain.verify() is False
        assert ('callback-chain is empty. variable_passings are not set. '
                'node name:') in caplog.text
