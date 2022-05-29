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

from functools import lru_cache
from typing import List, Union

from .lttng_info import LttngInfo
from .value_objects import (
    IntraProcessBufferValueLttng,
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    SubscriptionValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
)
from ...common import Util
from ...exceptions import ItemNotFoundError, MultipleItemFoundError, UnsupportedTypeError
from ...value_objects import (
    CallbackStructValue,
    IntraProcessBufferStructValue,
    PublisherStructValue,
    SubscriptionCallbackStructValue,
    SubscriptionStructValue,
    TimerCallbackStructValue,
    TransformBroadcasterStructValue,
    TransformBufferStructValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
)


class LttngBridge():

    def __init__(
        self,
        lttng_info: LttngInfo
    ) -> None:
        self._info = lttng_info

    @lru_cache
    def get_timer_callback(
        self,
        callback: TimerCallbackStructValue
    ) -> TimerCallbackValueLttng:
        """
        Compare timer callback value with the same conditions.

        used conditions:
        - node name
        - callback type
        - period_ns
        - publish topic names

        Parameters
        ----------
        callback :TimerCallbackStructValueTimerCallbackStructValue
            Callback value to be searched.

        Returns
        -------
        [TimerCallbackValueLttng]
            Timer callback value, including runtime information.

        Raises
        ------
        ItemNotFoundError
            No value matching the search condition is found.
        MultipleItemFoundError
            Multiple pieces of values matching the search condition are found.

        """
        node = self._info.get_node(callback.node_name)
        timer_callbacks = self._info.get_timer_callbacks(node)

        try:
            target_id_value = callback.id_value
            timer_callback = Util.find_one(
                lambda x: x.id_value == target_id_value, timer_callbacks)
        except ItemNotFoundError:
            msg = 'No value matching the search condition is found. '
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple pieces of values matching the search condition are found.'
            raise MultipleItemFoundError(msg)

        return timer_callback

    @lru_cache
    def get_subscription(
        self,
        subscription: SubscriptionStructValue
    ) -> SubscriptionValueLttng:
        node = self._info.get_node(subscription.node_name)
        subs = self._info.get_subscriptions(node)

        try:
            target_id_value = subscription.id_value
            sub = Util.find_one(
                lambda x: x.id_value == target_id_value, subs)
        except ItemNotFoundError:
            msg = 'No value matching the search condition is found. '
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple pieces of values matching the search condition are found. '
            raise MultipleItemFoundError(msg)

        return sub

    @lru_cache
    def get_subscription_callback(
        self,
        callback: SubscriptionCallbackStructValue
    ) -> SubscriptionCallbackValueLttng:
        """
        Get subscription callback value with the same conditions.

        used conditions:
        - node name
        - callback type
        - subscription topic name
        - publish topic names

        Parameters
        ----------
        callback : SubscriptionCallbackStructValue
            Callback value to be searched.

        Returns
        -------
        SubscriptionCallbackValueLttng
            Timer callback value, including runtime information.

        Raises
        ------
        ItemNotFoundError
            No value matching the search condition is found.
        MultipleItemFoundError
            Multiple pieces of values matching the search condition are found.

        """
        node = self._info.get_node(callback.node_name)
        sub_callbacks = self._info.get_subscription_callbacks(node)
        try:
            target_id_value = callback.id_value
            sub_callback = Util.find_one(
                lambda x: x.id_value == target_id_value, sub_callbacks)
        except ItemNotFoundError:
            msg = 'No value matching the search condition is found. '
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple pieces of values matching the search condition are found. '
            raise MultipleItemFoundError(msg)

        return sub_callback

    @lru_cache
    def get_tf_broadcaster(
        self,
        broadcaster: Union[TransformBroadcasterStructValue,
                           TransformFrameBroadcasterStructValue]
    ) -> TransformBroadcasterValueLttng:
        node = self._info.get_node(broadcaster.node_name)
        br = self._info.get_tf_broadcaster(node)

        if br is None:
            msg = 'No value matching the search condition is found. '
            raise ItemNotFoundError(msg)

        assert isinstance(br, TransformBroadcasterValueLttng)
        return br

    @lru_cache
    def get_tf_buffer(
        self,
        buffer: Union[TransformBufferStructValue, TransformFrameBufferStructValue]
    ) -> TransformBufferValueLttng:
        node = self._info.get_node(buffer.lookup_node_name)
        buf = self._info.get_tf_buffer(node)
        if buf is None:
            msg = 'No value matching the search condition is found. '
            raise ItemNotFoundError(msg)
        assert isinstance(buf, TransformBufferValueLttng)
        return buf

    @lru_cache
    def get_callback(
        self, callback: CallbackStructValue
    ) -> Union[TimerCallbackValueLttng, SubscriptionCallbackValueLttng]:

        if isinstance(callback, TimerCallbackStructValue):
            return self.get_timer_callback(callback)

        if isinstance(callback, SubscriptionCallbackStructValue):
            return self.get_subscription_callback(callback)

        msg = 'Failed to get callback object. '
        msg += f'{callback.callback_type.type_name} is not supported.'
        raise UnsupportedTypeError(msg)

    @lru_cache
    def get_publishers(
        self,
        publisher_value: PublisherStructValue
    ) -> List[PublisherValueLttng]:
        """
        Get publisher handles.

        Parameters
        ----------
        node_name : str
        topic_name : str

        Returns
        -------
        List[PublisherValueLttng]
            publisher values that matches the condition

        """
        try:
            node = self._info.get_node(publisher_value.node_name)
            pubs = self._info.get_publishers(node)
            target_value = publisher_value.id_value
            pubs_filtered = Util.filter_items(
                lambda x: x.id_value == target_value, pubs)
        except ItemNotFoundError:
            raise ItemNotFoundError('Failed to find publisher instance. ')

        return pubs_filtered

    @lru_cache
    def get_ipc_buffer(
        self,
        ipc_buffer: IntraProcessBufferStructValue,
    ) -> IntraProcessBufferValueLttng:
        """
        Get publisher handles.

        Parameters
        ----------
        node_name : str
        topic_name : str

        Returns
        -------
        List[PublisherValueLttng]
            publisher values that matches the condition

        """
        try:
            node = self._info.get_node(ipc_buffer.node_name)
            bufs = self._info.get_ipc_buffers(node)
            target_id_value = ipc_buffer.id_value
            return Util.find_one(lambda x: x.id_value == target_id_value, bufs)
        except ItemNotFoundError:
            raise ItemNotFoundError('Failed to find publisher instance. ')
