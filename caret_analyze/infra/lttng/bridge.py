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
from caret_analyze.infra.lttng.lttng_info import LttngInfo
from caret_analyze.value_objects.subscription import (
    IntraProcessBufferValue,
    SubscriptionStructValue,
    SubscriptionValue,
)

from .value_objects import (
    PublisherValueLttng,
    SubscriptionCallbackValueLttng,
    TimerCallbackValueLttng,
    TransformBroadcasterValueLttng,
    TransformBufferValueLttng,
    IntraProcessBufferValueLttng,
    SubscriptionValueLttng,
)
from ...common import Util
from ...exceptions import ItemNotFoundError, MultipleItemFoundError, UnsupportedTypeError
from ...value_objects import (
    PublisherStructValue,
    SubscriptionCallbackStructValue,
    CallbackStructValue,
    SubscriptionCallbackValue,
    TimerCallbackStructValue,
    TimerCallbackValue,
    TransformBroadcasterStructValue,
    TransformBroadcasterValue,
    TransformFrameBroadcasterStructValue,
    TransformFrameBufferStructValue,
    TransformBufferStructValue,
    TransformBufferValue,
    IntraProcessBufferStructValue,
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


# class TimerCallbackBindCondition:
#     """
#     Compare timer callback value with the same conditions.

#     used conditions:
#     - node name
#     - callback type
#     - period_ns
#     - publish topic names

#     """

#     def __init__(
#         self,
#         target_condition: Union[TimerCallbackValue, TimerCallbackStructValue]
#     ) -> None:
#         assert isinstance(target_condition, TimerCallbackValue) or \
#             isinstance(target_condition, TimerCallbackStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         callback_value: Union[TimerCallbackValue, TimerCallbackStructValue],
#     ) -> bool:
#         if isinstance(self._target, TimerCallbackValue) and \
#                 isinstance(callback_value, TimerCallbackStructValue):
#             return self._compare(self._target, callback_value)

#         if isinstance(self._target, TimerCallbackStructValue) and \
#                 isinstance(callback_value, TimerCallbackValue):
#             return self._compare(callback_value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: TimerCallbackValue,
#         struct_value: TimerCallbackStructValue
#     ) -> bool:
#         return value.node_name == struct_value.node_name and \
#             value.callback_type == struct_value.callback_type and \
#             value.period_ns == struct_value.period_ns and \
#             value.symbol == struct_value.symbol

#     def __str__(self):
#         return str(self._target)


# class SubscriptionCallbackBindCondition:
#     """
#     Get subscription callback value with the same conditions.

#     used conditions:
#     - node name
#     - callback type
#     - subscription topic name
#     - publish topic names

#     """

#     def __init__(
#         self,
#         target_condition: Union[SubscriptionCallbackValue,
#                                 SubscriptionCallbackStructValue]
#     ) -> None:
#         assert isinstance(target_condition, SubscriptionCallbackValue) or \
#             isinstance(target_condition, SubscriptionCallbackStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         callback_value: Union[SubscriptionCallbackValue, SubscriptionCallbackStructValue],
#     ) -> bool:
#         if isinstance(self._target, SubscriptionCallbackValue) and \
#                 isinstance(callback_value, SubscriptionCallbackStructValue):
#             return self._compare(self._target, callback_value)

#         if isinstance(self._target, SubscriptionCallbackStructValue) and \
#                 isinstance(callback_value, SubscriptionCallbackValue):
#             return self._compare(callback_value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: SubscriptionCallbackValue,
#         struct_value: SubscriptionCallbackStructValue
#     ) -> bool:
#         # The value on publish_topic_names obtained from lttng and
#         # publish_topic_names obtained from yaml are different.
#         # pub_match = True
#         # # if value.publish_topic_names is not None:
#         # #     pub_match = value.publish_topic_names == struct_value.publish_topic_names

#         return value.node_name == struct_value.node_name and \
#             value.callback_type == struct_value.callback_type and \
#             value.subscribe_topic_name == struct_value.subscribe_topic_name and \
#             value.symbol == struct_value.symbol

#     def __str__(self):
#         return str(self._target)


# class SubscriptionBindCondition:
#     """
#     Get subscription callback value with the same conditions.

#     used conditions:
#     - node name
#     - callback type
#     - subscription topic name
#     - publish topic names

#     """

#     def __init__(
#         self,
#         target_condition: Union[SubscriptionValue, SubscriptionStructValue]
#     ) -> None:
#         assert isinstance(target_condition, SubscriptionValue) or \
#             isinstance(target_condition, SubscriptionStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         value: Union[SubscriptionValue, SubscriptionStructValue],
#     ) -> bool:
#         if isinstance(self._target, SubscriptionValue) and \
#                 isinstance(value, SubscriptionStructValue):
#             return self._compare(self._target, value)

#         if isinstance(self._target, SubscriptionStructValue) and \
#                 isinstance(value, SubscriptionValue):
#             return self._compare(value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: SubscriptionValue,
#         struct_value: SubscriptionStructValue
#     ) -> bool:
#         # The value on publish_topic_names obtained from lttng and
#         # publish_topic_names obtained from yaml are different.
#         # pub_match = True
#         # # if value.publish_topic_names is not None:
#         # #     pub_match = value.publish_topic_names == struct_value.publish_topic_names

#         return value.node_name == struct_value.node_name and \
#             value.topic_name == struct_value.topic_name

#     def __str__(self):
#         return str(self._target)


# class TransformBroadcasterBindCondition:
#     """
#     Get transform broadcaster value with the same conditions.

#     used conditions:
#     - transforms
#     - node_name

#     """

#     def __init__(
#         self,
#         target_condition: Union[TransformBroadcasterValue,
#                                 TransformBroadcasterStructValue,
#                                 TransformFrameBroadcasterStructValue]
#     ) -> None:
#         assert isinstance(target_condition, TransformBroadcasterValue) or \
#             isinstance(target_condition, TransformBroadcasterStructValue) or \
#             isinstance(target_condition, TransformFrameBroadcasterStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         callback_value: Union[
#             TransformBroadcasterValue,
#             TransformBroadcasterStructValue,
#             TransformFrameBroadcasterStructValue],
#     ) -> bool:
#         if isinstance(self._target, TransformBroadcasterValue) and \
#                 isinstance(callback_value, TransformBroadcasterStructValue):
#             return self._compare(self._target, callback_value)

#         if isinstance(self._target, TransformBroadcasterStructValue) and \
#                 isinstance(callback_value, TransformBroadcasterValue):
#             return self._compare(callback_value, self._target)

#         if isinstance(self._target, TransformFrameBroadcasterStructValue) and \
#                 isinstance(callback_value, TransformBroadcasterValue):
#             return self._compare(callback_value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: TransformBroadcasterValue,
#         struct_value: Union[TransformBroadcasterStructValue,
# TransformFrameBroadcasterStructValue]
#     ) -> bool:
#         return value.node_name == struct_value.node_name

#     def __str__(self):
#         return str(self._target)


# class TransformBufferBindCondition:
#     """
#     Get transform buffer value with the same conditions.

#     used conditions:
#     - lookup_node_name
#     - listener_node_name

#     """

#     def __init__(
#         self,
#         target_condition: Union[TransformBufferValue,
#                                 TransformBufferStructValue,
#                                 TransformFrameBufferStructValue]
#     ) -> None:
#         assert isinstance(target_condition, TransformBufferValue) or \
#             isinstance(target_condition, TransformBufferStructValue) or \
#             isinstance(target_condition, TransformFrameBufferStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         callback_value: Union[TransformBufferValue,
#                               TransformBufferStructValue,
#                               TransformFrameBufferStructValue],
#     ) -> bool:
#         if isinstance(self._target, TransformBufferValue) and \
#                 isinstance(callback_value, TransformBufferStructValue):
#             return self._compare(self._target, callback_value)

#         if isinstance(self._target, TransformBufferStructValue) and \
#                 isinstance(callback_value, TransformBufferValue):
#             return self._compare(callback_value, self._target)

#         if isinstance(self._target, TransformFrameBufferStructValue) and \
#                 isinstance(callback_value, TransformBufferValue):
#             return self._compare(callback_value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: TransformBufferValue,
#         struct_value: Union[TransformBufferStructValue, TransformFrameBufferStructValue]
#     ) -> bool:
#         return value.lookup_node_name == struct_value.lookup_node_name and \
#             value.listener_node_name == struct_value.listener_node_name

#     def __str__(self):
#         return str(self._target)


# class PublisherBindCondition:
#     """
#     Get publisher value with the same conditions.

#     used conditions:
#     - node name
#     - topic name

#     """

#     def __init__(
#         self,
#         target_condition: Union[PublisherValueLttng, PublisherStructValue]
#     ) -> None:
#         assert isinstance(target_condition, PublisherValueLttng) or \
#             isinstance(target_condition, PublisherStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         publisher_value: Union[PublisherValueLttng, PublisherStructValue],
#     ) -> bool:
#         if isinstance(self._target, PublisherValueLttng) and \
#                 isinstance(publisher_value, PublisherStructValue):
#             return self._compare(self._target, publisher_value)

#         if isinstance(self._target, PublisherStructValue) and \
#                 isinstance(publisher_value, PublisherValueLttng):
#             return self._compare(publisher_value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: PublisherValueLttng,
#         struct_value: PublisherStructValue
#     ) -> bool:
#         return value.node_name == struct_value.node_name and \
#             value.topic_name == struct_value.topic_name

#     def __str__(self):
#         return self._target


# class IntraProcessBufferBindCondition:

#     def __init__(
#         self,
#         target_condition: Union[IntraProcessBufferValueLttng, IntraProcessBufferStructValue]
#     ) -> None:
#         assert isinstance(target_condition, IntraProcessBufferValue) or \
#             isinstance(target_condition, IntraProcessBufferStructValue)
#         self._target = target_condition

#     def __call__(
#         self,
#         value: Union[IntraProcessBufferValueLttng, IntraProcessBufferStructValue],
#     ) -> bool:
#         if isinstance(self._target, IntraProcessBufferValueLttng) and \
#                 isinstance(value, IntraProcessBufferStructValue):
#             return self._compare(self._target, value)

#         if isinstance(self._target, IntraProcessBufferStructValue) and \
#                 isinstance(value, IntraProcessBufferValueLttng):
#             return self._compare(value, self._target)

#         raise NotImplementedError()

#     def _compare(
#         self,
#         value: IntraProcessBufferValueLttng,
#         struct_value: IntraProcessBufferStructValue
#     ) -> bool:
#         return value.node_name == struct_value.node_name and \
#             value.topic_name == struct_value.topic_name

#     def __str__(self):
#         return self._target
