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

from typing import List, Union

from .lttng import Lttng
from .value_objects import (PublisherValueLttng,
                            SubscriptionCallbackValueLttng,
                            TimerCallbackValueLttng)
from ...common import Util
from ...exceptions import ItemNotFoundError, MultipleItemFoundError
from ...value_objects import (NodeValue, PublisherValue,
                              SubscriptionCallbackValue,
                              TimerCallbackValue)
from ...struct import (PublisherStruct,
                       SubscriptionCallbackStruct,
                       TimerCallbackStruct)


class LttngBridge:
    def __init__(
        self,
        lttng: Lttng
    ) -> None:
        self._lttng = lttng

    def get_timer_callback(
        self,
        callback: TimerCallbackStruct
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
        callback :TimerCallbackStructTimerCallbackStruct
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
        try:
            condition = TimerCallbackBindCondition(callback)
            node = NodeValue(callback.node_name, None)
            timer_callbacks = self._lttng.get_timer_callbacks(node)
            timer_callback = Util.find_one(condition, timer_callbacks)
        except ItemNotFoundError:
            msg = 'No value matching the search condition is found. {condition}'
            msg += str(condition)
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple pieces of values matching the search condition are found.'
            msg += str(condition)
            raise MultipleItemFoundError(msg)

        return timer_callback

    def get_subscription_callback(
        self,
        callback: SubscriptionCallbackStruct
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
        callback : SubscriptionCallbackStruct
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
        try:
            node = NodeValue(callback.node_name, None)
            sub_callbacks = self._lttng.get_subscription_callbacks(node)
            condition = SubscriptionCallbackBindCondition(callback)
            sub_callback = Util.find_one(condition, sub_callbacks)
        except ItemNotFoundError:
            msg = 'No value matching the search condition is found. '
            msg += str(condition)
            raise ItemNotFoundError(msg)
        except MultipleItemFoundError:
            msg = 'Multiple pieces of values matching the search condition are found. '
            msg += str(condition)
            raise MultipleItemFoundError(msg)

        return sub_callback

    def get_publishers(
        self,
        publisher_value: PublisherStruct
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
            publisher values that match the condition

        """
        try:
            condition = PublisherBindCondition(publisher_value)
            node = NodeValue(publisher_value.node_name, None)
            pubs = self._lttng.get_publishers(node)
            pubs_filtered = Util.filter_items(condition, pubs)
        except ItemNotFoundError:
            msg = 'Failed to find publisher instance. '
            msg += str(condition)
            raise ItemNotFoundError(msg)

        return pubs_filtered


class TimerCallbackBindCondition:
    """
    Compare timer callback value with the same conditions.

    used conditions:
    - node name
    - callback type
    - period_ns
    - publish topic names

    """

    def __init__(
        self,
        target_condition: Union[TimerCallbackValue, TimerCallbackStruct]
    ) -> None:
        assert isinstance(target_condition, TimerCallbackValue) or \
            isinstance(target_condition, TimerCallbackStruct)
        self._target = target_condition

    def __call__(
        self,
        callback_value: Union[TimerCallbackValue, TimerCallbackStruct],
    ) -> bool:
        if isinstance(self._target, TimerCallbackValue) and \
                isinstance(callback_value, TimerCallbackStruct):
            return self._compare(self._target, callback_value)

        if isinstance(self._target, TimerCallbackStruct) and \
                isinstance(callback_value, TimerCallbackValue):
            return self._compare(callback_value, self._target)

        raise NotImplementedError()

    def _compare(
        self,
        value: TimerCallbackValue,
        struct_value: TimerCallbackStruct
    ) -> bool:
        return value.node_name == struct_value.node_name and \
            value.callback_type == struct_value.callback_type and \
            value.period_ns == struct_value.period_ns and \
            value.symbol == struct_value.symbol

    def __str__(self):
        return str(self._target)


class SubscriptionCallbackBindCondition:
    """
    Get subscription callback value with the same conditions.

    used conditions:
    - node name
    - callback type
    - subscription topic name
    - publish topic names

    """

    def __init__(
        self,
        target_condition: Union[SubscriptionCallbackValue,
                                SubscriptionCallbackStruct]
    ) -> None:
        assert isinstance(target_condition, SubscriptionCallbackValue) or \
            isinstance(target_condition, SubscriptionCallbackStruct)
        self._target = target_condition

    def __call__(
        self,
        callback_value: Union[SubscriptionCallbackValue, SubscriptionCallbackStruct],
    ) -> bool:
        if isinstance(self._target, SubscriptionCallbackValue) and \
                isinstance(callback_value, SubscriptionCallbackStruct):
            return self._compare(self._target, callback_value)

        if isinstance(self._target, SubscriptionCallbackStruct) and \
                isinstance(callback_value, SubscriptionCallbackValue):
            return self._compare(callback_value, self._target)

        raise NotImplementedError()

    def _compare(
        self,
        value: SubscriptionCallbackValue,
        struct_value: SubscriptionCallbackStruct
    ) -> bool:
        # The value on publish_topic_names obtained from lttng and
        # publish_topic_names obtained from yaml are different.
        # pub_match = True
        # # if value.publish_topic_names is not None:
        # #     pub_match = value.publish_topic_names == struct_value.publish_topic_names

        return value.node_name == struct_value.node_name and \
            value.callback_type == struct_value.callback_type and \
            value.subscribe_topic_name == struct_value.subscribe_topic_name and \
            value.symbol == struct_value.symbol

    def __str__(self):
        return str(self._target)


class PublisherBindCondition:
    """
    Get publisher value with the same conditions.

    used conditions:
    - node name
    - topic name

    """

    def __init__(
        self,
        target_condition: Union[PublisherValue, PublisherStruct]
    ) -> None:
        assert isinstance(target_condition, PublisherValue) or \
            isinstance(target_condition, PublisherStruct)
        self._target = target_condition

    def __call__(
        self,
        publisher_value: Union[PublisherValue, PublisherStruct],
    ) -> bool:
        if isinstance(self._target, PublisherValue) and \
                isinstance(publisher_value, PublisherStruct):
            return self._compare(self._target, publisher_value)

        if isinstance(self._target, PublisherStruct) and \
                isinstance(publisher_value, PublisherValue):
            return self._compare(publisher_value, self._target)

        raise NotImplementedError()

    def _compare(
        self,
        value: PublisherValue,
        struct_value: PublisherStruct
    ) -> bool:
        return value.node_name == struct_value.node_name and \
            value.topic_name == struct_value.topic_name

    def __str__(self):
        return self._target
